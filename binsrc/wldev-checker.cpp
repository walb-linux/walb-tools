/**
 * @file
 * @brief walb log device realtime checker.
 */
#include "cybozu/option.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "aio_util.hpp"
#include "linux/walb/walb.h"
#include "walb_logger.hpp"
#include "walb_util.hpp"
#include "walb_log_file.hpp"
#include "wdev_log.hpp"
#include "wdev_util.hpp"
#include "easy_signal.hpp"

using namespace walb;

/**
 * Command line configuration.
 */
struct Option
{
    std::string wdevName;
    std::string logPath;
    uint64_t bgnLsid;
    size_t pollIntervalMs;
    size_t logIntervalS;
    size_t retryMs;
    bool dontUseAio;
    uint64_t readStepSize;
    bool isDeleteWlog;
    bool isDebug;
    bool checkMem;
    bool skipLogIos;
    bool isZeroDelete;
    bool keepCsum;
    bool monitorPadding;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wldev-checker: check wldev.");
        opt.appendParam(&wdevName, "WDEV_NAME", ": walb device name.");
        opt.appendOpt(&bgnLsid, UINT64_MAX, "b", "begin lsid.");
        opt.appendOpt(&pollIntervalMs, 1000, "i", "poll interval [ms] (default 1000)");
        opt.appendOpt(&retryMs, 100, "r", "retry interval [ms] (default 100)");
        opt.appendOpt(&logPath, "-", "l", "log output path (default '-')");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendOpt(&readStepSize, 128 * MEBI, "s", "read size at a step [bytes] (default 128M)");
        opt.appendOpt(&logIntervalS, 60, "logintvl", "interval for normal log [sec]. (default 60)");
        opt.appendBoolOpt(&isDeleteWlog, "delete", "delete wlogs after verify.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendBoolOpt(&checkMem, "mem", ": use /dev/walb/Xxxx instead of /dev/walb/Lxxx.");
        opt.appendBoolOpt(&skipLogIos, "skipio", ": skip logpack IOs.");
        opt.appendBoolOpt(&isZeroDelete, "zero", ": delete wlogs with filling zero data.");
        opt.appendBoolOpt(&keepCsum, "csum", ": keep checksum of each logical block. (enabled only if skipio is disabled.)");
        opt.appendBoolOpt(&monitorPadding, "monpadding", ": monitor padding record.");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (keepCsum && skipLogIos) {
            keepCsum = false;
            LOGs.warn() << "disable keepCsum option due to skipio is enabled.";
        }
    }
};

void dumpLogPackHeader(const std::string& wdevName, uint64_t lsid, const LogPackHeader& packH, const cybozu::Timespec& ts)
{
    cybozu::TmpFile tmpFile(".");
    cybozu::util::File file(tmpFile.fd());
    file.write(packH.rawData(), packH.pbs());
    cybozu::FilePath outPath(".");
    outPath += cybozu::util::formatString("logpackheader-%s-%" PRIu64 "-%s", wdevName.c_str(), lsid, ts.str().c_str());
    tmpFile.save(outPath.str());
}

void dumpLogPackIo(const std::string& wdevName, uint64_t lsid, size_t idx, const LogPackHeader& packH, const AlignedArray &buf, const cybozu::Timespec& ts)
{
    cybozu::TmpFile tmpFile(".");
    cybozu::util::File file(tmpFile.fd());
    const WlogRecord &rec = packH.record(idx);
    file.write(buf.data(), rec.io_size * LBS);
    cybozu::FilePath outPath(".");
    outPath += cybozu::util::formatString("logpackio-%s-%" PRIu64 "-%zu-%s", wdevName.c_str(), lsid, idx, ts.str().c_str());
    tmpFile.save(outPath.str());
}

void waitMs(size_t ms)
{
    if (ms > 0) util::sleepMs(ms);
}

bool isEqualLogPackHeaderImage(const LogPackHeader& packH, const AlignedArray& prevImg)
{
    return ::memcmp(packH.rawData(), prevImg.data(), prevImg.size()) == 0;
}

class IntervalChecker
{
    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point prevTime_;
    Clock::duration interval_;

public:
    explicit IntervalChecker(uint64_t intervalMs)
        : prevTime_(Clock::now()), interval_(std::chrono::milliseconds(intervalMs)) {
        assert(intervalMs > 0);
    }
    bool isElapsed() {
        Clock::time_point now = Clock::now();
        if (now < prevTime_ + interval_) return false;
        prevTime_ = now;
        return true;
    }
};

bool retryForeverReadLogPackHeader(
    const std::string& wdevName, device::SimpleWldevReader& sReader,
    LogPackHeader& packH, uint64_t lsid, size_t retryMs, uint64_t logCapacityPb)
{
    const std::string wdevPath = device::getWdevPathFromWdevName(wdevName);
    const cybozu::Timespec ts0 = cybozu::getNowAsTimespec();
    LOGs.error() << "invalid logpack header" << wdevName << lsid << ts0;
    dumpLogPackHeader(wdevName, lsid, packH, ts0);
    AlignedArray prevImg(packH.pbs());
    ::memcpy(prevImg.data(), packH.rawData(), prevImg.size());

    size_t c = 0;
    IntervalChecker ic(1000);
  retry:
    if (cybozu::signal::gotSignal()) return false;
    waitMs(retryMs);
    c++;
    sReader.reset(lsid);
    if (!readLogPackHeader(sReader, packH, lsid)) {
        if (!isEqualLogPackHeaderImage(packH, prevImg)) {
            if (device::isOverflow(wdevPath)) {
                throw cybozu::Exception("overflow") << wdevPath;
            }
            const cybozu::Timespec ts1 = cybozu::getNowAsTimespec();
            LOGs.info() << "invalid logpack header changed" << wdevName << lsid << ts1 << c;
            dumpLogPackHeader(wdevName, lsid, packH, ts1);
            ::memcpy(prevImg.data(), packH.rawData(), prevImg.size());
        }
        if (ic.isElapsed()) {
            // Check one lap behined.
            device::LsidSet lsidSet;
            device::getLsidSet(wdevName, lsidSet);
            if (lsidSet.latest - lsid > logCapacityPb) {
                throw cybozu::Exception("One lap behined.") << lsidSet.latest << lsid;
            }
        }
        goto retry;
    }
    const cybozu::Timespec ts1 = cybozu::getNowAsTimespec();
    const cybozu::TimespecDiff td = ts1 - ts0;
    LOGs.info() << "retry succeeded" << wdevName << lsid << ts0 << ts1 << td << c;
    dumpLogPackHeader(wdevName, lsid, packH, ts1);
    return true;
}

void copyLogPackIo(AlignedArray& dst, const AlignedArray& src)
{
    assert(dst.size() <= src.size());
    ::memcpy(dst.data(), src.data(), dst.size());
}

bool isEqualLogPackIoImage(const AlignedArray& buf0, const AlignedArray& buf1)
{
    return ::memcmp(buf0.data(), buf1.data(), std::min(buf0.size(), buf1.size())) == 0;
}

bool retryForeverReadLogPackIo(
    const std::string& wdevName, device::SimpleWldevReader& sReader,
    const LogPackHeader& packH, size_t i, AlignedArray& buf, size_t retryMs, uint64_t logCapacityPb)
{
    const std::string wdevPath = device::getWdevPathFromWdevName(wdevName);
    const cybozu::Timespec ts0 = cybozu::getNowAsTimespec();
    const uint64_t lsid = packH.logpackLsid();
    LOGs.error() << "invalid logpack IO" << wdevName << lsid << i << ts0;
    dumpLogPackHeader(wdevName, lsid, packH, ts0);
    dumpLogPackIo(wdevName, lsid, i, packH, buf, ts0);
    const WlogRecord &rec = packH.record(i);
    AlignedArray prevImg(rec.ioSizeLb() * LBS);
    copyLogPackIo(prevImg, buf);

    size_t c = 0;
    IntervalChecker ic(1000);
retry:
    if (cybozu::signal::gotSignal()) return false;
    waitMs(retryMs);
    c++;
    sReader.reset(rec.lsid);
    buf.clear();
    if (!readLogIo(sReader, packH, i, buf)) {
        if (!isEqualLogPackIoImage(buf, prevImg)) {
            if (device::isOverflow(wdevPath)) {
                throw cybozu::Exception("overflow") << wdevPath;
            }
            const cybozu::Timespec ts1 = cybozu::getNowAsTimespec();
            LOGs.info() << "invalid logpack IO changed" << wdevName << lsid << i << ts1 << c;
            dumpLogPackHeader(wdevName, lsid, packH, ts1);
            dumpLogPackIo(wdevName, lsid, i, packH, buf, ts1);
            copyLogPackIo(prevImg, buf);
        }
        if (ic.isElapsed()) {
            // Check one lap behined.
            device::LsidSet lsidSet;
            device::getLsidSet(wdevName, lsidSet);
            if (lsidSet.latest - lsid > logCapacityPb) {
                throw cybozu::Exception("One lap behined.") << lsidSet.latest << lsid;
            }
        }
        goto retry;
    }
    const cybozu::Timespec ts1 = cybozu::getNowAsTimespec();
    const cybozu::TimespecDiff td = ts1 - ts0;
    LOGs.info() << "retry succeeded" << wdevName << lsid << i << ts0 << ts1 << td << c;
    dumpLogPackHeader(wdevName, lsid, packH, ts1);
    dumpLogPackIo(wdevName, lsid, i, packH, buf, ts1);
    return true;
}

void pushCsumPerPb(const AlignedArray& buf, size_t pbs, std::deque<uint32_t>& outQ)
{
    assert(buf.size() % pbs == 0);
    for (size_t off = 0; off < buf.size(); off += pbs) {
        const uint32_t csum = cybozu::util::calcChecksum(buf.data() + off, pbs, 0);
        outQ.push_back(csum);
    }
}

template <typename Reader>
void checkWldev(const Option &opt)
{
    const std::string& wdevName = opt.wdevName;
    const std::string wdevPath = device::getWdevPathFromWdevName(wdevName);
    const std::string wldevPath =
        opt.checkMem ? (device::WDEV_PATH_PREFIX + "X" + wdevName) : device::getWldevPathFromWdevName(wdevName);
    Reader reader(wldevPath);
    device::SuperBlock &super = reader.super();
    const uint32_t pbs = super.pbs();
    const uint32_t salt = super.salt();
    const uint64_t readStepPb = opt.readStepSize / pbs;
    const uint64_t logCapacityPb = super.getRingBufferSize();

    uint64_t lsid = opt.bgnLsid;
    if (lsid == UINT64_MAX) {
        device::LsidSet lsidSet;
        device::getLsidSet(wdevName, lsidSet);
        if (device::isOverflow(wdevPath)) {
            lsid = lsidSet.prevWritten;
        } else {
            lsid = lsidSet.oldest;
        }
    }
    reader.reset(lsid);
    LOGs.info() << super;
    LOGs.info() << "start lsid" << wdevName << lsid;

    uint64_t csumLsid = lsid;
    std::deque<uint32_t> csumDeq;

    device::SimpleWldevReader sReader(wldevPath);

    double t0 = cybozu::util::getTime();
    LogPackHeader packH(pbs, salt);
    for (;;) {
        if (cybozu::signal::gotSignal()) goto fin;
        const double t1 = cybozu::util::getTime();
        if (t1 - t0 > opt.logIntervalS) {
            LOGs.info() << "current lsid" << wdevName << lsid;
            t0 = t1;
        }
        device::LsidSet lsidSet;
        device::getLsidSet(wdevName, lsidSet);
        if (lsid >= lsidSet.permanent) {
            waitMs(opt.pollIntervalMs);
            continue;
        }
        const uint64_t lsidEnd = std::min(lsid + readStepPb, lsidSet.permanent);
        uint64_t aheadPb = lsidEnd - lsid;
        reader.reset(lsid, aheadPb);
        while (lsid < lsidEnd) {
            if (!readLogPackHeader(reader, packH, lsid)) {
                if (!retryForeverReadLogPackHeader(wdevName, sReader, packH, lsid, opt.retryMs, logCapacityPb)) {
                    goto fin;
                }
                aheadPb = lsidEnd - (lsid + 1);
                reader.reset(lsid + 1, aheadPb); // for next read.
            }
            if (opt.monitorPadding && packH.nPadding() > 0) {
                LOGs.info() << std::string("padding found\n") + packH.str();
            }
            if (opt.keepCsum) {
                const uint32_t csum = cybozu::util::calcChecksum(packH.rawData(), pbs, 0);
                csumDeq.push_back(csum);
                csumLsid = lsid + 1;
            }
            if (opt.skipLogIos) {
                skipAllLogIos(reader, packH);
            } else {
                for (size_t i = 0; i < packH.nRecords(); i++) {
                    const WlogRecord &rec = packH.record(i);
                    if (!rec.hasData()) continue;
                    AlignedArray buf;
                    const uint64_t nextLsid = rec.lsid + rec.ioSizePb(pbs);
                    if (!readLogIo(reader, packH, i, buf)) {
                        if (!retryForeverReadLogPackIo(wdevName, sReader, packH, i, buf, opt.retryMs, logCapacityPb)) {
                            goto fin;
                        }
                        aheadPb = lsidEnd - nextLsid;
                        reader.reset(nextLsid, aheadPb); // for next read.
                    }
                    if (opt.keepCsum) {
                        pushCsumPerPb(buf, pbs, csumDeq);
                        csumLsid = nextLsid;
                    }
                }
            }
            lsid = packH.nextLogpackLsid();
        }
        if (opt.isDeleteWlog && !device::isOverflow(wdevPath) && lsidSet.oldest < lsid && lsidSet.oldest < lsidSet.prevWritten) {
            const uint64_t newOldestLsid = std::min(lsid, lsidSet.prevWritten);
            bool succeeded = false;
            try {
                device::eraseWal(wdevName, newOldestLsid);
                succeeded = true;
            } catch (std::exception& e) {
                LOGs.warn() << "erase wal failed" << e.what();
            }
            // When the wdev is going to overflow, it is dangerous to fill zero.
            if (opt.isZeroDelete && succeeded) {
                device::fillZeroToLdev(wdevName, lsidSet.oldest, newOldestLsid);
            }
        }
        if (opt.keepCsum) {
            // Remove old records.
            if (logCapacityPb * 2 < csumDeq.size()) {
                const size_t nr = csumDeq.size() - (logCapacityPb * 2);
                csumDeq.erase(csumDeq.begin(), csumDeq.begin() + nr);
            }
        }
    }
  fin:
    if (opt.keepCsum) {
        cybozu::util::File file(wdevName + ".csum", O_CREAT | O_TRUNC | O_WRONLY, 0644);
        uint64_t lsid = csumLsid - csumDeq.size();
        auto it = csumDeq.cbegin();
        while (it != csumDeq.cend()) {
            std::string s = cybozu::util::formatString("%" PRIu64 "\t%08x\n", lsid, *it);
            file.write(s.data(), s.size());
            ++it;
            lsid++;
        }
        file.fsync();
        file.close();
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting(opt.logPath, opt.isDebug);
    cybozu::signal::setSignalHandler({SIGINT, SIGQUIT, SIGTERM});

    if (opt.dontUseAio) {
        checkWldev<device::SimpleWldevReader>(opt);
    } else {
        checkWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wldev-checker")
