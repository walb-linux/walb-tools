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
    bool dontUseAio;
    uint64_t readStepSize;
    bool isDeleteWlog;
    bool isDebug;
    bool checkMem;
    bool skipLogIos;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wldev-checker: check wldev.");
        opt.appendParam(&wdevName, "WDEV_NAME", ": walb device name.");
        opt.appendOpt(&bgnLsid, UINT64_MAX, "b", "begin lsid.");
        opt.appendOpt(&pollIntervalMs, 1000, "i", "poll interval [ms] (defaalt 1000)");
        opt.appendOpt(&logPath, "-", "l", "log output path (default '-')");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendOpt(&readStepSize, 128 * MEBI, "s", "read size at a step [bytes] (default 128M)");
        opt.appendOpt(&logIntervalS, 60, "logintvl", "interval for normal log [sec]. (default 60)");
        opt.appendBoolOpt(&isDeleteWlog, "delete", "delete wlogs after verify.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendBoolOpt(&checkMem, "mem", ": use /dev/walb/Xxxx instead of /dev/walb/Lxxx.");
        opt.appendBoolOpt(&skipLogIos, "skipio", ": skip logpack IOs.");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
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

void dumpLogPackIo(const std::string& wdevName, uint64_t lsid, size_t i, const LogPackHeader& packH, const LogBlockShared& blockS, const cybozu::Timespec& ts)
{
    cybozu::TmpFile tmpFile(".");
    cybozu::util::File file(tmpFile.fd());
    const WlogRecord &rec = packH.record(i);
    size_t remaining = rec.io_size * LBS;
    for (size_t j = 0; j < blockS.nBlocks(); j++) {
        const size_t s = std::min<size_t>(packH.pbs(), remaining);
        file.write(blockS.get(j), s);
        remaining -= s;
    }
    cybozu::FilePath outPath(".");
    outPath += cybozu::util::formatString("logpackio-%s-%" PRIu64 "-%zu-%s", wdevName.c_str(), lsid, i, ts.str().c_str());
    tmpFile.save(outPath.str());
}

void wait(const Option &opt)
{
    if (opt.pollIntervalMs > 0) util::sleepMs(opt.pollIntervalMs);
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

    device::SimpleWldevReader sReader(wldevPath); // QQQ

    double t0 = cybozu::util::getTime();
    LogPackHeader packH(pbs, salt);
    bool overRead = false;
    for (;;) { // Infinite loop.
        const double t1 = cybozu::util::getTime();
        if (t1 - t0 > opt.logIntervalS) {
            LOGs.info() << "current lsid" << wdevName << lsid;
            t0 = t1;
        }
        device::LsidSet lsidSet;
        device::getLsidSet(wdevName, lsidSet);
        if (lsid >= lsidSet.permanent) {
            wait(opt);
            overRead = true;
            continue;
        }
        if (overRead) {
            reader.reset(lsid);
            overRead = false;
        }
        const uint64_t lsidEnd = std::min(lsid + readStepPb, lsidSet.permanent);
        while (lsid < lsidEnd) {
            if (!readLogPackHeader(reader, packH, lsid)) {
                const cybozu::Timespec ts0 = cybozu::getNowAsTimespec();
#if 0
                LOGs.error() << "invalid logpack header" << wdevName << lsid << ts0;
                dumpLogPackHeader(wdevName, lsid, packH, ts0);
#endif

                int c = 0;
              retry2:
                c++;
                sReader.reset(lsid);
                if (!readLogPackHeader(sReader, packH, lsid)) {
                    goto retry2;
                }
                const cybozu::Timespec ts1 = cybozu::getNowAsTimespec();
                const cybozu::TimespecDiff td = ts1 - ts0;
                LOGs.info() << "retry succeeded" << wdevName << lsid << ts0 << ts1 << td << c;
                reader.reset(lsid + 1);
#if 0
                wait(opt);
                reader.reset(lsid);
                continue;
#endif
            }
            if (opt.skipLogIos) {
                skipAllLogIos(reader, packH);
            } else {
                for (size_t i = 0; i < packH.nRecords(); i++) {
                    const WlogRecord &rec = packH.record(i);
                    if (!rec.hasData()) continue;
                    LogBlockShared blockS;
                  retry:
                    if (!readLogIo(reader, packH, i, blockS)) {
                        const cybozu::Timespec ts = cybozu::getNowAsTimespec();
                        LOGs.error() << "invalid logpack IO" << wdevName << lsid << i << ts;
                        dumpLogPackHeader(wdevName, lsid, packH, ts);
                        dumpLogPackIo(wdevName, lsid, i, packH, blockS, ts);
                        wait(opt);
                        reader.reset(packH.record(i).lsid);
                        blockS.clear();
                        goto retry;
                    }
                }
            }
            lsid = packH.nextLogpackLsid();
        }
        if (opt.isDeleteWlog && !device::isOverflow(wdevPath) && lsidSet.oldest < lsid && lsidSet.oldest < lsidSet.prevWritten) {
            const uint64_t newOldestLsid = std::min(lsid, lsidSet.prevWritten);
            device::eraseWal(wdevName, newOldestLsid);
        }
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting(opt.logPath, opt.isDebug);

    if (opt.dontUseAio) {
        checkWldev<device::SimpleWldevReader>(opt);
    } else {
        checkWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wldev-checker")
