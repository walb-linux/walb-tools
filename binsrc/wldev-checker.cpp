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
    uint64_t bgnLsid;
    size_t pollIntervalMs;
    bool dontUseAio;
    uint64_t readStepSize;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wldev-checker: check wldev.");
        opt.appendParam(&wdevName, "WDEV_NAME", ": walb device name.");
        opt.appendOpt(&bgnLsid, UINT64_MAX, "b", "begin lsid.");
        opt.appendOpt(&pollIntervalMs, 1000, "i", "poll interval [ms]");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendOpt(&readStepSize, 128 * MEBI, "s", "read size at a step [bytes]");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

void dumpLogPackHeader(uint64_t lsid, const LogPackHeader& packH, const std::string& ts)
{
    cybozu::TmpFile tmpFile(".");
    cybozu::util::File file(tmpFile.fd());
    file.write(packH.rawData(), packH.pbs());
    cybozu::FilePath outPath(".");
    outPath += cybozu::util::formatString("logpackheader-%" PRIu64 "-%s", lsid, ts.c_str());
    tmpFile.save(outPath.str());
}

void dumpLogPackIo(uint64_t lsid, size_t i, const LogPackHeader& packH, const LogBlockShared& blockS, const std::string& ts)
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
    outPath += cybozu::util::formatString("logpackio-%" PRIu64 "-%zu-%s", lsid, i, ts.c_str());
    tmpFile.save(outPath.str());
}

template <typename Reader>
void checkWldev(const Option &opt)
{
    const std::string& wdevName = opt.wdevName;
    const std::string wdevPath = device::getWdevPathFromWdevName(wdevName);
    const std::string wldevPath = device::getWldevPathFromWdevName(wdevName);
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

    LogPackHeader packH(pbs, salt);
    for (;;) { // Infinite loop.
        device::LsidSet lsidSet;
        device::getLsidSet(wdevName, lsidSet);
        if (lsid >= lsidSet.permanent) {
            util::sleepMs(opt.pollIntervalMs);
            reader.reset(lsid);
            continue;
        }
        const uint64_t lsidEnd = std::min(lsid + readStepPb, lsidSet.permanent);
        while (lsid < lsidEnd) {
            if (!readLogPackHeader(reader, packH, lsid)) {
                const std::string ts = util::getNowStr();
                LOGs.error() << "invalid logpack header" << lsid << ts;
                dumpLogPackHeader(lsid, packH, ts);
                util::sleepMs(opt.pollIntervalMs);
                reader.reset(lsid);
                continue;
            }
            bool invalid = false;
            for (size_t i = 0; i < packH.nRecords(); i++) {
                const WlogRecord &rec = packH.record(i);
                if (!rec.hasData()) continue;
                LogBlockShared blockS;
                if (!readLogIo(reader, packH, i, blockS)) {
                    const std::string ts = util::getNowStr();
                    LOGs.error() << "invalid logpack IO" << lsid << i << ts;
                    dumpLogPackHeader(lsid, packH, ts);
                    dumpLogPackIo(lsid, i, packH, blockS, ts);
                    invalid = true;
                    break;
                }
            }
            if (invalid) {
                util::sleepMs(opt.pollIntervalMs);
                reader.reset(lsid);
                continue;
            }
            lsid = packH.nextLogpackLsid();
        }
#if 0
        // Delete wlogs if necessary.
        if (!device::isOverflow(wdevPath) && lsidSet.oldest < lsid && lsidSet.oldest < lsidSet.prevWritten) {
            const uint64_t newOldestLsid = std::min(lsid, lsidSet.prevWritten);
            device::eraseWal(wdevName, newOldestLsid);
        }
#endif
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    if (opt.dontUseAio) {
        checkWldev<device::SimpleWldevReader>(opt);
    } else {
        checkWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wldev-checker")
