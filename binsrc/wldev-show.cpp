/**
 * @file
 * @brief walb log device pretty printer.
 */
#include "cybozu/option.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_logger.hpp"
#include "walb_util.hpp"
#include "walb_log_file.hpp"
#include "wdev_log.hpp"

using namespace walb;

/**
 * Command line configuration.
 */
struct Option
{
    std::string wldevPath;
    uint64_t bgnLsid;
    uint64_t endLsid;
    bool showSuper, showHead, showPack, showStat;
    bool dontUseAio;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wldev-show: pretty-print walb log device.");
        opt.appendOpt(&bgnLsid, 0, "b", "LSID: begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendParam(&wldevPath, "WLDEV_PATH", ": input walb log device  path.");
        opt.appendBoolOpt(&showSuper, "super", ": show super block.");
        opt.appendBoolOpt(&showHead, "head", ": show file header.");
        opt.appendBoolOpt(&showPack, "pack", ": show packs.");
        opt.appendBoolOpt(&showStat, "stat", ": show statistics.");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (endLsid <= bgnLsid) {
            throw RT_ERR("bgnLsid must be < endLsid.");
        }

        // In default, show all (not including showSuper).
        if (!showHead && !showPack && !showStat) {
            showHead = showPack = showStat = true;
        }
    }
};

template <typename Reader>
void showWldev(const Option &opt)
{
    Reader reader(opt.wldevPath);
    device::SuperBlock &super = reader.super();
    const uint32_t pbs = super.pbs();
    const uint32_t salt = super.salt();
    const uint64_t bgnLsid = std::max(opt.bgnLsid, super.getOldestLsid());
    if (opt.showSuper) super.print();

    log::FileHeader wh;
    wh.init(pbs, salt, super.getUuid(), bgnLsid, opt.endLsid);
    wh.updateChecksum();
    if (opt.showHead) std::cout << wh.str() << std::endl;
    uint64_t lsid = bgnLsid;
    reader.reset(lsid);

    LogStatistics logStat;
    logStat.init(bgnLsid, opt.endLsid);
    LogPackHeader packH(pbs, salt);
    while (lsid < opt.endLsid) {
        if (!readLogPackHeader(reader, packH, lsid)) break;
        if (opt.showPack) std::cout << packH << std::endl;
        skipAllLogIos(reader, packH);
        logStat.update(packH);
        lsid = packH.nextLogpackLsid();
    }
    if (opt.showStat) std::cout << logStat << std::endl;
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    if (opt.dontUseAio) {
        showWldev<device::SimpleWldevReader>(opt);
    } else {
        showWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-show")
