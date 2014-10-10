/**
 * @file
 * @brief WalB log pretty printer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

using namespace walb;

/**
 * Command line configuration.
 */
struct Option
{
    std::string inWlogPath;
    uint64_t beginLsid;
    uint64_t endLsid;
    bool showHead, showPack, showStat;
    bool isDebug;

    Option(int argc, char* argv[])
        : inWlogPath("-")
        , beginLsid(0)
        , endLsid(-1)
        , showHead(false)
        , showPack(false)
        , showStat(false)
        , isDebug(false) {

        cybozu::Option opt;
        opt.setDescription("wlog-show: pretty-print wlog input.");
        opt.appendOpt(&beginLsid, 0, "b", "LSID: begin lsid. (default: 0)");
        opt.appendOpt(&endLsid, uint64_t(-1), "e", "LSID: end lsid. (default: 0xffffffffffffffff)");
        opt.appendParamOpt(&inWlogPath, "-", "WLOG_PATH", ": input wlog path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&showHead, "head", ": show file header.");
        opt.appendBoolOpt(&showPack, "pack", ": show packs.");
        opt.appendBoolOpt(&showStat, "stat", ": show statistics.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (endLsid <= beginLsid) {
            throw RT_ERR("beginLsid must be < endLsid.");
        }

        // In default, show all.
        if (!showHead && !showPack && !showStat) {
            showHead = showPack = showStat = true;
        }
    }
    bool isInputStdin() const { return inWlogPath == "-"; }
};

void setupInputFile(LogFile &fileR, const Option &opt)
{
    if (opt.isInputStdin()) {
        fileR.setFd(0);
        fileR.setSeekable(false);
    } else {
        fileR.open(opt.inWlogPath, O_RDONLY);
        fileR.setSeekable(true);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    LogFile fileR;
    setupInputFile(fileR, opt);

    log::FileHeader wh;
    wh.readFrom(fileR);
    if (opt.showHead) std::cout << wh.str() << std::endl;
    uint64_t lsid = wh.beginLsid();

    LogStatistics logStat;
    logStat.init(wh.beginLsid(), wh.endLsid());
    LogPackHeader packH(wh.pbs(), wh.salt());
    while (readLogPackHeader(fileR, packH, lsid)) {
        if (opt.showPack) std::cout << packH << std::endl;
        skipAllLogIos(fileR, packH);
        if (opt.showStat) logStat.update(packH);
        lsid = packH.nextLogpackLsid();
    }

    if (opt.showStat) std::cout << logStat << std::endl;
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-show")
