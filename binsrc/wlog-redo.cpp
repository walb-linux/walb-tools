/**
 * @file
 * @brief Redo walb log.
 */
#include "cybozu/option.hpp"
#include "walb_log_redo.hpp"

using namespace walb;

struct Option
{
    std::string ddevPath;
    std::string inWlogPath;
    bool isDiscard;
    bool isZeroDiscard;
    bool dontUseAio;
    bool isVerbose;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Redo wlog on a block device.");
        opt.appendOpt(&inWlogPath, "-", "i", "PATH: input wlog path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isDiscard, "d", "issue discard for discard logs.");
        opt.appendBoolOpt(&isZeroDiscard, "z", "zero-clear for discard logs.");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendParam(&ddevPath, "DEVICE_PATH");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (isDiscard && isZeroDiscard) {
            throw RT_ERR("Do not specify both -d and -z together.");
        }
    }
    bool isFromStdin() const { return inWlogPath == "-"; }
};

void setupInputFile(cybozu::util::File &wlogFile, const Option &opt)
{
    if (opt.isFromStdin()) {
        wlogFile.setFd(0);
    } else {
        wlogFile.open(opt.inWlogPath, O_RDONLY);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File wlogFile;
    setupInputFile(wlogFile, opt);
    log::FileHeader wh;
    wh.readFrom(wlogFile);
    LogRedoConfig cfg;
    cfg = {opt.ddevPath, opt.isVerbose, opt.isDiscard, opt.isZeroDiscard,
           wh.pbs(), wh.salt(), wh.beginLsid(), false};

    if (opt.dontUseAio) {
        LogApplyer<SimpleBdevWriter> applyer(cfg);
        applyer.run(wlogFile);
    } else {
        LogApplyer<AsyncBdevWriter> applyer(cfg);
        applyer.run(wlogFile);
    }
    wlogFile.close();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-redo")
