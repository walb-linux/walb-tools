/**
 * @file
 * @brief Show walb diff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "util.hpp"
#include "walb_diff_file.hpp"
#include "cybozu/option.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    bool isHead;
    bool isDebug;
    std::string filePath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.appendBoolOpt(&isHead, "head", ": put record description.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendParamOpt(&filePath, "-", "WDIFF_PATH", ": wdiff file (default: stdin)");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    diff::Reader wdiffR;
    if (opt.filePath == "-") {
        wdiffR.setFd(0);
    } else {
        wdiffR.open(opt.filePath);
    }
    DiffFileHeader wdiffH;
    wdiffR.readHeader(wdiffH);
    wdiffH.print();

    if (opt.isHead) DiffRecord::printHeader();
    DiffRecord rec;
    DiffIo io;
    while (wdiffR.readDiff(rec, io)) {
        if (!rec.isValid()) {
            ::printf("Invalid record: ");
        }
        rec.printOneline();
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-show")
