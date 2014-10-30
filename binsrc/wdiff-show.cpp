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
    bool isHead, isDebug, doSearch;
    uint64_t addr;
    std::string filePath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.appendBoolOpt(&doSearch, "search", ": search a specific block.");
        opt.appendOpt(&addr, 0, "addr", ": search address [logical block].");
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

inline bool matchAddress(uint64_t addr, const DiffRecord& rec)
{
    return rec.io_address <= addr && addr < rec.endIoAddress();
}

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
        if (!opt.doSearch || matchAddress(opt.addr, rec)) {
            if (!rec.isValid()) {
                ::printf("Invalid record: ");
            }
            rec.printOneline();
        }
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-show")
