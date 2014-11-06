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
    bool isHead, isDebug, doSearch, doStat, noRec;
    uint64_t addr;
    std::string filePath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.appendBoolOpt(&doSearch, "search", ": search a specific block.");
        opt.appendOpt(&addr, 0, "addr", ": search address [logical block].");
        opt.appendBoolOpt(&isHead, "head", ": put record description.");
        opt.appendBoolOpt(&doStat, "stat", ": put statistics.");
        opt.appendBoolOpt(&noRec, "norec", "; does not put records.");
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

struct DiffStatistics
{
    size_t normalNr; // normal IOs.
    size_t allZeroNr; // all zero IOs.
    size_t discardNr; // discard IOs.

    uint64_t normalLb; // normal IO total size. [logical block]
    uint64_t allZeroLb; // all zero IO total size. [logical block]
    uint64_t discardLb; // discard IO totalsize. [logical block]

    uint64_t dataSize; // total IO data size (compressed) [byte]

    DiffStatistics()
        : normalNr(0)
        , allZeroNr(0)
        , discardNr(0)
        , normalLb(0)
        , allZeroLb(0)
        , discardLb(0)
        , dataSize(0) {
    }
    void update(const DiffRecord& rec) {
        if (rec.isNormal()) {
            normalNr++;
            normalLb += rec.io_blocks;
        } else if (rec.isDiscard()) {
            discardNr++;
            discardLb += rec.io_blocks;
        } else if (rec.isAllZero()) {
            allZeroNr++;
            allZeroLb += rec.io_blocks;
        }
        dataSize += rec.data_size;
    }
    void print() const {
        const char *const pre = "wdiff_stat:";
        ::printf(
            "%s normalNr %zu\n"
            "%s allZeroNr %zu\n"
            "%s discardNr %zu\n"
            "%s normalLb %" PRIu64 "\n"
            "%s allZeroLb %" PRIu64 "\n"
            "%s discardLb %" PRIu64 "\n"
            "%s dataSize %" PRIu64 "\n"
            , pre, normalNr
            , pre, allZeroNr
            , pre, discardNr
            , pre, normalLb
            , pre, allZeroLb
            , pre, discardLb
            , pre, dataSize);
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
    DiffStatistics stat;
    DiffRecord rec;
    DiffIo io;
    while (wdiffR.readDiff(rec, io)) {
        if (!opt.doSearch || matchAddress(opt.addr, rec)) {
            if (!opt.noRec) {
                if (!rec.isValid()) ::printf("Invalid record: ");
                rec.printOneline();
            }
            if (opt.doStat) stat.update(rec);
        }
    }
    if (opt.doStat) stat.print();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-show")
