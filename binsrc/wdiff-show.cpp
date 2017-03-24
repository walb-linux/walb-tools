/**
 * @file
 * @brief Show walb diff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "util.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_stat.hpp"
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "fileio.hpp"

using namespace walb;

struct Option
{
    bool isDebug, doSearch, doStat, noHead, noRec, verifyCsum;
    uint64_t addr;
    std::string filePath;
    std::vector<std::string> filePathV;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;

        std::string desc("wdiff-show: show the contents of wdiff files.\n");
        desc += "Records description:\n  ";
        desc += DiffRecord::getHeader();
        opt.setDescription(desc);

        opt.appendBoolOpt(&doSearch, "search", ": search a specific block.");
        opt.appendOpt(&addr, 0, "addr", ": search address [logical block].");
        opt.appendBoolOpt(&doStat, "stat", ": put statistics.");
        opt.appendBoolOpt(&noHead, "nohead", ": does not put header..");
        opt.appendBoolOpt(&noRec, "norec", ": does not put records.");
        opt.appendBoolOpt(&verifyCsum, "csum", ": verify checksum of each IO data.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendParamVec(&filePathV, "WDIFF_PATH_LIST", ": wdiff file list (default: stdin)");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (filePathV.empty()) {
            ::fprintf(::stderr, "Specify wdiff files.");
            opt.usage();
            ::exit(1);
        }
    }
};

template <typename Record>
inline bool matchAddress(uint64_t addr, const Record& rec)
{
    return rec.io_address <= addr && addr < rec.endIoAddress();
}

int printSortedWdiff(
    const DiffFileHeader &header, cybozu::util::File &file,
    DiffStatistics &stat, const Option &opt)
{
    if (!opt.noHead) header.print();

    ExtendedDiffPackHeader edp;
    DiffPackHeader &pack = edp.header;

    int ret = 0;
    for (;;) {
        pack.readFrom(file, false);
        if (!pack.isValid()) {
            ::printf("invalid pack: ");
            pack.print();
            return 1;
        }
        if (pack.isEnd()) break;
        stat.update(pack);

        for (size_t i = 0; i < pack.n_records; i++) {
            const DiffRecord &rec = pack[i];
            assert(rec.isValid()); // If pack is valid, record must be valid.
            std::string extra;
            if (rec.isNormal()) {
                if (opt.verifyCsum) {
                    DiffIo io;
                    io.setAndReadFrom(rec, file, false);
                    const uint32_t csum = io.calcChecksum();
                    if (rec.checksum != csum) {
                        extra += cybozu::util::formatString(
                            " invalid data checksum: %08x", csum);
                        ret = 1;
                    }
                } else {
                    file.skip(rec.data_size);
                }
            }
            const bool showRec = !opt.doSearch || matchAddress(opt.addr, rec);
            if ((!opt.noRec && showRec) || !extra.empty()) {
                ::printf("wdiff_sorted_rec:\t%s%s\n", rec.toStr().c_str(), extra.c_str());
            }
        }
    }
    return ret;
}

int printIndexedWdiff(
    cybozu::util::File &&file,
    DiffStatistics &stat, const Option &opt)
{
    IndexedDiffReader reader;
    IndexedDiffCache cache;
    cache.setMaxSize(32 * MEBI);
    reader.setFile(std::move(file), cache);

    if (!opt.noHead) reader.header().print();

    IndexedDiffRecord rec;
    AlignedArray data;
    int ret = 0;
    while (reader.readDiffRecord(rec, false)) {
        std::string extra;
        if (!rec.isValid()) {
            extra += cybozu::util::formatString(" invalid record");
            ret = 1;
        }
        if (rec.isValid() && opt.verifyCsum && !reader.isOnCache(rec)) {
            if (!reader.loadToCache(rec, false)) {
                extra += cybozu::util::formatString(" invalid data checksum");
                ret = 1;
            }
        }
        const bool showRec = !opt.doSearch || matchAddress(opt.addr, rec);
        if ((!opt.noRec && showRec) || !extra.empty()) {
            ::printf("wdiff_indexed_rec:\t%s%s\n", rec.toStr().c_str(), extra.c_str());
        }
    }
    stat.update(reader.getStat());
    reader.close();
    return ret;
}


int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    DiffStatistics stat;

    int ret = 0;
    for (const std::string &path : opt.filePathV) {
        DiffFileHeader header;
        cybozu::util::File file(path, O_RDONLY);
        header.readFrom(file);
        if (header.isIndexed()) {
            ret = printIndexedWdiff(std::move(file), stat, opt);
        } else {
            ret = printSortedWdiff(header, file, stat, opt);
        }
    }
    if (opt.doStat) stat.print(::stdout, "wdiff_stat: ");
    return ret;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-show")
