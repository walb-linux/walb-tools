/**
 * Dump block images from a wdiff file.
 */
#include "util.hpp"
#include "walb_diff_file.hpp"
#include "cybozu/option.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    bool isDebug;
    uint64_t addr;
    uint32_t blks;
    std::string wdiffPath, dumpPath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendParam(&addr, "ADDR", ": start address to dump [logical block].");
        opt.appendParam(&blks, "SIZE", ": number of blocks to dump [logical block].");
        opt.appendParam(&wdiffPath, "WDIFF_PATH", ": input wdiff file.");
        opt.appendParam(&dumpPath, "DUMP_PATH", ": output dump file.");
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

    SortedDiffReader wdiffR(opt.wdiffPath);
    DiffFileHeader wdiffH;
    wdiffR.readHeader(wdiffH);
    if (wdiffH.isIndexed()) {
        throw cybozu::Exception(__func__) << "indexed wdiff files are not supported.";
    }

    cybozu::util::File outF(opt.dumpPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    uint64_t addr = opt.addr;
    uint32_t blks = opt.blks;
    uint64_t totalBlks = 0;
    DiffRecord rec;
    DiffIo io;
    while (blks > 0 && wdiffR.readDiff(rec, io)) {
        if (addr + blks < rec.io_address) break;
        if (!matchAddress(addr, rec)) continue;
        const uint64_t endAddr = std::min<uint64_t>(addr + blks, rec.endIoAddress());
        const uint64_t offB = addr - rec.io_address;
        const uint32_t sizeB = endAddr - addr;

        if (rec.isNormal()) {
            if (rec.isCompressed()) {
                DiffRecord outRec;
                DiffIo outIo;
                uncompressDiffIo(rec, io.data.data(), outRec, outIo.data);
                outIo.set(outRec);
                io = std::move(outIo);
            }
            outF.write(io.data.data() + offB * LBS, sizeB * LBS);
        } else {
            // currently dump all-zero for discard records also.
            const std::vector<char> buf(sizeB * LBS);
            outF.write(buf.data(), buf.size());
        }
        ::fprintf(::stderr,
                  "dump %" PRIu64 " %u %c%c\n"
                  , addr, sizeB
                  , rec.isAllZero() ? 'Z' : '-'
                  , rec.isDiscard() ? 'D' : '-');
        totalBlks += sizeB;
        addr += sizeB;
        blks -= sizeB;
    }
    outF.fsync();
    outF.close();
    ::fprintf(::stderr, "dump %" PRIu64 " blocks\n", totalBlks);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-dump")
