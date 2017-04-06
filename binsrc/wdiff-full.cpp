/**
 * @file
 * @brief Convert raw images to wdiff files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "fileio.hpp"
#include "walb_diff_base.hpp"
#include "walb_diff_file.hpp"

using namespace walb;

struct Option
{
    std::string input, output;
    bool isIndexed;
    size_t ioSize;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setUsage("Usage: wdiff-full -i [full image] -o [wdiff]", true);
        opt.appendOpt(&input, "-", "i", ": input full image (default: stdin)");
        opt.appendOpt(&output, "-", "o", ": output diff file (default: stdout)");
        opt.appendOpt(&ioSize, 64 * KIBI, "ios", ": IO size in bytes (default 64K)");
        opt.appendBoolOpt(&isIndexed, "indexed", ": use indexed format instead of sorted format.");
        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (ioSize == 0 || ioSize % LOGICAL_BLOCK_SIZE != 0) {
            ::fprintf(::stderr, "ioSize must be multiples of logical block size %u.\n"
                      , LOGICAL_BLOCK_SIZE);
            ::exit(1);
        }
    }
};


class FullImageToWalbDiffConverter
{
private:
    const uint32_t ioBlocks_;
    const uint32_t ioSize_;
    std::vector<char> buf0_;

public:
    explicit FullImageToWalbDiffConverter(uint32_t ioBlocks)
        : ioBlocks_(ioBlocks)
        , ioSize_(ioBlocks * LOGICAL_BLOCK_SIZE)
        , buf0_(ioSize_) {
    }
    void run(cybozu::util::File &inFile, cybozu::util::File &outFile, bool isIndexed) {
        SortedDiffWriter sWriter;
        IndexedDiffWriter iWriter;
        if (isIndexed) {
            iWriter.setFd(outFile.fd());
        } else {
            sWriter.setFd(outFile.fd());
        }

        DiffFileHeader head;
        if (isIndexed) {
            iWriter.writeHeader(head);
        } else {
            sWriter.writeHeader(head);
        }

        uint64_t ioAddr = 0;
        uint32_t blks = readChunk(inFile);
        while (0 < blks) {
            if (isIndexed) {
                writeIndexedDiff(iWriter, ioAddr, blks);
            } else {
                writeSortedDiff(sWriter, ioAddr, blks);
            }
            ioAddr += blks;
            blks = readChunk(inFile);
        }

        if (isIndexed) {
            iWriter.finalize();
        } else {
            sWriter.close();
        }
    }

private:
    /**
     * RETURN:
     *   Number of read blocks [logical block]
     */
    uint32_t readChunk(cybozu::util::File &reader) {
        uint32_t c = 0;
        char *p = buf0_.data();
        try {
            while (c < ioBlocks_) {
                reader.read(p, LOGICAL_BLOCK_SIZE);
                p += LOGICAL_BLOCK_SIZE;
                c++;
            }
            assert(ioBlocks_ == c);
        } catch (cybozu::util::EofError &e) {
            /* do nothing. */
        }
        return c;
    }
    void writeSortedDiff(SortedDiffWriter& writer, uint64_t addr, uint32_t blks) {
        DiffRecord rec;
        rec.io_address = addr;
        rec.io_blocks = blks;
        rec.setNormal();
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.data_size = blks * LOGICAL_BLOCK_SIZE;
        rec.checksum = 0; // calculated later.
        writer.compressAndWriteDiff(rec, buf0_.data());
    }
    void writeIndexedDiff(IndexedDiffWriter& writer, uint64_t addr, uint32_t blks) {
        IndexedDiffRecord rec;
        rec.io_address = addr;
        rec.io_blocks = blks;
        rec.setNormal();
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.data_size = blks * LOGICAL_BLOCK_SIZE;
        rec.orig_blocks = blks;
        rec.io_offset = 0;
        rec.io_checksum = 0; // calculated later.
        rec.rec_checksum = 0; // calculated later.
        writer.compressAndWriteDiff(rec, buf0_.data());
    }
};

void setupFiles(const std::string& inName, const std::string& outName,
                cybozu::util::File& inFile, cybozu::util::File& outFile)
{
    if (inName == "-") {
        inFile.setFd(0);
    } else {
        inFile.open(inName, O_RDONLY);
    }
    if (outName == "-") {
        outFile.setFd(1);
    } else {
        outFile.open(outName, O_CREAT | O_TRUNC | O_RDWR, 0644);
    }
}

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);

    cybozu::util::File inFile, outFile;
    setupFiles(opt.input, opt.output, inFile, outFile);

    FullImageToWalbDiffConverter c(opt.ioSize / LOGICAL_BLOCK_SIZE);
    c.run(inFile, outFile, opt.isIndexed);

    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-full")
