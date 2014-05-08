/**
 * @file
 * @brief Convert raw images to wdiff files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <stdexcept>
#include <vector>
#include <cstdio>
#include <cassert>
#include "fileio.hpp"
#include "walb_diff_base.hpp"
#include "walb_diff_file.hpp"

class FullImageToWalbDiffConverter
{
private:
    const uint16_t ioBlocks_;
    const uint32_t ioSize_;
    std::vector<char> buf0_;

public:
    explicit FullImageToWalbDiffConverter(uint16_t ioBlocks)
        : ioBlocks_(ioBlocks)
        , ioSize_(ioBlocks * LOGICAL_BLOCK_SIZE)
        , buf0_(ioSize_) {
    }
    void convert(int inFd, int outFd) {
        cybozu::util::File reader(inFd);
        walb::diff::Writer writer(outFd);
        walb::DiffFileHeader head;
        head.setMaxIoBlocksIfNecessary(ioBlocks_);
        writer.writeHeader(head);

        uint64_t ioAddr = 0;
        uint16_t blks = readChunk(reader);
        while (0 < blks) {
            walb::DiffRecord rec;
            rec.io_address = ioAddr;
            rec.io_blocks = blks;
            rec.setNormal();
            rec.data_size = blks * LOGICAL_BLOCK_SIZE;
            uint32_t csum = cybozu::util::calcChecksum(
                buf0_.data(), blks * LOGICAL_BLOCK_SIZE, 0);
            rec.checksum = csum;

            writer.compressAndWriteDiff(rec, buf0_.data());

            ioAddr += blks;
            blks = readChunk(reader);
        }
        writer.close();
    }
private:
    /**
     * RETURN:
     *   Number of read blocks [logical block]
     */
    uint16_t readChunk(cybozu::util::File &reader) {
        uint16_t c = 0;
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
};

int doMain(int argc, char *[])
{
    if (1 < argc) {
        ::printf("Usage: wdiff-full < [full image] > [wdiff]\n");
        return 1;
    }
    FullImageToWalbDiffConverter c(64 * 1024 / LOGICAL_BLOCK_SIZE);
    c.convert(0, 1);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-full")
