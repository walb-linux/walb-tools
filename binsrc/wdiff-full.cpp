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
#include "memory_buffer.hpp"

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
        cybozu::util::FdReader reader(inFd);
        walb::diff::Writer writer(outFd);
        struct walb_diff_file_header rawHead;
        walb::diff::FileHeaderRef head(rawHead);

        head.init();
        head.setMaxIoBlocksIfNecessary(ioBlocks_);
        writer.writeHeader(head);

        uint64_t ioAddr = 0;
        uint16_t blks = readChunk(reader);
        while (0 < blks) {
            auto iop = std::make_shared<walb::diff::IoData>();
            iop->setIoBlocks(blks);
            iop->copyFrom(&buf0_[0], blks * LOGICAL_BLOCK_SIZE);
            walb::diff::RecordRaw rec;
            rec.setIoAddress(ioAddr);
            rec.setIoBlocks(blks);
            rec.setNormal();
            rec.setDataSize(blks * LOGICAL_BLOCK_SIZE);
            rec.setChecksum(iop->calcChecksum());

            writer.writeDiff(rec, iop);

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
    uint16_t readChunk(cybozu::util::FdReader &reader) {
        uint16_t c = 0;
        char *p = &buf0_[0];
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

int main(int argc, UNUSED char *argv[])
{
    try {
        if (1 < argc) {
            ::printf("Usage: wdiff-full < [full image] > [wdiff]\n");
            return 1;
        }
        FullImageToWalbDiffConverter c(64 * 1024 / LOGICAL_BLOCK_SIZE);
        c.convert(0, 1);
        return 0;
    } catch (std::runtime_error &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
        return 1;
    }
}

/* end of file. */
