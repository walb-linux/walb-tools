#pragma once
/**
 * @file
 * @brief Converter from wlog to wdiff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <memory>
#include <cassert>
#include <cstdio>
#include <cstring>

#include <chrono>
#include <thread>

#include "fileio.hpp"
#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "walb_diff_base.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_file.hpp"

namespace walb {

/**
 * Convert a logpack data to a diff data.
 *
 * RETURN:
 *   false if the pack IO is padding data.
 *   true if the pack IO is normal IO or discard or allzero.
 */
inline bool convertLogToDiff(
    uint32_t pbs, const WlogRecord &rec, const LogBlockShared& blockS,
    DiffRecord& mrec, DiffIo &diffIo)
{
    /* Padding */
    if (rec.isPadding()) return false;

    mrec.init();
    mrec.io_address = rec.offset;
    mrec.io_blocks = rec.io_size;

    /* Discard */
    if (rec.isDiscard()) {
        mrec.setDiscard();
        mrec.data_size = 0;
        diffIo.set(mrec);
        return true;
    }

    /* AllZero */
    if (blockS.isAllZero(rec.ioSizeLb())) {
        mrec.setAllZero();
        mrec.data_size = 0;
        diffIo.set(mrec);
        return true;
    }

    /* Copy data from logpack data to diff io data. */
    assert(0 < rec.ioSizeLb());
    const size_t ioSizeB = rec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
    AlignedArray buf(ioSizeB, false);
    size_t remaining = ioSizeB;
    size_t off = 0;
    const uint32_t ioSizePb = rec.ioSizePb(pbs);
    for (size_t i = 0; i < ioSizePb; i++) {
        const size_t copySize = std::min<size_t>(pbs, remaining);
        ::memcpy(&buf[off], blockS.get(i), copySize);
        off += copySize;
        remaining -= copySize;
    }
    diffIo.set(mrec);
    diffIo.data.swap(buf);

    /* Compression. (currently NONE). */
    mrec.compression_type = ::WALB_DIFF_CMPR_NONE;
    mrec.data_offset = 0;
    mrec.data_size = ioSizeB;

    /* Checksum. */
    mrec.checksum = diffIo.calcChecksum();

    return true;
}

/**
 * Converter from walb logs to a walb diff.
 */
class DiffConverter /* final */
{
public:
    void convert(int inputLogFd, int outputWdiffFd,
                 uint16_t maxIoBlocks = uint16_t(-1)) {
        /* Prepare walb diff. */
        DiffMemory diffMem(maxIoBlocks);

        /* Loop */
        uint64_t lsid = -1;
        uint64_t writtenBlocks = 0;
        while (convertWlog(lsid, writtenBlocks, inputLogFd, diffMem)) {}

#ifdef DEBUG
        /* finalize */
        try {
            LOGd_("Check no overlapped.\n"); /* debug */
            diffMem.checkNoOverlappedAndSorted(); /* debug */
        } catch (std::runtime_error &e) {
            LOGe("checkNoOverlapped failed: %s\n", e.what());
        }
#endif

        /* Get statistics */
        LOGd_("\n"
              "Written blocks: %" PRIu64 "\n"
              "nBlocks: %" PRIu64 "\n"
              "nIos: %" PRIu64 "\n"
              "lsid: %" PRIu64 "\n",
              writtenBlocks, diffMem.getNBlocks(),
              diffMem.getNIos(), lsid);

        diffMem.writeTo(outputWdiffFd, ::WALB_DIFF_CMPR_SNAPPY);
    }
private:
    /**
     * Convert a wlog.
     *
     * @lsid begin lsid.
     * @writtenBlocks written logical blocks.
     * @fd input wlog file descriptor.
     * @diffMem walb diff memory manager.
     *
     * RETURN:
     *   true if wlog is remaining, or false.
     */
    bool convertWlog(uint64_t &lsid, uint64_t &writtenBlocks, int fd, DiffMemory &diffMem) {
        WlogReader reader(fd);

        /* Read walblog header. */
        WlogFileHeader wlHeader;
        try {
            reader.readHeader(wlHeader);
        } catch (cybozu::util::EofError &e) {
            return false;
        }

        /* Block buffer. */
        const uint32_t pbs = wlHeader.pbs();

        /* Initialize walb diff db. */
        auto verifyUuid = [&]() {
            const cybozu::Uuid diffUuid = diffMem.header().getUuid();
            const cybozu::Uuid logUuid = wlHeader.getUuid();
            if (diffUuid != logUuid) {
                throw cybozu::Exception(__func__) << "uuid differ" << logUuid << diffUuid;
            }
        };
        if (lsid == uint64_t(-1)) {
            /* First time. */
            /* Initialize uuid. */
            diffMem.header().setUuid(wlHeader.getUuid());
            lsid = wlHeader.beginLsid();
        } else {
            /* Second or more. */
            if (lsid != wlHeader.beginLsid()) {
                throw RT_ERR("lsid mismatch.");
            }
            verifyUuid();
        }

        /* Convert each log. */
        WlogRecord lrec;
        LogBlockShared blockS;
        while (reader.readLog(lrec, blockS)) {
            DiffRecord diffRec;
            DiffIo diffIo;
            if (convertLogToDiff(pbs, lrec, blockS, diffRec, diffIo)) {
                diffMem.add(diffRec, std::move(diffIo));
                writtenBlocks += diffRec.io_blocks;
            }
        }

        lsid = reader.endLsid();
        ::fprintf(::stderr, "converted until lsid %" PRIu64 "\n", lsid);
        return true;
    }
};

} //namespace walb
