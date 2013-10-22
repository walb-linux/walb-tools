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
#include "memory_buffer.hpp"
#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "walb_diff_base.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_file.hpp"

namespace walb {
namespace diff {

/**
 * Converter from walb logs to a walb diff.
 */
class Converter /* final */
{
private:
    using Block = std::shared_ptr<uint8_t>;
    using LogpackHeader = log::PackHeaderRaw;
    using LogpackHeaderPtr = std::shared_ptr<LogpackHeader>;
    using DiffRecord = RecordRaw;
    using DiffRecordPtr = std::shared_ptr<DiffRecord>;
    using DiffIo = IoData;
    using DiffIoPtr = std::shared_ptr<DiffIo>;

public:
    Converter() = default;
    ~Converter() noexcept = default;

    void convert(int inputLogFd, int outputWdiffFd,
                 uint16_t maxIoBlocks = uint16_t(-1)) {
        /* Prepare walb diff. */
        MemoryData walbDiff(maxIoBlocks);

        /* Loop */
        uint64_t lsid = -1;
        uint64_t writtenBlocks = 0;
        while (convertWlog(lsid, writtenBlocks, inputLogFd, walbDiff)) {}

#ifdef DEBUG
        /* finalize */
        try {
            LOGd_("Check no overlapped.\n"); /* debug */
            walbDiff.checkNoOverlappedAndSorted(); /* debug */
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
              writtenBlocks, walbDiff.getNBlocks(),
              walbDiff.getNIos(), lsid);

        walbDiff.writeTo(outputWdiffFd, ::WALB_DIFF_CMPR_SNAPPY);
    }
private:
    /**
     * Convert a wlog.
     *
     * @lsid begin lsid.
     * @writtenBlocks written logical blocks.
     * @fd input wlog file descriptor.
     * @walbDiff walb diff memory manager.
     *
     * RETURN:
     *   true if wlog is remaining, or false.
     */
    bool convertWlog(uint64_t &lsid, uint64_t &writtenBlocks, int fd, MemoryData &walbDiff) {
        log::Reader reader(fd);

        /* Read walblog header. */
        log::FileHeader wlHeader;
        try {
            reader.readHeader(wlHeader);
        } catch (cybozu::util::EofError &e) {
            return false;
        }
#if 0
        wlHeader.print(::stderr); /* debug */
#endif

        /* Block buffer. */
        const unsigned int BUF_SIZE = 4U << 20;
        const unsigned int pbs = wlHeader.pbs();
        cybozu::util::BlockAllocator<uint8_t> ba(BUF_SIZE / pbs, pbs, pbs);

        /* Initialize walb diff db. */
        auto checkUuid = [&]() {
            if (::memcmp(walbDiff.header().getUuid(), wlHeader.uuid(), UUID_SIZE) != 0) {
                throw RT_ERR("Uuid mismatch.");
            }
        };
        if (lsid == uint64_t(-1)) {
            /* First time. */
            /* Initialize uuid. */
            walbDiff.header().setUuid(wlHeader.uuid());
            lsid = wlHeader.beginLsid();
        } else {
            /* Second or more. */
            if (lsid != wlHeader.beginLsid()) {
                throw RT_ERR("lsid mismatch.");
            }
            checkUuid();
        }

        /* Convert each log. */
        while (!reader.isEnd()) {
            log::PackIoRaw packIo;
            if (reader.isFirstInPack()) {
                lsid = reader.packHeader().nextLogpackLsid();
            }
            reader.readLog(packIo);

            DiffRecordPtr diffRec;
            DiffIoPtr diffIo;
            std::tie(diffRec, diffIo) = convertLogpackDataToDiffRecord(packIo);
            if (diffRec) {
                walbDiff.add(*diffRec, std::move(*diffIo));
                writtenBlocks += diffRec->ioBlocks();
            }
        }

        lsid = reader.endLsid();
        ::fprintf(::stderr, "converted until lsid %" PRIu64 "\n", lsid);
        return true;
    }

    /**
     * Convert a logpack data to a diff record.
     */
    std::pair<DiffRecordPtr, DiffIoPtr> convertLogpackDataToDiffRecord(log::PackIoRef<log::RecordRaw> &packIo) {
        const log::RecordRaw &rec = packIo.record();
        log::BlockData &blockD = packIo.blockData();

        if (rec.isPadding()) {
            return std::make_pair(DiffRecordPtr(), DiffIoPtr());
        }

        std::shared_ptr<DiffRecord> p(new DiffRecord());
        DiffRecord &mrec = *p;
        mrec.setIoAddress(rec.record().offset);
        mrec.setIoBlocks(rec.record().io_size);

        if (rec.isDiscard()) {
            mrec.setDiscard();
            return std::make_pair(p, DiffIoPtr(new DiffIo()));
        }

        /* Copy data from logpack data to diff io data. */
        assert(0 < rec.ioSizeLb());
        const size_t ioSizeB = rec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
        auto iop = std::make_shared<DiffIo>();
        iop->setIoBlocks(rec.ioSizeLb());
        std::vector<char> buf(ioSizeB);
        size_t remaining = ioSizeB;
        size_t off = 0;
        const unsigned int pbs = rec.pbs();
        for (size_t i = 0; i < rec.ioSizePb(); i++) {
            if (pbs <= remaining) {
                ::memcpy(&buf[off], blockD.rawData<char>(i), pbs);
                off += pbs;
                remaining -= pbs;
            } else {
                ::memcpy(&buf[off], blockD.rawData<char>(i), remaining);
                off += remaining;
                remaining = 0;
            }
        }
        assert(remaining == 0);
        assert(off == ioSizeB);
        iop->moveFrom(std::move(buf));

        /* All-zero. */
        if (iop->calcIsAllZero()) {
            mrec.setAllZero();
            mrec.setDataSize(0);
            *iop = DiffIo();
            return std::make_pair(p, iop);
        }

        /* Checksum. */
        mrec.setChecksum(iop->calcChecksum());

        /* Compression. (currently NONE). */
        mrec.setCompressionType(::WALB_DIFF_CMPR_NONE);
        mrec.setDataOffset(0);
        mrec.setDataSize(ioSizeB);

        return std::make_pair(p, iop);
    }
};

}} //namespace walb::diff
