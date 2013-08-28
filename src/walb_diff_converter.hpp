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
#include "walb_log.hpp"
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
    using LogpackHeader = log::PackHeader;
    using LogpackHeaderPtr = std::shared_ptr<LogpackHeader>;
    using LogpackDataRef = log::PackDataRef;
    using DiffRecord = RecordRaw;
    using DiffRecordPtr = std::shared_ptr<DiffRecord>;
    using DiffIo = IoData;
    using DiffIoPtr = std::shared_ptr<DiffIo>;

public:
    Converter() = default;
    ~Converter() noexcept = default;

    void convert(int inputLogFd, int outputWdiffFd,
                 uint16_t maxIoBlocks = uint16_t(-1)) {
        /* Wrap input. */
        cybozu::util::FdReader fdr(inputLogFd);

        /* Prepare walb diff. */
        MemoryData walbDiff(maxIoBlocks);

        /* Loop */
        uint64_t lsid = -1;
        uint64_t writtenBlocks = 0;
        while (convertWlog(lsid, writtenBlocks, fdr, walbDiff)) {}

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
     * @fdr input wlog reader.
     * @walbDiff walb diff memory manager.
     *
     * RETURN:
     *   true if wlog is remaining, or false.
     */
    bool convertWlog(
        uint64_t &lsid, uint64_t &writtenBlocks, cybozu::util::FdReader &fdr,
        MemoryData &walbDiff) {

        bool ret = true;

        /* Read walblog header. */
        log::FileHeader wlHeader;
        try {
            ::printf("try to read header.\n"); /* debug */
            wlHeader.read(fdr);
            ::printf("       read header.\n"); /* debug */
        } catch (cybozu::util::EofError &e) {
            return false;
        }
        if (!wlHeader.isValid()) {
            throw RT_ERR("walb log header invalid.");
        }
        wlHeader.print(::stderr); /* debug */

        /* Block buffer. */
        const unsigned int BUF_SIZE = 4 * 1024 * 1024;
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
            if (walbDiff.init()) {
                checkUuid();
            } else {
                /* Initialize uuid. */
                walbDiff.header().setUuid(wlHeader.uuid());
                walbDiff.sync();
            }
            lsid = wlHeader.beginLsid();
        } else {
            /* Second or more. */
            if (lsid != wlHeader.beginLsid()) {
                throw RT_ERR("lsid mismatch.");
            }
            checkUuid();
        }

        /* Convert each log. */
        while (lsid < wlHeader.endLsid()) {
            LogpackHeaderPtr loghp;
            try {
                //std::this_thread::sleep_for(std::chrono::milliseconds(100)); /* debug */
                ::printf("try to read logpack %" PRIu64 "\n", lsid); /* debug */
                loghp = readLogpackHeader(fdr, ba, wlHeader.salt());
                ::printf("       read logpack %" PRIu64 "\n", lsid); /* debug */
            } catch (cybozu::util::EofError &e) {
                ret = false;
                break;
            }
            LogpackHeader &logh = *loghp;
            for (size_t i = 0; i < logh.nRecords(); i++) {
                LogpackDataRef logd(logh, i);
                readLogpackData(fdr, ba, logd);
                //logd.print(); /* debug */
                DiffRecordPtr diffRec;
                DiffIoPtr diffIo;
                std::tie(diffRec, diffIo) = convertLogpackDataToDiffRecord(logd);
                if (diffRec) {
                    walbDiff.add(*diffRec, std::move(*diffIo));
                    writtenBlocks += diffRec->ioBlocks();
                }
            }
            lsid = logh.nextLogpackLsid();
        }

        ::printf("converted until lsid %" PRIu64 "\n", lsid);
        return ret;
    }

    /**
     * Convert a logpack data to a diff record.
     */
    std::pair<DiffRecordPtr, DiffIoPtr> convertLogpackDataToDiffRecord(LogpackDataRef &logd) {
        if (logd.isPadding()) {
            return std::make_pair(DiffRecordPtr(), DiffIoPtr());
        }

        std::shared_ptr<DiffRecord> p(new DiffRecord());
        DiffRecord &mrec = *p;
        mrec.setIoAddress(logd.record().offset);
        mrec.setIoBlocks(logd.record().io_size);

        if (logd.isDiscard()) {
            mrec.setDiscard();
            return std::make_pair(p, DiffIoPtr(new DiffIo()));
        }

        /* Copy data from logpack data to diff io data. */
        assert(0 < logd.ioSizeLb());
        const size_t ioSizeB = logd.ioSizeLb() * LOGICAL_BLOCK_SIZE;
        auto iop = std::make_shared<DiffIo>();
        iop->setIoBlocks(logd.ioSizeLb());
        std::vector<char> buf(ioSizeB);
        size_t remaining = ioSizeB;
        size_t off = 0;
        const unsigned int pbs = logd.pbs();
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            if (pbs <= remaining) {
                ::memcpy(&buf[off], logd.rawData<char>(i), pbs);
                off += pbs;
                remaining -= pbs;
            } else {
                ::memcpy(&buf[off], logd.rawData<char>(i), remaining);
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

    Block readBlock(
        cybozu::util::FdReader &fdr, cybozu::util::BlockAllocator<uint8_t> &ba) {
        Block b = ba.alloc();
        unsigned int bs = ba.blockSize();
        fdr.read(reinterpret_cast<char *>(b.get()), bs);
        return b;
    }

    LogpackHeaderPtr readLogpackHeader(
        cybozu::util::FdReader &fdr, cybozu::util::BlockAllocator<uint8_t> &ba,
        uint32_t salt) {

        Block b = readBlock(fdr, ba);
        return LogpackHeaderPtr(new LogpackHeader(b, ba.blockSize(), salt));
    }

    void readLogpackData(
        cybozu::util::FdReader &fdr,
        cybozu::util::BlockAllocator<uint8_t> &ba, LogpackDataRef &logd) {

        if (!logd.hasData()) { return; }
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            logd.addBlock(readBlock(fdr, ba));
        }
        if (!logd.isValid()) {
            logd.print(::stderr); /* debug */
            throw log::InvalidLogpackData();
        }
    }
};

}} //namespace walb::diff
