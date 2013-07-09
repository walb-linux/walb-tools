/**
 * @file
 * @brief Convert walb logs to a walb diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <random>
#include <stdexcept>
#include <cstdint>
#include <cinttypes>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <type_traits>

#include "cybozu/option.hpp"

#include "util.hpp"
#include "walb_log.hpp"
#include "walb_diff.hpp"
#include "simple_logger.hpp"

/**
 * Converter from walb logs to a walb diff.
 */
class WalbLogToDiff /* final */
{
private:
    using Block = std::shared_ptr<uint8_t>;
    using LogpackHeader = walb::log::WalbLogpackHeader;
    using LogpackHeaderPtr = std::shared_ptr<LogpackHeader>;
    using LogpackData = walb::log::WalbLogpackDataRef;
    using DiffRecord = walb::diff::WalbDiffRecord;
    using DiffRecordPtr = std::shared_ptr<DiffRecord>;
    using DiffIo = walb::diff::BlockDiffIo;
    using DiffIoPtr = std::shared_ptr<DiffIo>;

public:
    WalbLogToDiff() {}
    ~WalbLogToDiff() noexcept {}

    void convert(int inputLogFd, int outputWdiffFd,
                 uint16_t maxIoBlocks = uint16_t(-1)) {
        /* Wrap input. */
        cybozu::util::FdReader fdr(inputLogFd);

        /* Prepare walb diff. */
        walb::diff::WalbDiffMemory walbDiff(maxIoBlocks);

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
        walb::diff::WalbDiffMemory &walbDiff) {

        bool ret = true;

        /* Read walblog header. */
        walb::log::WalbLogFileHeader wlHeader;
        try {
            wlHeader.read(fdr);
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
                loghp = readLogpackHeader(fdr, ba, wlHeader.salt());
            } catch (cybozu::util::EofError &e) {
                ret = false;
                break;
            }
            LogpackHeader &logh = *loghp;
            for (size_t i = 0; i < logh.nRecords(); i++) {
                LogpackData logd(logh, i);
                readLogpackData(fdr, ba, logd);
                //logd.print(); /* debug */
                DiffRecordPtr diffRec;
                DiffIoPtr diffIo;
                std::tie(diffRec, diffIo) = convertLogpackDataToDiffRecord(logd);
                if (diffRec) {
                    bool ret = walbDiff.add(*diffRec, diffIo);
                    if (!ret) {
                        throw RT_ERR("add failed.");
                    }
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
    std::pair<DiffRecordPtr, DiffIoPtr> convertLogpackDataToDiffRecord(LogpackData &logd) {
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
        DiffIoPtr iop(new DiffIo(logd.ioSizeLb()));

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
            iop.reset(new DiffIo());
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
        cybozu::util::BlockAllocator<uint8_t> &ba, LogpackData &logd) {

        if (!logd.hasData()) { return; }
        for (size_t i = 0; i < logd.ioSizePb(); i++) {
            logd.addBlock(readBlock(fdr, ba));
        }
        if (!logd.isValid()) {
            logd.print(::stderr); /* debug */
            throw walb::log::InvalidLogpackData();
        }
    }
};

struct Option : public cybozu::Option
{
    uint32_t maxIoSize;
    Option() {
        //setUsage("Usage: wlog-to-wdiff < [wlogs] > [wdiff]");
        appendOpt(&maxIoSize, uint16_t(-1), "x", "max IO size in the output wdiff [byte].");
        appendHelp("h");
    }
};

int main(int argc, UNUSED char *argv[])
{
    try {
        Option opt;
        if (!opt.parse(argc, argv)) {
            opt.usage();
            return 1;
        }
        WalbLogToDiff l2d;
        l2d.convert(0, 1, opt.maxIoSize);
        return 0;
    } catch (std::runtime_error &e) {
        LOGe("%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        LOGe("%s\n", e.what());
        return 1;
    } catch (...) {
        LOGe("caught other error.\n");
        return 1;
    }
}

/* end of file. */
