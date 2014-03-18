/**
 * @file
 * @brief walb log restore tool for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <cstdint>
#include <queue>
#include <memory>
#include <deque>
#include <algorithm>
#include <utility>
#include <set>
#include <limits>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "memory_buffer.hpp"
#include "walb_log_file.hpp"
#include "walb_log_dev.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    std::string ldevPath_; /* Log device to restore wlog. */
    uint64_t beginLsid_; /* start lsid to restore. */
    uint64_t endLsid_; /* end lsid to restore. The range is [start, end). */
    int64_t lsidDiff_; /* 0 means no change. */
    uint64_t invalidLsid_; /* -1 means no invalidation. */
    uint64_t ddevLb_; /* 0 means no clipping. */
    bool isVerify_;
    bool isVerbose_;
public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , beginLsid_(0)
        , endLsid_(-1)
        , lsidDiff_(0)
        , invalidLsid_(-1)
        , ddevLb_(0)
        , isVerify_(false)
        , isVerbose_(false) {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    int64_t lsidDiff() const { return lsidDiff_; }
    uint64_t invalidLsid() const { return invalidLsid_; }
    uint64_t ddevLb() const { return ddevLb_; }
    bool isVerify() const { return isVerify_; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (beginLsid() >= endLsid()) {
            throw RT_ERR("beginLsid must be < endLsid.");
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlresotre: restore walb log to a log device.");
        opt.appendOpt(&beginLsid_, 0, "b", "LSID:  begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid_, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendOpt(&lsidDiff_, 0, "d", "DIFF: lsid diff. (default: 0)");
        opt.appendOpt(&invalidLsid_, uint64_t(-1), "i", "LSID:invalidate lsid after restore. (default: no invalidation)");
        opt.appendOpt(&ddevLb_, 0, "s", "SIZE: data device size for clipping. (default: no clipping)");
        opt.appendBoolOpt(&isVerify_, "-verify", ": verify written logpack (default: no)");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&ldevPath_, "LOG_DEVICE_PATH < WLOG_FILE");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

/**
 * WalbLog Generator for test.
 */
class WalbLogRestorer
{
private:
    const Config& config_;
    int64_t lsidDiff_;

    using Block = std::shared_ptr<uint8_t>;
    using BlockA = cybozu::util::BlockAllocator<uint8_t>;
    using BlockDev = cybozu::util::BlockDevice;
    using WlogHeader = walb::log::FileHeader;
    using PackHeader = walb::log::PackHeaderRaw;
    using PackIo = walb::log::PackIoRaw<walb::log::BlockDataShared>;
    using FdReader = cybozu::util::FdReader;
    using SuperBlock = walb::device::SuperBlock;

public:
    WalbLogRestorer(const Config& config)
        : config_(config)
        , lsidDiff_(config.lsidDiff())
        {}

    /**
     * Restore the log to the device.
     */
    void restore(int fdIn) {
        /* Read walb log file header from stdin. */
        FdReader fdr(fdIn);
        WlogHeader wlHead;
        wlHead.read(fdr);
        if (!wlHead.isValid()) {
            throw RT_ERR("Walb log file header is invalid.");
        }
        const unsigned int pbs = wlHead.pbs();

        /* Open the log device. */
        BlockDev blkdev(config_.ldevPath(), O_RDWR);
        if (!blkdev.isBlockDevice()) {
            ::fprintf(
                ::stderr,
                "Warning: the log device does not seem to be block device.\n");
        }

        /* Load superblock. */
        SuperBlock super(blkdev);

        /* Check physical block size. */
        if (super.getPhysicalBlockSize() != pbs) {
            throw RT_ERR("Physical block size differs.\n");
        }

        /* Allocate buffer for logpacks. */
        const unsigned int BUFFER_SIZE = 16 * 1024 * 1024; /* 16MB */
        BlockA ba(BUFFER_SIZE / pbs, pbs, pbs);

        /* Set lsid range. */
        uint64_t beginLsid = wlHead.beginLsid() + lsidDiff_;
        ::printf("Try to restore lsid range [%" PRIu64 ", %" PRIu64 ")\n",
                 wlHead.beginLsid(), wlHead.endLsid());
        if (lsidDiff_ != 0) {
            ::printf("Lsid map %" PRIu64 " to %" PRIu64 " (diff %" PRIi64 ")\n",
                     wlHead.beginLsid(), beginLsid, lsidDiff_);
        }
        uint64_t restoredLsid = beginLsid;

        /* Read and write each logpack. */
        try {
            while (readLogpackAndRestore(
                       fdr, blkdev, super, ba, wlHead, restoredLsid)) {}
        } catch (cybozu::util::EofError &e) {
            ::printf("Reached input EOF.\n");
        }

        /* Create and write superblock finally. */
        super.setOldestLsid(beginLsid);
        super.setWrittenLsid(beginLsid); /* for redo */
        super.setUuid(wlHead.uuid());
        super.setLogChecksumSalt(wlHead.salt());
        super.write();

        /* Invalidate the last log block. */
        if (beginLsid < restoredLsid) {
            invalidateLsid(blkdev, super, ba, pbs, restoredLsid);
        }
        /* Invalidate the specified block. */
        if (config_.invalidLsid() != uint64_t(-1)) {
            invalidateLsid(blkdev, super, ba, pbs, config_.invalidLsid());
        }

        /* Finalize the log device. */
        blkdev.fdatasync();
        blkdev.close();

        ::printf("Restored lsid range [%" PRIu64 ", %" PRIu64 "].\n",
                 beginLsid, restoredLsid);
    }
private:
    /**
     * Invalidate a specified lsid.
     */
    void invalidateLsid(
        BlockDev &blkdev, SuperBlock &super, BlockA &ba,
        unsigned int pbs, uint64_t lsid) {
        uint64_t off = super.getOffsetFromLsid(lsid);
        Block b = ba.alloc();
        ::memset(b.get(), 0, pbs);
        blkdev.write(off * pbs, pbs, reinterpret_cast<const char *>(b.get()));
    }

    /**
     * Read a block data from a fd reader.
     */
    Block readBlock(FdReader& fdr, BlockA& ba, unsigned int pbs) {
        Block b = ba.alloc();
        if (b.get() == nullptr) {
            throw RT_ERR("allocate failed.");
        }
        fdr.read(reinterpret_cast<char *>(b.get()), pbs);
        return b;
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(PackIo &packIo, FdReader &fdr, BlockA &ba) {
        const walb::log::Record &rec = packIo.record();
        if (!rec.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        for (size_t i = 0; i < rec.ioSizePb(); i++) {
            packIo.blockData().addBlock(readBlock(fdr, ba, rec.pbs()));
        }
        if (!packIo.isValid()) {
            throw walb::log::InvalidIo();
        }
    }

    /**
     * Read a logpack and restore it.
     *
     * @fdr wlog input.
     * @blkdev log block device.
     * @super super block (with the blkdev).
     * @ba block allocator.
     * @wlHead wlog header.
     * @restoresLsid lsid of the next logpack will be set if restored.
     *
     * RETURN:
     *   true in success, or false.
     */
    bool readLogpackAndRestore(
        FdReader &fdr, BlockDev &blkdev,
        SuperBlock &super, BlockA &ba, WlogHeader &wlHead,
        uint64_t &restoredLsid) {

        uint32_t salt = wlHead.salt();
        unsigned int pbs = wlHead.pbs();

        /* Read logpack header. */
        PackHeader logh(readBlock(fdr, ba, pbs), pbs, salt);
        if (logh.isEnd()) return false;
        if (!logh.isValid()) return false;
        if (config_.isVerbose()) logh.printShort();
        const uint64_t originalLsid = logh.logpackLsid();
        if (config_.endLsid() <= originalLsid) return false;
        /* Update lsid if necessary. */
        if (lsidDiff_ != 0) {
            if (!logh.updateLsid(logh.logpackLsid() + lsidDiff_)) {
                ::fprintf(::stderr, "lsid overflow ocurred.\n");
                return false;
            }
        }

        /* Padding check. */
        uint64_t offPb = super.getOffsetFromLsid(logh.logpackLsid());
        const uint64_t endOffPb = super.getRingBufferOffset() +
            super.getRingBufferSize();
        if (endOffPb < offPb + 1 + logh.totalIoSize()) {
            /* Create and write padding logpack. */
            unsigned int paddingPb = endOffPb - offPb;
            assert(0 < paddingPb);
            assert(paddingPb < (1U << 16));
            PackHeader paddingLogh(ba.alloc(), pbs, wlHead.salt());
            paddingLogh.init(logh.logpackLsid());
            paddingLogh.addPadding(paddingPb - 1);
            paddingLogh.updateChecksum();
            assert(paddingLogh.isValid());
            blkdev.write(
                offPb * pbs, pbs, paddingLogh.ptr<char>());

            /* Update logh's lsid information. */
            lsidDiff_ += paddingPb;
            if (!logh.updateLsid(logh.logpackLsid() + paddingPb)) {
                ::fprintf(::stderr, "lsid overflow ocurred.\n");
                return false;
            }
            assert(super.getOffsetFromLsid(logh.logpackLsid())
                   == super.getRingBufferOffset());
            offPb = super.getRingBufferOffset();
        }

        /* Read logpack data. */
        std::vector<Block> blocks;
        blocks.reserve(logh.totalIoSize());
        for (size_t i = 0; i < logh.nRecords(); i++) {
            PackIo packIo(logh, i);
            readLogpackData(packIo, fdr, ba);
            walb::log::Record &rec = packIo.record();
            if (rec.hasData()) {
                for (size_t j = 0; j < rec.ioSizePb(); j++) {
                    blocks.push_back(packIo.blockData().getBlock(j));
                }
            }
            if (0 < config_.ddevLb() &&
                config_.ddevLb() < rec.offset() + rec.ioSizeLb()) {
                /* This IO should be clipped. */
                rec.setPadding();
                rec.record().offset = 0;
                rec.record().checksum = 0;
            }
        }
        assert(blocks.size() == logh.totalIoSize());

        if (originalLsid < config_.beginLsid()) {
            /* Skip to restore. */
            return true;
        }

        /* Restore. */
        logh.updateChecksum();
        assert(logh.isValid());
        assert(offPb + 1 + logh.totalIoSize() <= endOffPb);

        if (config_.isVerbose()) {
            ::printf("header %u records\n", logh.nRecords());
            ::printf("offPb %" PRIu64 "\n", offPb);
        }
        blkdev.write(offPb * pbs, pbs, logh.ptr<char>());
        for (size_t i = 0; i < blocks.size(); i++) {
            blkdev.write((offPb + 1 + i) * pbs, pbs,
                         reinterpret_cast<const char *>(blocks[i].get()));
        }

        if (config_.isVerify()) {
            /* Currently only header block will be verified. */
            Block b2 = ba.alloc();
            blkdev.read(
                offPb * pbs, pbs,
                reinterpret_cast<char *>(b2.get()));
            PackHeader logh2(b2, pbs, salt);
            int ret = ::memcmp(logh.ptr<char>(), logh2.ptr<char>(), pbs);
            if (ret) {
                throw RT_ERR("Logpack header verification failed: "
                             "lsid %" PRIu64 " offPb %" PRIu64 ".",
                             logh2.logpackLsid(), offPb);
            }
            if (!logh2.isValid()) {
                throw RT_ERR("Stored logpack header Invalid: "
                             "lsid %" PRIu64 " offPb %" PRIu64 ".",
                             logh2.logpackLsid(), offPb);
            }
        }

        restoredLsid = logh.logpackLsid() + 1 + logh.totalIoSize();
        return true;
    }
};

int main(int argc, char* argv[]) try
{
    cybozu::SetLogFILE(::stderr);

    Config config(argc, argv);
    config.check();

    WalbLogRestorer wlRes(config);
    wlRes.restore(0);
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
