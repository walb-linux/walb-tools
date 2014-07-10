/**
 * @file
 * @brief walb log restore tool for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "walb_log_file.hpp"
#include "wdev_log.hpp"
#include "walb_util.hpp"

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

    using AlignedArray = walb::AlignedArray;
    using WlogHeader = walb::log::FileHeader;
    using LogPackHeader = walb::LogPackHeader;
    using LogPackIo = walb::LogPackIo;
    using File = cybozu::util::File;
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
        File fileR(fdIn);
        WlogHeader wlHead;
        wlHead.readFrom(fileR);
        if (!wlHead.isValid()) {
            throw RT_ERR("Walb log file header is invalid.");
        }
        const uint32_t pbs = wlHead.pbs();

        /* Open the log device. */
        cybozu::FilePath path(config_.ldevPath());
        if (!path.stat().isBlock()) {
            ::fprintf(
                ::stderr,
                "Warning: the log device does not seem to be block device.\n");
        }
        File fileB(config_.ldevPath(), O_RDWR);

        /* Load superblock. */
        SuperBlock super;
        super.read(fileB.fd());

        /* Check physical block size. */
        if (super.getPhysicalBlockSize() != pbs) {
            throw RT_ERR("Physical block size differs. super %u head %u\n"
                         , super.getPhysicalBlockSize(), pbs);
        }

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
                       fileR, fileB, super, wlHead, restoredLsid)) {}
        } catch (cybozu::util::EofError &e) {
            ::printf("Reached input EOF.\n");
        }

        /* Create and write superblock finally. */
        super.setOldestLsid(beginLsid);
        super.setWrittenLsid(beginLsid); /* for redo */
        super.setUuid(wlHead.getUuid());
        super.setLogChecksumSalt(wlHead.salt());
        super.write(fileB.fd());

        /* Invalidate the last log block. */
        if (beginLsid < restoredLsid) {
            invalidateLsid(fileB, super, pbs, restoredLsid);
        }
        /* Invalidate the specified block. */
        if (config_.invalidLsid() != uint64_t(-1)) {
            invalidateLsid(fileB, super, pbs, config_.invalidLsid());
        }

        /* Finalize the log device. */
        fileB.fdatasync();
        fileB.close();

        ::printf("Restored lsid range [%" PRIu64 ", %" PRIu64 "].\n",
                 beginLsid, restoredLsid);
    }
private:
    /**
     * Invalidate a specified lsid.
     */
    void invalidateLsid(
        File &file, SuperBlock &super,
        uint32_t pbs, uint64_t lsid) {
        uint64_t off = super.getOffsetFromLsid(lsid);
        AlignedArray b(pbs);
        file.pwrite(b.data(), pbs, off * pbs);
    }

    /**
     * Read a block data from a fd reader.
     */
    AlignedArray readBlock(File& fileR, uint32_t pbs) {
        AlignedArray b(pbs);
        fileR.read(b.data(), pbs);
        return b;
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(LogPackIo &packIo, File &fileR, uint32_t pbs) {
        const walb::LogRecord &rec = packIo.rec;
        if (!rec.hasData()) return;
        const uint32_t ioSizePb = rec.ioSizePb(pbs);
        for (size_t i = 0; i < ioSizePb; i++) {
            packIo.blockS.addBlock(readBlock(fileR, pbs));
        }
        if (!packIo.isValid()) {
            throw walb::log::InvalidIo();
        }
    }

    /**
     * Read a logpack and restore it.
     *
     * @fileR wlog input.
     * @fileB log block device.
     * @super super block (with the blkdev).
     * @wlHead wlog header.
     * @restoresLsid lsid of the next logpack will be set if restored.
     *
     * RETURN:
     *   true in success, or false.
     */
    bool readLogpackAndRestore(
        File &fileR, File &fileB,
        SuperBlock &super, WlogHeader &wlHead,
        uint64_t &restoredLsid) {

        const uint32_t salt = wlHead.salt();
        const uint32_t pbs = wlHead.pbs();

        /* Read logpack header. */
        LogPackHeader packH(readBlock(fileR, pbs), pbs, salt);
        if (packH.isEnd()) return false;
        if (!packH.isValid()) return false;
        if (config_.isVerbose()) {
            std::cout << packH << std::endl;
        }
        const uint64_t originalLsid = packH.logpackLsid();
        if (config_.endLsid() <= originalLsid) return false;
        /* Update lsid if necessary. */
        if (lsidDiff_ != 0) {
            if (!packH.updateLsid(packH.logpackLsid() + lsidDiff_)) {
                ::fprintf(::stderr, "lsid overflow ocurred.\n");
                return false;
            }
        }

        /* Padding check. */
        uint64_t offPb = super.getOffsetFromLsid(packH.logpackLsid());
        const uint64_t endOffPb = super.getRingBufferOffset() +
            super.getRingBufferSize();
        if (endOffPb < offPb + 1 + packH.totalIoSize()) {
            /* Create and write padding logpack. */
            uint32_t paddingPb = endOffPb - offPb;
            assert(0 < paddingPb);
            assert(paddingPb < (1U << 16));
            LogPackHeader paddingPackH(pbs, wlHead.salt());
            paddingPackH.init(packH.logpackLsid());
            paddingPackH.addPadding(paddingPb - 1);
            paddingPackH.updateChecksum();
            assert(paddingPackH.isValid());
            fileB.pwrite(paddingPackH.rawData(), pbs, offPb * pbs);

            /* Update logh's lsid information. */
            lsidDiff_ += paddingPb;
            if (!packH.updateLsid(packH.logpackLsid() + paddingPb)) {
                ::fprintf(::stderr, "lsid overflow ocurred.\n");
                return false;
            }
            assert(super.getOffsetFromLsid(packH.logpackLsid())
                   == super.getRingBufferOffset());
            offPb = super.getRingBufferOffset();
        }

        /* Read logpack data. */
        std::vector<AlignedArray> blocks;
        blocks.reserve(packH.totalIoSize());
        for (size_t i = 0; i < packH.nRecords(); i++) {
            LogPackIo packIo;
            packIo.set(packH, i);
            readLogpackData(packIo, fileR, pbs);
            walb::LogRecord &rec = packIo.rec;
            if (rec.hasData()) {
                const uint32_t ioSizePb = rec.ioSizePb(pbs);
                for (size_t j = 0; j < ioSizePb; j++) {
                    blocks.push_back(std::move(packIo.blockS.getBlock(j)));
                }
            }
            if (0 < config_.ddevLb() &&
                config_.ddevLb() < rec.offset + rec.ioSizeLb()) {
                /* This IO should be clipped. */
                rec.setPadding();
                rec.offset = 0;
                rec.checksum = 0;
            }
        }
        assert(blocks.size() == packH.totalIoSize());

        if (originalLsid < config_.beginLsid()) {
            /* Skip to restore. */
            return true;
        }

        /* Restore. */
        packH.updateChecksum();
        assert(packH.isValid());
        assert(offPb + 1 + packH.totalIoSize() <= endOffPb);

        if (config_.isVerbose()) {
            ::printf("header %u records\n", packH.nRecords());
            ::printf("offPb %" PRIu64 "\n", offPb);
        }
        fileB.pwrite(packH.rawData(), pbs, offPb * pbs);
        for (size_t i = 0; i < blocks.size(); i++) {
            fileB.pwrite(blocks[i].data(), pbs, (offPb + 1 + i) * pbs);
        }

        if (config_.isVerify()) {
            /* Currently only header block will be verified. */
            AlignedArray b2(pbs);
            fileB.pread(b2.data(), pbs, offPb * pbs);
            LogPackHeader logh2(std::move(b2), pbs, salt);
            int ret = ::memcmp(packH.rawData(), logh2.rawData(), pbs);
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

        restoredLsid = packH.logpackLsid() + 1 + packH.totalIoSize();
        return true;
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    config.check();
    WalbLogRestorer wlRes(config);
    wlRes.restore(0);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-restore")
