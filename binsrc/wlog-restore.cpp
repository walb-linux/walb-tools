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

using namespace walb;

/**
 * Command line configuration.
 */
struct Option
{
    std::string ldevPath; /* Log device to restore wlog. */
    uint64_t beginLsid; /* start lsid to restore. */
    uint64_t endLsid; /* end lsid to restore. The range is [start, end). */
    int64_t lsidDiff; /* 0 means no change. */
    uint64_t invalidLsid; /* -1 means no invalidation. */
    uint64_t ddevLb; /* 0 means no clipping. */
    bool isVerify;
    bool isVerbose;
    bool isDebug;

    Option(int argc, char* argv[])
        : ldevPath()
        , beginLsid(0)
        , endLsid(-1)
        , lsidDiff(0)
        , invalidLsid(-1)
        , ddevLb(0)
        , isVerify(false)
        , isVerbose(false)
        , isDebug(false) {

        cybozu::Option opt;
        opt.setDescription("Wlresotre: restore walb log to a log device.");
        opt.appendOpt(&beginLsid, 0, "b", "LSID:  begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendOpt(&lsidDiff, 0, "d", "DIFF: lsid diff. (default: 0)");
        opt.appendOpt(&invalidLsid, uint64_t(-1), "i", "LSID:invalidate lsid after restore. (default: no invalidation)");
        opt.appendOpt(&ddevLb, 0, "s", "SIZE: data device size for clipping. (default: no clipping)");
        opt.appendBoolOpt(&isVerify, "-verify", ": verify written logpack (default: no)");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug logs.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&ldevPath, "LOG_DEVICE_PATH < WLOG_FILE");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (beginLsid >= endLsid) {
            throw RT_ERR("beginLsid must be < endLsid.");
        }
    }
};

/**
 * WalbLog Generator for test.
 */
class WalbLogRestorer
{
private:
    const Option& opt_;
    int64_t lsidDiff_;

public:
    WalbLogRestorer(const Option& opt)
        : opt_(opt)
        , lsidDiff_(opt.lsidDiff)
    {}

    /**
     * Restore the log to the device.
     */
    void restore(int fdIn) {
        /* Read walb log file header from stdin. */
        cybozu::util::File fileR(fdIn);
        log::FileHeader wlHead;
        wlHead.readFrom(fileR);
        if (!wlHead.isValid()) {
            throw RT_ERR("Walb log file header is invalid.");
        }
        const uint32_t pbs = wlHead.pbs();

        /* Open the log device. */
        cybozu::FilePath path(opt_.ldevPath);
        if (!path.stat().isBlock()) {
            ::fprintf(
                ::stderr,
                "Warning: the log device does not seem to be block device.\n");
        }
        cybozu::util::File fileB(opt_.ldevPath, O_RDWR);

        /* Load superblock. */
        device::SuperBlock super;
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
        if (opt_.invalidLsid != uint64_t(-1)) {
            invalidateLsid(fileB, super, pbs, opt_.invalidLsid);
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
        cybozu::util::File &file, device::SuperBlock &super,
        uint32_t pbs, uint64_t lsid) {
        uint64_t off = super.getOffsetFromLsid(lsid);
        AlignedArray b(pbs);
        file.pwrite(b.data(), pbs, off * pbs);
    }

    /**
     * Read a block data from a fd reader.
     */
    AlignedArray readBlock(cybozu::util::File& fileR, uint32_t pbs) {
        AlignedArray b(pbs);
        fileR.read(b.data(), pbs);
        return b;
    }

    /**
     * Read a logpack data.
     */
    void readLogpackData(LogPackIo &packIo, cybozu::util::File &fileR, uint32_t pbs) {
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
        cybozu::util::File &fileR, cybozu::util::File &fileB,
        device::SuperBlock &super, log::FileHeader &wlHead,
        uint64_t &restoredLsid) {

        const uint32_t salt = wlHead.salt();
        const uint32_t pbs = wlHead.pbs();

        /* Read logpack header. */
        LogPackHeader packH(readBlock(fileR, pbs), pbs, salt);
        if (packH.isEnd()) return false;
        if (!packH.isValid()) return false;
        if (opt_.isVerbose) {
            std::cout << packH << std::endl;
        }
        const uint64_t originalLsid = packH.logpackLsid();
        if (opt_.endLsid <= originalLsid) return false;
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
            if (0 < opt_.ddevLb &&
                opt_.ddevLb < rec.offset + rec.ioSizeLb()) {
                /* This IO should be clipped. */
                rec.setPadding();
                rec.offset = 0;
                rec.checksum = 0;
            }
        }
        assert(blocks.size() == packH.totalIoSize());

        if (originalLsid < opt_.beginLsid) {
            /* Skip to restore. */
            return true;
        }

        /* Restore. */
        packH.updateChecksum();
        assert(packH.isValid());
        assert(offPb + 1 + packH.totalIoSize() <= endOffPb);

        if (opt_.isVerbose) {
            ::printf("header %u records\n", packH.nRecords());
            ::printf("offPb %" PRIu64 "\n", offPb);
        }
        fileB.pwrite(packH.rawData(), pbs, offPb * pbs);
        for (size_t i = 0; i < blocks.size(); i++) {
            fileB.pwrite(blocks[i].data(), pbs, (offPb + 1 + i) * pbs);
        }

        if (opt_.isVerify) {
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
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    WalbLogRestorer wlRes(opt);
    wlRes.restore(0);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-restore")
