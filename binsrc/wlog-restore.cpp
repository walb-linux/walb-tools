/**
 * @file
 * @brief walb log restore tool for test.
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
    uint64_t bgnLsid; /* start lsid to restore. */
    uint64_t endLsid; /* end lsid to restore. The range is [start, end). */
    int64_t lsidDiff; /* 0 means no change. */
    uint64_t invalidLsid; /* -1 means no invalidation. */
    uint64_t ddevLb; /* 0 means no clipping. */
    bool isVerify;
    bool isVerbose;
    bool isDebug;

    Option(int argc, char* argv[])
        : ldevPath()
        , bgnLsid(0)
        , endLsid(-1)
        , lsidDiff(0)
        , invalidLsid(-1)
        , ddevLb(0)
        , isVerify(false)
        , isVerbose(false)
        , isDebug(false) {

        cybozu::Option opt;
        opt.setDescription("Wlresotre: restore walb log to a log device.");
        opt.appendOpt(&bgnLsid, 0, "b", "LSID:  begin lsid to restore. (default: 0)");
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

        if (bgnLsid >= endLsid) {
            throw RT_ERR("bgnLsid must be < endLsid.");
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

public:
    WalbLogRestorer(const Option& opt)
        : opt_(opt) {
    }
    /**
     * Restore the log to the device.
     */
    void restore(LogFile &wlogFile) {
        /* Read walb log file header from stdin. */
        WlogFileHeader wlHead;
        wlHead.readFrom(wlogFile);
        const uint32_t pbs = wlHead.pbs();
        const uint32_t salt = wlHead.salt();
        int64_t lsidDiff = opt_.lsidDiff;

        /* Open the log device. */
        cybozu::FilePath path(opt_.ldevPath);
        if (!path.stat().isBlock()) {
            ::fprintf(::stderr, "Warning: the log device does not seem to be block device.\n");
        }
        cybozu::util::File ldevFile(opt_.ldevPath, O_RDWR);

        /* Load superblock. */
        device::SuperBlock super;
        super.read(ldevFile.fd());
        if (super.pbs() != pbs) {
            throw RT_ERR("Physical block size differs. super %u head %u\n"
                         , super.pbs(), pbs);
        }

        /* Set lsid range. */
        const uint64_t bgnLsidX = wlHead.beginLsid() + lsidDiff;
        ::printf("Try to restore lsid range [%" PRIu64 ", %" PRIu64 ")\n",
                 wlHead.beginLsid(), wlHead.endLsid());
        if (lsidDiff != 0) {
            ::printf("Lsid map %" PRIu64 " to %" PRIu64 " (diff %" PRIi64 ")\n",
                     wlHead.beginLsid(), bgnLsidX, lsidDiff);
        }

        /* Read and write each logpack. */
        uint64_t lsid = wlHead.beginLsid();
        LogPackHeader packH(pbs, salt);
        std::queue<LogBlockShared> ioQ;
        while (lsid < wlHead.endLsid()) {
            if (!readLogPackHeader(wlogFile, packH, lsid)) break;
            clipIfNecessary(packH);
            uint64_t nextLsid = packH.nextLogpackLsid();
            if (lsid < opt_.bgnLsid) {
                /* Skip to restore. */
                if (opt_.isVerbose) std::cout << "SKIP " << packH << std::endl;
                skipAllLogIos(wlogFile, packH);
                lsid = nextLsid;
                continue;
            }
            if (opt_.isVerbose) std::cout << packH << std::endl;
            if (!readAllLogIos(wlogFile, packH, ioQ)) {
                throw cybozu::Exception(__func__) << "wlog file corrupt" << lsid;
            }
            packH.updateLsid(lsid + lsidDiff);
            const uint32_t paddingPb = writePaddingIfNecessary(ldevFile, super, packH);
            LOGs.debug() << "paddingPb" << paddingPb;
            if (paddingPb > 0) {
                lsidDiff += paddingPb;
                packH.updateLsid(lsid + lsidDiff);
            }
            packH.updateChecksum();
            restorePack(ldevFile, super, packH, std::move(ioQ));
            assert(ioQ.empty());
            if (opt_.isVerify) verifyPack(ldevFile, super, packH);
            lsid = nextLsid;
        }

        /* Create and write superblock finally. */
        super.setOldestLsid(bgnLsidX);
        super.setWrittenLsid(bgnLsidX); /* for redo */
        super.setUuid(wlHead.getUuid());
        super.setLogChecksumSalt(salt);
        super.write(ldevFile.fd());

        /* Invalidate the last log block. */
        const uint64_t endLsidX = lsid + lsidDiff;
        if (bgnLsidX < endLsidX) {
            invalidateLsid(ldevFile, super, pbs, endLsidX);
        }
        /* Invalidate the specified block. */
        if (opt_.invalidLsid != uint64_t(-1)) {
            invalidateLsid(ldevFile, super, pbs, opt_.invalidLsid);
        }

        /* Finalize the log device. */
        ldevFile.fdatasync();
        ldevFile.close();

        ::printf("Restored lsid range [%" PRIu64 ", %" PRIu64 "]\n"
                 "Wlog lsid range [%" PRIu64 ", %" PRIu64 "]\n"
                 "Lsid diff start %" PRIi64 " end %" PRIi64 "\n"
                 , bgnLsidX, endLsidX
                 , bgnLsidX - opt_.lsidDiff, endLsidX - lsidDiff
                 , opt_.lsidDiff, lsidDiff);
    }
private:
    /**
     * Invalidate a specified lsid.
     */
    void invalidateLsid(
        cybozu::util::File &ldevFile, const device::SuperBlock &super,
        uint32_t pbs, uint64_t lsid) {

        const uint64_t offPb = super.getOffsetFromLsid(lsid);
        AlignedArray b(pbs, true);
        ldevFile.pwrite(b.data(), pbs, offPb * pbs);
    }
    void clipIfNecessary(LogPackHeader &packH) {
        if (opt_.ddevLb == 0) return;
        size_t i = 0;
        UNUSED const uint16_t totalIoSize0 = packH.totalIoSize();
        while (i < packH.nRecords()) {
            WlogRecord &rec = packH.record(i);
            if (rec.offset + rec.ioSizeLb() <= opt_.ddevLb) {
                // do nothing.
                i++;
            } else {
                i = packH.invalidate(i);
            }
        }
        assert(packH.isValid(false));
        assert(totalIoSize0 == packH.totalIoSize()); // unchanged.
    }
    /**
     * RETURN:
     *   padding size [physical block].
     */
    uint32_t writePaddingIfNecessary(
        cybozu::util::File &ldevFile, const device::SuperBlock &super,
        const LogPackHeader &packH) {

        const uint32_t pbs = packH.pbs();
        const uint32_t salt = packH.salt();

        /* Padding check. */
        const uint32_t paddingPb = getPaddingPb(super, packH);
        if (paddingPb == 0) {
            // No need to write padding pack.
            return 0;
        }

        /* Create and write padding logpack. */
        LogPackHeader padH(pbs, salt);
        padH.init(packH.logpackLsid());
        padH.addPadding(paddingPb - 1);
        ldevFile.lseek(super.getOffsetFromLsid(packH.logpackLsid()) * pbs);
        padH.updateChecksumAndWriteTo(ldevFile);
        return paddingPb;
    }
    uint32_t getPaddingPb(const device::SuperBlock &super, const LogPackHeader &packH) {
        const uint64_t offPb = super.getOffsetFromLsid(packH.logpackLsid());
        const uint64_t endOffPb = super.getRingBufferOffset() + super.getRingBufferSize();
        if (offPb + 1 + packH.totalIoSize() <= endOffPb) return 0;
        const uint32_t paddingPb = endOffPb - offPb;
        assert(paddingPb < (1U << 16));
        assert((packH.logpackLsid() + paddingPb) % super.getRingBufferSize() == 0);
        return paddingPb;
    }
    void restorePack(
        cybozu::util::File &ldevFile, const device::SuperBlock &super,
        const LogPackHeader &packH, std::queue<LogBlockShared> &&ioQ) {

        assert(getPaddingPb(super, packH) == 0);
        const uint64_t offPb = super.getOffsetFromLsid(packH.logpackLsid());
        if (opt_.isVerbose) {
            ::printf("header %u records\n", packH.nRecords());
            ::printf("offPb %" PRIu64 "\n", offPb);
        }
        ldevFile.lseek(offPb * packH.pbs());
        packH.writeTo(ldevFile);

        while (!ioQ.empty()) {
            // lseek is not required because it is contiguous.
            ioQ.front().write(ldevFile);
            ioQ.pop();
        }
    }
    void verifyPack(
        cybozu::util::File &ldevFile, const device::SuperBlock &super,
        const LogPackHeader &packH) {

        /* Currently only header block will be verified. */
        const uint32_t pbs = packH.pbs();
        const uint32_t salt = packH.salt();

        LogPackHeader packH2(pbs, salt);
        uint64_t offPb = super.getOffsetFromLsid(packH.logpackLsid());
        ldevFile.lseek(offPb * pbs);
        if (!packH2.readFrom(ldevFile)) {
            throw cybozu::Exception(__func__) << "read failed" << packH.logpackLsid();
        }
        if (::memcmp(packH.rawData(), packH2.rawData(), pbs) != 0) {
            throw cybozu::Exception(__func__) << "not equal" << packH2 << packH;
        }

        /* TODO: verify checksum of log Io. */
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    WalbLogRestorer wlRes(opt);
    LogFile wlogFile(0);
    wlogFile.setSeekable(false);
    wlRes.restore(wlogFile);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-restore")
