/**
 * @file
 * @brief Read walb log and analyze it.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "linux/walb/walb.h"
#include "walb_util.hpp"

using namespace walb;

/**
 * Command line configuration.
 */
class Config
{
private:
    bool isFromStdin_;
    uint32_t blockSize_;
    bool isVerbose_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : isFromStdin_(false)
        , blockSize_(LOGICAL_BLOCK_SIZE)
        , isVerbose_(false)
        , args_() {
        parse(argc, argv);
    }

    size_t numWlogs() const { return isFromStdin() ? 1 : args_.size(); }
    const std::string& inWlogPath(size_t idx) const { return args_[idx]; }
    bool isFromStdin() const { return isFromStdin_; }
    uint32_t blockSize() const { return blockSize_; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (numWlogs() == 0) {
            throw cybozu::Exception("Specify input wlog path.");
        }
        if (blockSize() % LOGICAL_BLOCK_SIZE != 0) {
            throw cybozu::Exception("invalid blockSize()") << blockSize() << LOGICAL_BLOCK_SIZE;
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlanalyze: analyze wlog.");
        opt.appendOpt(&blockSize_, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE: block size in bytes. (default: %u)", LOGICAL_BLOCK_SIZE).c_str());
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParamVec(&args_, "WLOG_PATH [WLOG_PATH...]");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
        if (args_.empty() || args_[0] == "-") {
            isFromStdin_ = true;
        }
    }
};

class WalbLogAnalyzer
{
private:
    const Config &config_;

    /* If bits_[physical block offset] is set,
       the block is written once or more. */
    std::vector<bool> bits_;

    /* Number of written logical blocks. */
    uint64_t writtenLb_;

public:
    WalbLogAnalyzer(const Config &config)
        : config_(config), bits_(), writtenLb_(0) {}
    void analyze() {
        uint64_t lsid = -1;
        cybozu::Uuid uuid;
        if (config_.isFromStdin()) {
            while (true) {
                uint64_t nextLsid = analyzeWlog(0, lsid, uuid);
                if (nextLsid == lsid) { break; }
                lsid = nextLsid;
            }
        } else {
            for (size_t i = 0; i < config_.numWlogs(); i++) {
                cybozu::util::File file(config_.inWlogPath(i), O_RDONLY);
                lsid = analyzeWlog(file.fd(), lsid, uuid);
                file.close();
            }
        }
        printResult();
    }
private:
    /**
     * Try to read wlog data.
     *
     * @inFd fd for wlog input stream.
     * @beginLsid begin lsid to check continuity of wlog(s).
     *   specify uint64_t(-1) not to check that.
     * @uuid uuid for equality check.
     *   If beginLsid is uint64_t(-1), the uuid will be set.
     *   Else the uuid will be used to check equality of wlog source device.
     *
     * RETURN:
     *   end lsid of the wlog data.
     */
    uint64_t analyzeWlog(int inFd, uint64_t beginLsid, cybozu::Uuid &uuid) {
        /* Read header. */
        WlogReader reader(inFd);
        WlogFileHeader wh;
        try {
            reader.readHeader(wh);
        } catch (cybozu::util::EofError &e) {
            return beginLsid;
        }
        if (config_.isVerbose()) {
            std::cerr << wh << std::endl;
        }

        /* Check uuid if required. */
        if (beginLsid == uint64_t(-1)) {
            /* First call. */
            uuid = wh.getUuid();
        } else {
            const cybozu::Uuid readUuid = wh.getUuid();
            if (uuid != readUuid) {
                throw cybozu::Exception(__func__) << "uuid differ" << uuid << readUuid;
            }
        }

        uint64_t lsid = wh.beginLsid();
        if (beginLsid != uint64_t(-1) && lsid != beginLsid) {
            throw RT_ERR("wrong lsid.");
        }
        /* Read pack IO. */
        WlogRecord rec;
        AlignedArray buf;
        while (reader.readLog(rec, buf)) {
            updateBitmap(rec);
        }
        return reader.endLsid();
    }
    /**
     * Update bitmap with a log record.
     */
    void updateBitmap(const WlogRecord &rec) {
        if (rec.isPadding()) return;
        const uint32_t pbs = config_.blockSize();
        const uint64_t offLb = rec.offset;
        const uint32_t sizeLb = rec.ioSizeLb();
        const uint64_t offPb0 = ::addr_pb(pbs, offLb);
        const uint64_t offPb1 = ::capacity_pb(pbs, offLb + sizeLb);
        setRange(offPb0, offPb1);

        writtenLb_ += sizeLb;
    }
    void resize(size_t size) {
        const size_t s = bits_.size();
        if (size <= s) { return; }
        bits_.resize(size);
        for (size_t i = s; i < size; i++) {
            bits_[i] = false;
        }
    }
    void setRange(size_t off0, size_t off1) {
        resize(off1);
        assert(off0 <= off1);
        for (size_t i = off0; i < off1; i++) {
            bits_[i] = true;
        }
    }
    uint64_t rank(size_t offset) const {
        uint64_t c = 0;
        for (size_t i = 0; i < offset && i < bits_.size(); i++) {
            c += (bits_[i] ? 1 : 0);
        }
        return c;
    }
    uint64_t count() const {
        return rank(bits_.size());
    }
    void printResult() const {
        uint32_t bs = config_.blockSize();

        const uint64_t written = ::capacity_pb(bs, writtenLb_);
        const uint64_t changed = count();
        double rate = 0;
        if (written > 0) {
            rate = static_cast<double>(written - changed)
                / static_cast<double>(written);
        }

        ::printf("block size: %u\n"
                 "number of written blocks: %" PRIu64 "\n"
                 "number of changed blocks: %" PRIu64 "\n"
                 "overwritten rate: %.2f\n",
                 bs, written, changed, rate);
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    config.check();
    WalbLogAnalyzer wlAnalyzer(config);
    wlAnalyzer.analyze();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-analyze")
