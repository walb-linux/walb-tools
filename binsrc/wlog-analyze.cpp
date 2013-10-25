/**
 * @file
 * @brief Read walb log and analyze it.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <queue>
#include <memory>
#include <deque>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include "cybozu/option.hpp"

#include "stdout_logger.hpp"

#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    bool isFromStdin_;
    unsigned int blockSize_;
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
    unsigned int blockSize() const { return blockSize_; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (numWlogs() == 0) {
            throw RT_ERR("Specify input wlog path.");
        }
        if (blockSize() % LOGICAL_BLOCK_SIZE != 0) {
            throw RT_ERR("Block size must be a multiple of %u.",
                       LOGICAL_BLOCK_SIZE);
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
        u8 uuid[UUID_SIZE];
        if (config_.isFromStdin()) {
            while (true) {
                uint64_t nextLsid = analyzeWlog(0, lsid, uuid);
                if (nextLsid == lsid) { break; }
                lsid = nextLsid;
            }
        } else {
            for (size_t i = 0; i < config_.numWlogs(); i++) {
                cybozu::util::FileOpener fo(config_.inWlogPath(i), O_RDONLY);
                lsid = analyzeWlog(fo.fd(), lsid, uuid);
                fo.close();
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
    uint64_t analyzeWlog(int inFd, uint64_t beginLsid, u8 *uuid) {
        if (inFd < 0) {
            throw RT_ERR("inFd is not valid");
        }
        walb::log::Reader reader(inFd);

        /* Read header. */
        walb::log::FileHeader wh;
        try {
            reader.readHeader(wh);
        } catch (cybozu::util::EofError &e) {
            return beginLsid;
        }
        if (config_.isVerbose()) {
            wh.print(::stderr);
        }

        /* Check uuid if required. */
        if (beginLsid == uint64_t(-1)) {
            /* First call. */
            ::memcpy(uuid, wh.uuid(), UUID_SIZE);
        } else {
            if (::memcmp(uuid, wh.uuid(), UUID_SIZE) != 0) {
                throw RT_ERR("Not the same wlog uuid.");
            }
        }

        uint64_t lsid = wh.beginLsid();
        if (beginLsid != uint64_t(-1) && lsid != beginLsid) {
            throw RT_ERR("wrong lsid.");
        }
        /* Read pack IO. */
        while (!reader.isEnd()) {
            walb::log::PackIoRaw<walb::log::BlockDataVec> packIo;
            reader.readLog(packIo);
            updateBitmap(packIo.record());
        }
        return reader.endLsid();
    }
    /**
     * Update bitmap with a log record.
     */
    void updateBitmap(const walb::log::Record &rec) {
        if (rec.isPadding()) return;
        const unsigned int pbs = config_.blockSize();
        uint64_t offLb = rec.offset();
        unsigned int sizeLb = rec.ioSizeLb();
        uint64_t offPb0 = ::addr_pb(pbs, offLb);
        uint64_t offPb1 = ::capacity_pb(pbs, offLb + sizeLb);
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
        unsigned int bs = config_.blockSize();

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

int main(int argc, char* argv[])
try {
    Config config(argc, argv);
    config.check();

    WalbLogAnalyzer wlAnalyzer(config);
    wlAnalyzer.analyze();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
