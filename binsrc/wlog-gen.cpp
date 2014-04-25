/**
 * @file
 * @brief walb log generator for test.
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
#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "walb_log_gen.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    uint32_t pbs_; /* physical block size [byte] */
    uint64_t devSize_; /* [byte]. */
    uint32_t minIoSize_; /* [byte]. */
    uint32_t maxIoSize_; /* [byte]. */
    uint32_t maxPackSize_; /* [byte]. */
    uint64_t outLogSize_; /* Approximately output log size [byte]. */
    uint64_t lsid_; /* start lsid [physical block]. */
    bool isNotPadding_;
    bool isNotDiscard_;
    bool isNotAllZero_;
    bool isVerbose_;
    std::string outPath_;

    bool isOpened_;
    cybozu::util::File file_;

public:
    Config(int argc, char* argv[])
        : pbs_(LOGICAL_BLOCK_SIZE)
        , devSize_(16 * 1024 * 1024) /* default 16MB. */
        , minIoSize_(pbs_)
        , maxIoSize_(32 * 1024) /* default 32KB. */
        , maxPackSize_(16 * 1024 * 1024) /* default 16MB. */
        , outLogSize_(1024 * 1024) /* default 1MB. */
        , lsid_(0)
        , isNotPadding_(false)
        , isNotDiscard_(false)
        , isNotAllZero_(false)
        , isVerbose_(false)
        , outPath_()
        , isOpened_(false)
        , file_() {
        parse(argc, argv);
    }

    uint64_t devLb() const { return devSize_ / LOGICAL_BLOCK_SIZE; }
    uint32_t minIoLb() const { return minIoSize_ / LOGICAL_BLOCK_SIZE; }
    uint32_t maxIoLb() const { return maxIoSize_ / LOGICAL_BLOCK_SIZE; }
    uint32_t pbs() const { return pbs_; }
    uint32_t maxPackPb() const { return maxPackSize_ / pbs(); }
    uint64_t outLogPb() const { return outLogSize_ / pbs(); }
    uint64_t lsid() const { return lsid_; }
    bool isPadding() const { return !isNotPadding_; }
    bool isDiscard() const { return !isNotDiscard_; }
    bool isAllZero() const { return !isNotAllZero_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& outPath() const { return outPath_; }

    int getOutFd() {
        if (isOpened_) return file_.fd();
        if (outPath() == "-") return 1;
        file_.open(outPath(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        isOpened_ = true;
        return file_.fd();
    }

    walb::log::Generator::Config genConfig() const {
        walb::log::Generator::Config cfg;
        cfg.devLb = devLb();
        cfg.minIoLb = minIoLb();
        cfg.maxIoLb = maxIoLb();
        cfg.pbs = pbs();
        cfg.maxPackPb = maxPackPb();
        cfg.outLogPb = outLogPb();
        cfg.lsid = lsid();
        cfg.isPadding = isPadding();
        cfg.isDiscard = isDiscard();
        cfg.isAllZero = isAllZero();
        cfg.isVerbose = isVerbose();
        return cfg;
    }

    void check() const {
        genConfig().check();
    }

private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlog-gen: generate walb log randomly.");
        opt.appendOpt(&devSize_, 16 * 1024 * 1024, "s", "SIZE: device size [byte]. (default: 16M)");
        opt.appendOpt(&minIoSize_, LOGICAL_BLOCK_SIZE, "-minIoSize", cybozu::format("SIZE: minimum IO size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&maxIoSize_, 32 * 1024, "-maxIoSize", "SIZE: maximum IO size [byte]. (default: 32K)");
        opt.appendOpt(&pbs_, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE: physical block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&maxPackSize_, 16 * 1024 * 1024, "-maxPackSize", "SIZE: maximum logpack size [byte]. (default: 16M)");
        opt.appendOpt(&outLogSize_, 1024 * 1024, "z", "SIZE: total log size to generate [byte]. (default: 1M)");
        opt.appendOpt(&lsid_, 0, "-lsid", "LSID: lsid of the first log. (default: 0)");
        opt.appendBoolOpt(&isNotPadding_, "-nopadding", "no padding. (default: randomly inserted)");
        opt.appendBoolOpt(&isNotDiscard_, "-nodiscard", "no discard. (default: randomly inserted)");
        opt.appendBoolOpt(&isNotAllZero_, "-noallzero", "no all-zero. (default: randomly inserted)");
        opt.appendOpt(&outPath_, "-", "o", "PATH: output file path or '-' for stdout.");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);
    Config config(argc, argv);
    config.check();
    walb::log::Generator::Config cfg = config.genConfig();
    walb::log::Generator wlGen(cfg);
    wlGen.generate(config.getOutFd());
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
