/**
 * @file
 * @brief verify data written by write_random_data.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "checksum.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "io_recipe.hpp"
#include "walb/common.h"
#include "walb/block_size.h"
#include "walb_util.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    uint32_t bs_; /* block size [byte] */
    bool isVerbose_;
    std::string recipePath_; /* recipe file path. */
    std::string targetPath_; /* device or file path. */

public:
    Config(int argc, char* argv[])
        : bs_(LOGICAL_BLOCK_SIZE)
        , isVerbose_(false)
        , recipePath_("-")
        , targetPath_() {
        parse(argc, argv);
    }

    uint32_t blockSize() const { return bs_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& targetPath() const { return targetPath_; }
    const std::string& recipePath() const { return recipePath_; }

    bool isDirect() const {
        return blockSize() % LOGICAL_BLOCK_SIZE == 0;
    }

    void check() const {
        if (blockSize() == 0) {
            throw RT_ERR("blockSize must be non-zero.");
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("verify_written_data: verify data written by write_random_data.");
        opt.appendOpt(&bs_, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE: block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&recipePath_, "-", "i", "PATH: recipe file path. '-' for stdin. (default: '-')");
        opt.appendParam(&targetPath_, "DEVICE|FILE");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
        check();
    }
};

class IoDataVerifier
{
private:
    const Config &config_;
    cybozu::util::BlockDevice bd_;
    size_t bufSizeB_; /* buffer size [block]. */
    using AlignedArray = walb::AlignedArray;
    AlignedArray buf_;

public:
    IoDataVerifier(const Config &config)
        : config_(config)
        , bd_(config.targetPath(), O_RDONLY | (config.isDirect() ? O_DIRECT : 0))
        , bufSizeB_(1024 * 1024 / config.blockSize()) /* 1MB */
        , buf_(config.blockSize() * bufSizeB_) {
    }

    void run() {
        const uint32_t bs = config_.blockSize();

        /* Get IO recipe parser. */
        cybozu::util::File fileR;
        if (config_.recipePath() != "-") {
            fileR.open(config_.recipePath(), O_RDONLY);
        } else {
            fileR.setFd(0);
        }
        walb::util::IoRecipeParser recipeParser(fileR.fd());

        /* Read and verify for each IO recipe. */
        while (!recipeParser.isEnd()) {
            walb::util::IoRecipe r = recipeParser.get();
            buf_.resize(bs * r.ioSizeB());
            bd_.read(buf_.size(), r.ioSizeB() * bs, buf_.data());
            uint32_t csum = cybozu::util::calcChecksum(buf_.data(), r.ioSizeB() * bs, 0);
            ::printf("%s\t%s\t%08x\n",
                     (csum == r.csum() ? "OK" : "NG"), r.toString().c_str(), csum);
        }
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    IoDataVerifier v(config);
    v.run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("verify_written_data")
