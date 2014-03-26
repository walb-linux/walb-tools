/**
 * @file
 * @brief verify data written by write_random_data.
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
#include "checksum.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"
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
    unsigned int bs_; /* block size [byte] */
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

    unsigned int blockSize() const { return bs_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& targetPath() const { return targetPath_; }
    const std::string& recipePath() const { return recipePath_; }

    bool isDirect() const {
#if 0
        return false;
#else
        return blockSize() % LOGICAL_BLOCK_SIZE == 0;
#endif
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
    std::shared_ptr<char> buf_;

public:
    IoDataVerifier(const Config &config)
        : config_(config)
        , bd_(config.targetPath(), O_RDONLY | (config.isDirect() ? O_DIRECT : 0))
        , bufSizeB_(1024 * 1024 / config.blockSize()) /* 1MB */
        , buf_(getBufferStatic(config.blockSize(), bufSizeB_, config.isDirect())) {
        assert(buf_);
    }

    void run() {
        const unsigned int bs = config_.blockSize();

        /* Get IO recipe parser. */
        std::shared_ptr<cybozu::util::FileOpener> fop;
        if (config_.recipePath() != "-") {
            fop.reset(new cybozu::util::FileOpener(config_.recipePath(), O_RDONLY));
        }
        int fd = 0;
        if (fop) { fd = fop->fd(); }
        walb::util::IoRecipeParser recipeParser(fd);

        /* Read and verify for each IO recipe. */
        while (!recipeParser.isEnd()) {
            walb::util::IoRecipe r = recipeParser.get();
            resizeBufferIfneed(r.ioSizeB());
            bd_.read(r.offsetB() * bs, r.ioSizeB() * bs, buf_.get());
            uint32_t csum = cybozu::util::calcChecksum(buf_.get(), r.ioSizeB() * bs, 0);
            ::printf("%s\t%s\t%08x\n",
                     (csum == r.csum() ? "OK" : "NG"), r.toString().c_str(), csum);
        }
    }

private:
    static std::shared_ptr<char> getBufferStatic(unsigned int blockSize, unsigned sizeB, bool isDirect) {
        assert(0 < blockSize);
        assert(0 < sizeB);
        if (isDirect) {
            return cybozu::util::allocateBlocks<char>(blockSize, blockSize * sizeB);
        } else {
            return std::shared_ptr<char>(reinterpret_cast<char *>(::malloc(blockSize * sizeB)));
        }
    }

    void resizeBufferIfneed(unsigned int newSizeB) {
        if (newSizeB <= bufSizeB_) { return; }
        const unsigned int bs = config_.blockSize();
        if (config_.isDirect()) {
            buf_ = cybozu::util::allocateBlocks<char>(bs, bs * newSizeB);
        } else {
            buf_.reset(reinterpret_cast<char *>(::malloc(bs * newSizeB)));
        }
        if (!buf_) { throw std::bad_alloc(); }
        bufSizeB_ = newSizeB;
    }
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);
    Config config(argc, argv);
    IoDataVerifier v(config);
    v.run();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end file. */
