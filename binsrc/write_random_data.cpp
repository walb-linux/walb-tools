/**
 * @file
 * @brief write random data for test.
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
#include "random.hpp"
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
    uint64_t offsetB_; /* [block]. */
    uint64_t sizeB_; /* [block]. */
    uint32_t minIoB_; /* [block]. */
    uint32_t maxIoB_; /* [block]. */
    bool isVerbose_;
    std::string targetPath_; /* device or file path. */

public:
    Config(int argc, char* argv[])
        : bs_(LOGICAL_BLOCK_SIZE)
        , offsetB_(0)
        , sizeB_(0)
        , minIoB_(1)
        , maxIoB_(64)
        , isVerbose_(false)
        , targetPath_() {
        parse(argc, argv);
    }

    uint32_t blockSize() const { return bs_; }
    uint64_t offsetB() const { return offsetB_; }
    uint64_t sizeB() const { return sizeB_; }
    uint32_t minIoB() const { return minIoB_; }
    uint32_t maxIoB() const { return maxIoB_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& targetPath() const { return targetPath_; }

    bool isDirect() const {
        return blockSize() % LOGICAL_BLOCK_SIZE == 0;
    }
    void check() const {
        if (blockSize() == 0) {
            throw RT_ERR("blockSize must be non-zero.");
        }
        if (minIoB() == 0) {
            throw RT_ERR("minIoSize must be > 0.");
        }
        if (maxIoB() == 0) {
            throw RT_ERR("maxIoSize must be > 0.");
        }
        if (maxIoB() < minIoB()) {
            throw RT_ERR("minIoSize must be <= maxIoSize.");
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("write_random_data: generate random data and write them.");
        opt.appendOpt(&bs_, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE: block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&offsetB_, 0, "o", "OFFSET: start offset [block]. (default: 0)");
        opt.appendOpt(&sizeB_, 0, "s", "SIZE: written size [block]. (default: device size)");
        opt.appendOpt(&minIoB_, 1, "n", "SIZE: minimum IO size [block]. (default: 1)");
        opt.appendOpt(&maxIoB_, 64, "x", "SIZE: maximum IO size [block]. (default: 64)");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&targetPath_, "[DEVICE|FILE]");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

class RandomDataWriter
{
private:
    const Config &config_;
    cybozu::util::BlockDevice bd_;
    cybozu::util::Random<uint32_t> randUint_;
    walb::AlignedArray buf_;

public:
    RandomDataWriter(const Config &config)
        : config_(config)
        , bd_(config.targetPath(), O_RDWR | (config.isDirect() ? O_DIRECT : 0))
        , randUint_()
        , buf_(config.blockSize() * config.maxIoB()) {
    }

    void run() {
        uint64_t totalSize = decideSize();
        uint64_t offset = config_.offsetB();
        uint64_t written = 0;

        while (written < totalSize) {
            const uint32_t bs = config_.blockSize();
            const uint32_t ioSize = decideIoSize(totalSize - written);
            fillBufferRandomly(ioSize);
            const uint32_t csum = cybozu::util::calcChecksum(buf_.data(), bs * ioSize, 0);
            bd_.write(offset * bs, bs * ioSize, buf_.data());
            walb::util::IoRecipe r(offset, ioSize, csum);
            r.print();

            offset += ioSize;
            written += ioSize;
        }

        config_.offsetB();
        bd_.fdatasync();
    }
private:
    uint64_t decideSize() {
        uint64_t size = config_.sizeB();
        if (size == 0) {
            size = bd_.getDeviceSize() / config_.blockSize();
        }
        if (size == 0) {
            throw RT_ERR("device or file size is 0.");
        }
        return size;
    }

    uint32_t decideIoSize(uint64_t maxSize) {
        uint32_t min = config_.minIoB();
        uint32_t max = config_.maxIoB();
        if (maxSize < max) { max = maxSize; }
        if (max < min) { min = max; }
        return randomUInt(min, max);
    }

    uint32_t randomUInt(uint32_t min, uint32_t max) {
        assert(min <= max);
        if (min == max) { return min; }
        return randUint_() % (max - min) + min;
    }

    void fillBufferRandomly(uint32_t sizeB) {
        assert(0 < sizeB);
        size_t offset = 0;
        size_t remaining = config_.blockSize() * sizeB;
        uint32_t r;
        assert(0 < remaining);
        while (sizeof(r) <= remaining) {
            r = randUint_();
            ::memcpy(buf_.data() + offset, &r, sizeof(r));
            offset += sizeof(r);
            remaining -= sizeof(r);
        }
        if (0 < remaining) {
            r = randUint_();
            ::memcpy(buf_.data() + offset, &r, remaining);
            offset += remaining;
        }
        assert(offset == config_.blockSize() * sizeB);
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    config.check();

    RandomDataWriter rdw(config);
    rdw.run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("write_random_data")
