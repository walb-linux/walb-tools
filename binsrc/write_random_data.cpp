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
#include "memory_buffer.hpp"
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
    uint64_t offsetB_; /* [block]. */
    uint64_t sizeB_; /* [block]. */
    unsigned int minIoB_; /* [block]. */
    unsigned int maxIoB_; /* [block]. */
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

    unsigned int blockSize() const { return bs_; }
    uint64_t offsetB() const { return offsetB_; }
    uint64_t sizeB() const { return sizeB_; }
    unsigned int minIoB() const { return minIoB_; }
    unsigned int maxIoB() const { return maxIoB_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& targetPath() const { return targetPath_; }

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
    cybozu::util::Random<unsigned int> randUint_;
    std::shared_ptr<char> buf_;

public:
    RandomDataWriter(const Config &config)
        : config_(config)
        , bd_(config.targetPath(), O_RDWR | (config.isDirect() ? O_DIRECT : 0))
        , randUint_()
        , buf_(getBufferStatic(config.blockSize(), config.maxIoB(), config.isDirect())) {
        assert(buf_);
    }

    void run() {
        uint64_t totalSize = decideSize();
        uint64_t offset = config_.offsetB();
        uint64_t written = 0;

        while (written < totalSize) {
            const unsigned int bs = config_.blockSize();
            unsigned int ioSize = decideIoSize(totalSize - written);
            fillBufferRandomly(ioSize);
            uint32_t csum = cybozu::util::calcChecksum(buf_.get(), bs * ioSize, 0);
            bd_.write(offset * bs, bs * ioSize, buf_.get());
            walb::util::IoRecipe r(offset, ioSize, csum);
            r.print();

            offset += ioSize;
            written += ioSize;
        }
        assert(written == totalSize);

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

    unsigned int decideIoSize(uint64_t maxSize) {
        unsigned int min = config_.minIoB();
        unsigned int max = config_.maxIoB();
        if (maxSize < max) { max = maxSize; }
        if (max < min) { min = max; }
        return randomUInt(min, max);
    }

    unsigned int randomUInt(unsigned int min, unsigned int max) {
        assert(min <= max);
        if (min == max) { return min; }
        return randUint_() % (max - min) + min;
    }

    static std::shared_ptr<char> getBufferStatic(
        unsigned int blockSize, unsigned int maxIoB, bool isDirect) {
        assert(0 < blockSize);
        assert(0 < maxIoB);
        if (isDirect) {
            return cybozu::util::allocateBlocks<char>(blockSize, blockSize * maxIoB);
        } else {
            return std::shared_ptr<char>(reinterpret_cast<char *>(::malloc(blockSize * maxIoB)));
        }
    }

    void fillBufferRandomly(unsigned int sizeB) {
        assert(0 < sizeB);
        size_t offset = 0;
        size_t remaining = config_.blockSize() * sizeB;
        unsigned int r;
        assert(0 < remaining);
        while (sizeof(r) <= remaining) {
            r = randUint_();
            ::memcpy(buf_.get() + offset, &r, sizeof(r));
            offset += sizeof(r);
            remaining -= sizeof(r);
        }
        if (0 < remaining) {
            r = randUint_();
            ::memcpy(buf_.get() + offset, &r, remaining);
            offset += remaining;
        }
        assert(offset == config_.blockSize() * sizeB);
    }
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);

    Config config(argc, argv);
    config.check();

    RandomDataWriter rdw(config);
    rdw.run();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end file. */
