/**
 * @file
 * @brief write overlapped IOs concurrently and verify the written data.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <stdexcept>
#include <cinttypes>
#include <string>
#include <vector>
#include <memory>

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "util.hpp"
#include "fileio.hpp"
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
    uint32_t bs_; /* block size [byte] */
    uint64_t offset_; /* [byte]. */
    uint64_t size_; /* [byte]. */
    uint32_t minIoSize_; /* [byte]. */
    uint32_t maxIoSize_; /* [byte]. */
    uint32_t counts_; /* The number of write IOs per thread. */
    uint32_t numThreads_; /* The number of threads. */
    bool isVerbose_;
    std::string targetPath_; /* device or file path. */
public:
    Config(int argc, char* argv[])
        : bs_(LOGICAL_BLOCK_SIZE)
        , offset_(0)
        , size_(0)
        , minIoSize_(LOGICAL_BLOCK_SIZE)
        , maxIoSize_(64 * LOGICAL_BLOCK_SIZE)
        , counts_(100)
        , numThreads_(2)
        , isVerbose_(false)
        , targetPath_() {
        parse(argc, argv);
    }

    uint32_t blockSize() const { return bs_; }
    uint64_t offsetB() const { return offset_ / bs_; }
    uint64_t sizeB() const { return size_ / bs_; }
    uint32_t minIoB() const { return minIoSize_ / bs_; }
    uint32_t maxIoB() const { return maxIoSize_ / bs_; }
    uint32_t counts() const { return counts_; }
    uint32_t numThreads() const { return numThreads_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& targetPath() const { return targetPath_; }

    void setSizeB(uint64_t sizeB) {
        size_ = sizeB * blockSize();
    }

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
        if (blockSize() % LOGICAL_BLOCK_SIZE != 0) {
            throw RT_ERR("blockSize must be multiples of 512.");
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
        if (counts() == 0) {
            throw RT_ERR("counts must be > 0.");
        }
        if (numThreads() == 0) {
            throw RT_ERR("numThreads must be > 0.");
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("write_overlapped_and_verify: issud overlapped write IOs and verify the written data.");
        opt.appendOpt(&bs_, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE:  block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&offset_, 0, "o", "OFFSET: start offset [byte]. (default: 0)");
        opt.appendOpt(&size_, 0, "s", "SIZE: target size [byte]. (default: device size)");
        opt.appendOpt(&minIoSize_, LOGICAL_BLOCK_SIZE, "n", cybozu::format("SIZE: minimum IO size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&maxIoSize_, LOGICAL_BLOCK_SIZE * 64, "x", cybozu::format("SIZE: maximum IO size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE * 64));
        opt.appendOpt(&counts_, 100, "c", "N: number of write IOs per threads (default: 100)");
        opt.appendOpt(&numThreads_, 2, "t", "N: number of threads (default: 2)");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&targetPath_, "[DEVICE|FILE]");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

/**
 * A pair of ioAddr and ioBlocks generator.
 */
class RandomIoSpecGenerator
{
private:
    cybozu::util::Random<uint64_t> rand_;
    const uint64_t offsetB_;
    const uint64_t sizeB_;
public:
    RandomIoSpecGenerator(uint64_t offsetB, uint32_t sizeB)
        : rand_()
        , offsetB_(offsetB)
        , sizeB_(sizeB) {
        assert(0 < sizeB);
    }
    ~RandomIoSpecGenerator() noexcept {}
    DISABLE_COPY_AND_ASSIGN(RandomIoSpecGenerator);
    DISABLE_MOVE(RandomIoSpecGenerator);

    void get(uint64_t &ioAddr, uint32_t &ioBlocks) {
        ioAddr = rand_() % sizeB_ + offsetB_;
        uint64_t sizeB = sizeB_ - (ioAddr - offsetB_);
        assert(0 < sizeB);
        ioBlocks = 1;
        if (sizeB == 1) {
            ioBlocks = 1;
        } else {
            ioBlocks = rand_() % (sizeB - 1) + 1;
        }
        assert(offsetB_ <= ioAddr);
        assert(ioAddr + ioBlocks <= offsetB_ + sizeB_);
    }
};

class Worker
{
private:
    const Config &config_;
    cybozu::util::BlockDevice bd_;
    RandomIoSpecGenerator ioSpecGen_;
    const std::shared_ptr<char> dataPtr_;
    std::vector<bool> bmp_;

public:
    Worker(const Config &config, std::shared_ptr<char> dataPtr)
        : config_(config)
        , bd_(config.targetPath(), O_RDWR | O_DIRECT)
        , ioSpecGen_(config.offsetB(), config.sizeB())
        , dataPtr_(dataPtr)
        , bmp_(config.sizeB(), false) {}

    void operator()() {
        const uint32_t bs = config_.blockSize();
        uint64_t ioAddr;
        uint32_t ioBlocks;
        for (size_t i = 0; i < config_.counts(); i++) {
            ioSpecGen_.get(ioAddr, ioBlocks);
            bd_.write(ioAddr * bs, ioBlocks * bs,
                      &dataPtr_.get()[(ioAddr - config_.offsetB()) * bs]);
            for (uint64_t i = ioAddr - config_.offsetB();
                 i < ioAddr - config_.offsetB() + ioBlocks; i++) {
                bmp_[i] = true;
            }
        }
    }
    const std::vector<bool> &getBmp() const {
        return bmp_;
    }
};

static bool writeConcurrentlyAndVerify(Config &config)
{
    /* Prepare */
    const uint32_t bs = config.blockSize();
    cybozu::util::BlockDevice bd(
        config.targetPath(), O_RDWR | O_DIRECT);
    std::shared_ptr<char> block =
        cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, bs);

    /* Decide target area size. */
    size_t sizeB = config.sizeB();
    const size_t devSizeB = bd.getDeviceSize() / bs;
    if (sizeB == 0 || devSizeB <= config.offsetB() + sizeB) {
        sizeB = devSizeB - config.offsetB();
    }
    config.setSizeB(sizeB);

    /* Write zero to the target range. */
    ::memset(block.get(), 0, bs);
    for (uint64_t i = 0; i < sizeB; i++) {
        bd.write((config.offsetB() + i) * bs, bs, block.get());
    }

    /* Prepare shared blocks and fill randomly. */
    std::shared_ptr<char> blocks0 =
        cybozu::util::allocateBlocks<char>(
            LOGICAL_BLOCK_SIZE, bs, sizeB);
    cybozu::util::Random<uint64_t> rand;
    rand.fill(blocks0.get(), sizeB * bs);

    /* Prepare writer threads and run concurrently. */
    std::vector<std::shared_ptr<Worker> > v;
    for (size_t i = 0; i < config.numThreads(); i++) {
        v.push_back(std::make_shared<Worker>(config, blocks0));
    }
    cybozu::thread::ThreadRunnerSet thSet;
    for (std::shared_ptr<Worker> &w : v) {
        thSet.add(w);
    }
    thSet.start();
    thSet.join();

    /* execute OR for each bit. */
    std::vector<bool> bmp(sizeB, false);
    for (size_t i = 0; i < sizeB; i++) {
        for (std::shared_ptr<Worker> &w : v) {
            if (w->getBmp()[i]) { bmp[i] = true; break; }
        }
    }

    /* Verify. */
    bool ret = true;
    uint64_t nVerified = 0, nWritten = 0;
    for (uint64_t i = 0; i < sizeB; i++) {
        if (!bmp[i]) { continue; }
        nWritten++;
        bd.read((config.offsetB() + i) * bs, bs, block.get());
        if (::memcmp(block.get(), blocks0.get() + (i * bs), bs) == 0) {
            nVerified++;
        } else {
            ::printf("block %" PRIu64 " invalid.\n", i);
            ret = false;
        }
    }
    ::printf("total/written/verified %" PRIu64 "/%" PRIu64 "/%" PRIu64 ".\n"
             , sizeB, nWritten, nVerified);
    return ret;
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);
    Config config(argc, argv);
    config.check();
    if (!writeConcurrentlyAndVerify(config)) {
        throw std::runtime_error("The written data could not be read.");
    }
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end file. */
