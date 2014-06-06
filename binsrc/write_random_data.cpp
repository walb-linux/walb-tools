/**
 * @file
 * @brief write random data for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
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
#include "aio_util.hpp"

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
    size_t qSize_;
public:
    int fixVar_;
private:
    bool isVerbose_;
    std::string targetPath_; /* device or file path. */

public:
    enum { noFixVar = ~0xff };
    Config(int argc, char* argv[])
        : bs_(LOGICAL_BLOCK_SIZE)
        , offsetB_(0)
        , sizeB_(0)
        , minIoB_(1)
        , maxIoB_(64)
        , qSize_()
        , fixVar_(0)
        , isVerbose_(false)
        , targetPath_() {
        parse(argc, argv);
    }

    uint32_t blockSize() const { return bs_; }
    uint64_t offsetB() const { return offsetB_; }
    uint64_t sizeB() const { return sizeB_; }
    uint32_t minIoB() const { return minIoB_; }
    uint32_t maxIoB() const { return maxIoB_; }
    size_t qSize() const { return qSize_; }
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
        opt.appendOpt(&fixVar_, noFixVar, "set", ": fill 8bit data(default:none)");
        opt.appendOpt(&qSize_, 128, "q", ": aio queue size (default: 128)");
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
    cybozu::util::File file_;
    const uint64_t devSize_;
    cybozu::util::Random<uint32_t> randUint_;
    struct Io {
        uint32_t key;
        walb::AlignedArray buf;
    };
    std::list<Io> ioL_;

public:
    RandomDataWriter(const Config &config)
        : config_(config)
        , file_(config.targetPath(), O_RDWR | (config.isDirect() ? O_DIRECT : 0))
        , devSize_(cybozu::util::getBlockDeviceSize(file_.fd()))
        , randUint_()
        , ioL_()
    {
    }
    void run() {
        const char *const FUNC = __func__;
        uint64_t totalSize = decideSize();
        uint64_t offset = config_.offsetB();
        uint64_t written = 0;
        cybozu::aio::Aio aio(file_.fd(), config_.qSize());

        while (written < totalSize) {
            const size_t bs = config_.blockSize();
            const size_t ioSize = decideIoSize(totalSize - written);
            ioL_.push_back(Io{0, walb::AlignedArray(ioSize * bs)});
            Io &io = ioL_.back();
            fillBuffer(io.buf);
            const uint32_t csum = cybozu::util::calcChecksum(io.buf.data(), io.buf.size(), 0);
            io.key = aio.prepareWrite(offset * bs, io.buf.size(), io.buf.data());
            if (io.key == 0) throw cybozu::Exception(FUNC) << "prepareWrite failed" << cybozu::ErrorNo();
            aio.submit();
            walb::util::IoRecipe r(offset, ioSize, csum);
            r.print();
            if (ioL_.size() >= config_.qSize()) completeAnIo(aio);
            offset += ioSize;
            written += ioSize;
        }
        while (!ioL_.empty()) completeAnIo(aio);
        file_.fdatasync();
    }
private:
    void completeAnIo(cybozu::aio::Aio &aio) {
        aio.waitFor(ioL_.front().key);
        ioL_.pop_front();
    }
    uint64_t decideSize() {
        uint64_t size = config_.sizeB();
        if (size == 0) {
            size = devSize_ / config_.blockSize();
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
    void fillBuffer(walb::AlignedArray& array) {
        if (config_.fixVar_ != Config::noFixVar) {
            ::memset(array.data(), config_.fixVar_, array.size());
        } else {
            randUint_.fill(array.data(), array.size());
        }
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
