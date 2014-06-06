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
struct Option : cybozu::Option
{
    uint32_t blockSize; /* block size [byte] */
    uint64_t offsetB; /* [block]. */
    uint64_t sizeB; /* [block]. */
    uint32_t minIoB; /* [block]. */
    uint32_t maxIoB; /* [block]. */
    size_t qSize;
    int fixVar;
    bool isVerbose;
    std::string targetPath; /* device or file path. */

    enum { noFixVar = ~0xff };
    Option () {
        setDescription("write_random_data: generate random data and write them.");
        appendOpt(&blockSize, LOGICAL_BLOCK_SIZE, "b",
                  cybozu::format("SIZE: block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        appendOpt(&offsetB, 0, "o", "OFFSET: start offset [block]. (default: 0)");
        appendOpt(&sizeB, 0, "s", "SIZE: written size [block]. (default: device size)");
        appendOpt(&minIoB, 1, "n", "SIZE: minimum IO size [block]. (default: 1)");
        appendOpt(&maxIoB, 64, "x", "SIZE: maximum IO size [block]. (default: 64)");
        appendOpt(&fixVar, noFixVar, "set", ": fill 8bit data(default:none)");
        appendOpt(&qSize, 128, "q", ": aio queue size (default: 128)");
        appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        appendHelp("h", ": show this message.");
        appendParam(&targetPath, "[DEVICE|FILE]");
    }
    bool isDirect() const {
        return blockSize % LOGICAL_BLOCK_SIZE == 0;
    }
    bool parse(int argc, char* argv[]) {
        if (!cybozu::Option::parse(argc, argv)) return false;
        if (blockSize == 0) throw RT_ERR("blockSize must be non-zero.");
        if (minIoB == 0) throw RT_ERR("minIoSize must be > 0.");
        if (maxIoB == 0) throw RT_ERR("maxIoSize must be > 0.");
        if (maxIoB < minIoB) throw RT_ERR("minIoSize must be <= maxIoSize.");
        if (qSize == 0) throw RT_ERR("queueSize must be non-zero.");
        return true;
    }
private:
};

class RandomDataWriter
{
private:
    const Option &opt_;
    cybozu::util::File file_;
    cybozu::util::Random<uint32_t> randUint_;
    struct Io {
        uint32_t key;
        walb::AlignedArray buf;
    };
    std::list<Io> ioL_;

public:
    RandomDataWriter(const Option &opt)
        : opt_(opt)
        , file_(opt.targetPath, O_RDWR | (opt.isDirect() ? O_DIRECT : 0))
        , randUint_()
        , ioL_() {
    }
    void run() {
        const char *const FUNC = __func__;
        const size_t bs = opt_.blockSize;
        uint64_t offB = opt_.offsetB;
        uint64_t endB = offB + getTargetSizeB();
        cybozu::aio::Aio aio(file_.fd(), opt_.qSize);

        while (offB < endB) {
            const size_t ioB = decideIoSizeB(endB - offB);
            ioL_.push_back(Io{0, walb::AlignedArray(ioB * bs)});
            Io &io = ioL_.back();
            fillBuffer(io.buf);
            const uint32_t csum = cybozu::util::calcChecksum(io.buf.data(), io.buf.size(), 0);
            io.key = aio.prepareWrite(offB * bs, io.buf.size(), io.buf.data());
            if (io.key == 0) throw cybozu::Exception(FUNC) << "prepareWrite failed" << cybozu::ErrorNo();
            aio.submit();
            walb::util::IoRecipe r(offB, ioB, csum);
            r.print();
            if (ioL_.size() >= opt_.qSize) completeAnIo(aio);
            offB += ioB;
        }
        while (!ioL_.empty()) completeAnIo(aio);
        file_.fdatasync();
    }
private:
    void completeAnIo(cybozu::aio::Aio &aio) {
        aio.waitFor(ioL_.front().key);
        ioL_.pop_front();
    }
    uint64_t getTargetSizeB() const {
        const uint64_t devSize = cybozu::util::getBlockDeviceSize(file_.fd());
        if (devSize == 0) throw RT_ERR("device or file size is 0.");
        return std::min(opt_.sizeB, devSize / opt_.blockSize);
    }
    uint32_t decideIoSizeB(uint64_t maxB) {
        const uint32_t max = std::min<uint64_t>(opt_.maxIoB, maxB);
        const uint32_t min = std::min<uint64_t>(opt_.minIoB, max);
        return getRandomUInt(min, max);
    }
    uint32_t getRandomUInt(uint32_t min, uint32_t max) {
        assert(min <= max);
        if (min == max) return min;
        return randUint_() % (max - min) + min;
    }
    void fillBuffer(walb::AlignedArray& array) {
        if (opt_.fixVar != Option::noFixVar) {
            ::memset(array.data(), opt_.fixVar, array.size());
        } else {
            randUint_.fill(array.data(), array.size());
        }
    }
};

int doMain(int argc, char* argv[])
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    RandomDataWriter rdw(opt);
    rdw.run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("write_random_data")
