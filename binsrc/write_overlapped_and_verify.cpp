/**
 * @file
 * @brief write overlapped IOs concurrently and verify the written data.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "thread_util.hpp"
#include "random.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "bdev_util.hpp"
#include "walb/common.h"
#include "walb/block_size.h"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    uint32_t bs; /* block size [byte] */
    uint64_t offset; /* [byte]. */
    uint64_t size; /* [byte]. */
    uint32_t minIoSize; /* [byte]. */
    uint32_t maxIoSize; /* [byte]. */
    uint32_t counts; /* The number of write IOs per thread. */
    uint32_t numThreads; /* The number of threads. */
    bool isDebug;
    std::string targetPath; /* device or file path. */

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("write_overlapped_and_verify: issud overlapped write IOs and verify the written data.");
        opt.appendOpt(&bs, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE:  block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&offset, 0, "o", "OFFSET: start offset [byte]. (default: 0)");
        opt.appendOpt(&size, 0, "s", "SIZE: target size [byte]. (default: device size)");
        opt.appendOpt(&minIoSize, LOGICAL_BLOCK_SIZE, "n", cybozu::format("SIZE: minimum IO size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&maxIoSize, LOGICAL_BLOCK_SIZE * 64, "x", cybozu::format("SIZE: maximum IO size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE * 64));
        opt.appendOpt(&counts, 100, "c", "N: number of write IOs per threads (default: 100)");
        opt.appendOpt(&numThreads, 2, "t", "N: number of threads (default: 2)");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&targetPath, "[DEVICE|FILE]");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        verify();
    }

    uint64_t offsetB() const { return offset / bs; }
    uint64_t sizeB() const { return size / bs; }
    uint32_t minIoB() const { return minIoSize / bs; }
    uint32_t maxIoB() const { return maxIoSize / bs; }

    bool isDirect() const {
#if 0
        return false;
#else
        return bs % LOGICAL_BLOCK_SIZE == 0;
#endif
    }

private:
    void verify() const {
        if (bs == 0) {
            throw RT_ERR("blockSize must be non-zero.");
        }
        if (bs % LOGICAL_BLOCK_SIZE != 0) {
            throw cybozu::Exception(__func__)
                << "blockSize must be multiples of 512" << bs;
        }
        if (minIoB() == 0) {
            throw RT_ERR("minIoSize must be > 0.");
        }
        if (maxIoB() == 0) {
            throw RT_ERR("maxIoSize must be > 0.");
        }
        if (maxIoB() < minIoB()) {
            throw cybozu::Exception(__func__)
                << "minIoSize must be <= maxIoSize" << minIoB() << maxIoB();
        }
        if (counts == 0) {
            throw RT_ERR("counts must be > 0.");
        }
        if (numThreads == 0) {
            throw RT_ERR("numThreads must be > 0.");
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
    const Option &opt_;
    cybozu::util::File file_;
    RandomIoSpecGenerator ioSpecGen_;
    const char * const dataPtr_;
    std::vector<bool> bmp_;

public:
    Worker(const Option &opt, const char *dataPtr)
        : opt_(opt)
        , file_(opt.targetPath, O_RDWR | O_DIRECT)
        , ioSpecGen_(opt.offsetB(), opt.sizeB())
        , dataPtr_(dataPtr)
        , bmp_(opt.sizeB(), false) {}

    void operator()() {
        const uint32_t bs = opt_.bs;
        uint64_t ioAddr;
        uint32_t ioBlocks;
        for (size_t i = 0; i < opt_.counts; i++) {
            ioSpecGen_.get(ioAddr, ioBlocks);
            const uint64_t offLb = (ioAddr - opt_.offsetB());
            file_.pwrite(dataPtr_ + (offLb * bs), ioBlocks * bs, ioAddr * bs);
            for (uint64_t i = offLb; i < offLb + ioBlocks; i++) {
                bmp_[i] = true;
            }
        }
    }
    const std::vector<bool> &getBmp() const {
        return bmp_;
    }
};

void writeConcurrentlyAndVerify(Option &opt)
{
    /* Prepare */
    const uint32_t bs = opt.bs;
    cybozu::util::File file(opt.targetPath, O_RDWR | O_DIRECT);

    /* Decide target area size. */
    size_t sizeB = opt.sizeB();
    const size_t devSizeB = cybozu::util::getBlockDeviceSize(file.fd()) / bs;
    if (sizeB == 0 || devSizeB <= opt.offsetB() + sizeB) {
        sizeB = devSizeB - opt.offsetB();
    }

    /* Write zero to the target range. */
    AlignedArray blk(bs);
    file.lseek(opt.offsetB() * bs);
    for (uint64_t i = 0; i < sizeB; i++) {
        file.write(blk.data(), blk.size());
    }

    /* Prepare shared blocks and fill randomly. */
    cybozu::util::Random<uint64_t> rand;
    AlignedArray blks(sizeB * bs);
    rand.fill(blk.data(), blk.size());

    /* Prepare writer threads and run concurrently. */
    std::vector<std::shared_ptr<Worker>> v;
    for (size_t i = 0; i < opt.numThreads; i++) {
        v.push_back(std::make_shared<Worker>(opt, blks.data()));
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
    uint64_t nVerified = 0, nWritten = 0;
    for (uint64_t i = 0; i < sizeB; i++) {
        if (!bmp[i]) continue;
        nWritten++;
        file.pread(blk.data(), bs, (opt.offsetB() + i) * bs);
        if (::memcmp(blk.data(), blks.data() + (i * bs), bs) == 0) {
            nVerified++;
        } else {
            ::printf("block %" PRIu64 " invalid.\n", i);
        }
    }
    ::printf("total/written/verified %" PRIu64 "/%" PRIu64 "/%" PRIu64 ".\n"
             , sizeB, nWritten, nVerified);

    if (nVerified < nWritten) {
        throw cybozu::Exception(__func__)
            << "invalid blocks found" << (nWritten - nVerified);
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    writeConcurrentlyAndVerify(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("write_overlapped_and_verify")
