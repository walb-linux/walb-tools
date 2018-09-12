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
#include "linux/walb/common.h"
#include "linux/walb/block_size.h"
#include "walb_util.hpp"
#include "checksum.hpp"

using namespace walb;

struct Option
{
    uint32_t bs; /* block size [byte] */
    uint64_t offset; /* [byte]. */
    uint64_t size; /* [byte]. */
    uint32_t minIoSize; /* [byte]. */
    uint32_t maxIoSize; /* [byte]. */
    uint32_t periodMs; /* running period [ms]. */
    uint32_t numThreads; /* The number of threads. */
    uint32_t salt;
    size_t numVerify;
    size_t verifyIntervalMs;
    bool isVerbose;
    bool isDebug;
    std::string targetPath; /* device or file path. */

    Option(int argc, char* argv[]) {

        const uint32_t DEFAULT_MIN_IO_SIZE = LOGICAL_BLOCK_SIZE;
        const uint32_t DEFAULT_MAX_IO_SIZE = LOGICAL_BLOCK_SIZE * 64;

        cybozu::Option opt;
        opt.setDescription("write_overlapped_and_verify: "
                           "issud overlapped write IOs and verify the written data.");
        opt.appendOpt(&bs, LOGICAL_BLOCK_SIZE, "b",
                      cybozu::format("SIZE:  block size [byte]. (default: %u)",
                                     LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&offset, 0, "o", "OFFSET: start offset [byte]. (default: 0)");
        opt.appendOpt(&size, MEBI, "s", "SIZE: target size [byte]. (default: 1M)");
        opt.appendOpt(&minIoSize, DEFAULT_MIN_IO_SIZE, "n",
                      cybozu::format("SIZE: minimum IO size [byte]. (default: %u)",
                                     DEFAULT_MIN_IO_SIZE));
        opt.appendOpt(&maxIoSize, DEFAULT_MAX_IO_SIZE, "x",
                      cybozu::format("SIZE: maximum IO size [byte]. (default: %u)",
                                     DEFAULT_MAX_IO_SIZE));
        opt.appendOpt(&periodMs, 100, "p", "N: running period [ms] (default: 100)");
        opt.appendOpt(&numThreads, 2, "t", "N: number of threads (default: 2)");
        opt.appendOpt(&salt, 0, "salt", "VAL: checksum salt (default: 0)");
        opt.appendOpt(&numVerify, 1, "vn", "N: number of verification (default: 1)");
        opt.appendOpt(&verifyIntervalMs, 100, "vi", "N: verification interval [ms] (default: 100)");
        opt.appendBoolOpt(&isVerbose, "v", ": put verbose messages to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&targetPath, "[DEVICE|FILE]");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (isDebug) std::cout << opt << std::endl;

        if (minIoSize < bs) minIoSize = bs;
        if (maxIoSize < minIoSize) maxIoSize = minIoSize;

        verify();
    }

    uint64_t offsetB() const { return offset / bs; }
    uint64_t sizeB() const { return size / bs; }
    uint32_t minIoB() const { return minIoSize / bs; }
    uint32_t maxIoB() const { return maxIoSize / bs; }

    bool isDirect() const {
        return bs % LOGICAL_BLOCK_SIZE == 0;
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
            throw RT_ERR("minIoSize [block] must be > 0.");
        }
        if (maxIoB() == 0) {
            throw RT_ERR("maxIoSize [block] must be > 0.");
        }
        if (maxIoB() < minIoB()) {
            throw cybozu::Exception(__func__)
                << "minIoSize must be <= maxIoSize" << minIoB() << maxIoB();
        }
        if (sizeB() == 0) {
            throw RT_ERR("target size [block] must be > 0.");
        }
        if (sizeB() < minIoB()) {
            throw cybozu::Exception(__func__)
                << "target size [block] must be >= minIoSize [block]"
                << sizeB() << minIoB();
        }
        if (periodMs == 0) {
            throw RT_ERR("periodMs must be > 0.");
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
    const uint32_t minIoB_;
    const uint32_t maxIoB_;
public:
    RandomIoSpecGenerator(uint64_t offsetB, uint32_t sizeB, uint32_t minIoB, uint32_t maxIoB)
        : rand_()
        , offsetB_(offsetB)
        , sizeB_(sizeB)
        , minIoB_(minIoB)
        , maxIoB_(maxIoB) {
        assert(0 < sizeB);
        assert(0 < minIoB);
        assert(0 < maxIoB);
        assert(minIoB <= maxIoB);
    }
    void get(uint64_t &ioAddr, uint32_t &ioBlocks) {
        if (minIoB_ == maxIoB_) {
            ioBlocks = minIoB_;
        } else {
            ioBlocks = rand_() % (maxIoB_ - minIoB_ + 1) + minIoB_;
        }
        if (sizeB_ == ioBlocks) {
            ioAddr =  offsetB_;
        } else {
            ioAddr = rand_() % (sizeB_ - ioBlocks + 1) + offsetB_;
        }
        assert(offsetB_ <= ioAddr);
        assert(ioAddr + ioBlocks <= offsetB_ + sizeB_);
        LOGs.debug() << "addr" << ioAddr << "size" << ioBlocks;
    }
};

class RangeMutex
{
private:
    const size_t size_;
    std::mutex mutex_;
    std::condition_variable cv_;

    using Bmp = std::vector<bool>;
    Bmp bmp_;

public:
    explicit RangeMutex(size_t size)
        : size_(size), mutex_(), cv_(), bmp_(size) {
        unusedVar(size_);
    }
    void lock(size_t idx, size_t size) noexcept {
        assert(idx + size <= size_);
        Bmp::iterator bgn = bmp_.begin() + idx, end = bgn + size;

        std::unique_lock<std::mutex> lk(mutex_);
        while (std::any_of(bgn, end, [](bool b) { return b; })) {
            cv_.wait(lk);
        }
        std::fill(bgn, end, true);
    }
    void unlock(size_t idx, size_t size) noexcept {
        assert(idx + size <= size_);
        Bmp::iterator bgn = bmp_.begin() + idx, end = bgn + size;

        std::unique_lock<std::mutex> lk(mutex_);
        assert(std::all_of(bgn, end, [](bool b) { return b; }));
        std::fill(bgn, end, false);
        cv_.notify_all();
    }
};

class RangeLock
{
private:
    RangeMutex *muP_;
    size_t idx_;
    size_t size_;
    bool locked_;

public:
    RangeLock()
        : muP_(nullptr), idx_(-1), size_(-1), locked_(false) {
    }
    explicit RangeLock(RangeMutex &mu, size_t idx, size_t size) : RangeLock() {
        muP_ = &mu;
        idx_ = idx;
        size_ = size;
        lock();
    }
    RangeLock(RangeLock&& rhs) noexcept : RangeLock() {
        swap(rhs);
    }
    RangeLock& operator=(RangeLock&& rhs) noexcept {
        unlock();
        swap(rhs);
        return *this;
    }
    void swap(RangeLock& rhs) noexcept {
        std::swap(muP_, rhs.muP_);
        std::swap(idx_, rhs.idx_);
        std::swap(size_, rhs.size_);
        std::swap(locked_, rhs.locked_);
    }
    void lock() noexcept {
        assert(muP_);
        assert(!locked_);
        muP_->lock(idx_, size_);
        locked_ = true;
    }
    void unlock() noexcept {
        assert(muP_);
        if (!locked_) return;
        muP_->unlock(idx_, size_);
        locked_ = false;
    }
    ~RangeLock() noexcept {
        unlock();
    }
};

class Worker
{
private:
    const Option &opt_;
    cybozu::util::File file_;
    RandomIoSpecGenerator ioSpecGen_;
    const char *dataPtr_;
    std::vector<uint64_t> ioIdV_;
    RangeMutex &mu_;
    std::atomic<uint64_t> &ioIdGen_;
    std::atomic<bool> &shouldStop_;

public:
    Worker(const Option &opt, const char *dataPtr, RangeMutex &mu, std::atomic<uint64_t> &ioIdGen,
           std::atomic<bool> &shouldStop)
        : opt_(opt)
        , file_(opt.targetPath, O_RDWR | O_DIRECT)
        , ioSpecGen_(opt.offsetB(), opt.sizeB(), opt_.minIoB(), opt_.maxIoB())
        , dataPtr_(dataPtr)
        , ioIdV_(opt.sizeB(), 0)
        , mu_(mu)
        , ioIdGen_(ioIdGen)
        , shouldStop_(shouldStop) {
    }
    void operator()() {
        const uint32_t bs = opt_.bs;
        uint64_t ioAddr;
        uint32_t ioBlocks;
        size_t writtenLb = 0;
        while (!shouldStop_) {
            ioSpecGen_.get(ioAddr, ioBlocks);

            RangeLock lk(mu_, ioAddr, ioBlocks);
            const uint64_t ioId = ioIdGen_++;
            const uint64_t offLb = (ioAddr - opt_.offsetB());
            file_.pwrite(dataPtr_ + (offLb * bs), ioBlocks * bs, ioAddr * bs);
            fillIoIdV(offLb, ioBlocks, ioId);
            writtenLb += ioBlocks;
        }
        std::cout << "thread done: " << writtenLb << std::endl;
    }
    const std::vector<uint64_t> &getIoIdV() const {
        return ioIdV_;
    }
private:
    void fillIoIdV(uint64_t offLb, uint32_t ioBlocks, uint64_t ioId) {
        std::vector<uint64_t>::iterator bgn, end;
        bgn = ioIdV_.begin() + offLb;
        end = bgn + ioBlocks;
        std::fill(bgn, end, ioId);
    }
};


void verify(
    const Option &opt, uint64_t sizeB, const std::vector<size_t> &threadIdV,
    cybozu::util::File &file, const std::vector<AlignedArray> &blksV)
{
    const uint32_t bs = opt.bs;
    AlignedArray blk(bs, false);
    uint64_t nVerified = 0, nWritten = 0;
    if (opt.isVerbose) {
        ::printf("thId written read\n");
    }
    for (uint64_t i = 0; i < sizeB; i++) {
        const size_t thId = threadIdV[i];
        if (thId == size_t(-1)) continue;
        nWritten++;
        file.pread(blk.data(), bs, (opt.offsetB() + i) * bs);
        const char *data = blksV[thId].data() + (i * bs);
        if (::memcmp(blk.data(), data, bs) == 0) {
            nVerified++;
        } else {
            //::printf("block %" PRIu64 " invalid.\n", i);
        }
        if (opt.isVerbose) {
            const uint32_t csum0 = cybozu::util::calcChecksum(data, bs, opt.salt);
            const uint32_t csum1 = cybozu::util::calcChecksum(blk.data(), blk.size(), opt.salt);
            ::printf("%zu %08x %08x\n", thId, csum0, csum1);
        }
    }
    ::printf("total/written/verified %" PRIu64 "/%" PRIu64 "/%" PRIu64 ".\n"
             , sizeB, nWritten, nVerified);

    if (nVerified < nWritten) {
#if 1
        throw cybozu::Exception(__func__)
            << "invalid blocks found" << (nWritten - nVerified);
#else
        ::printf("!!!invalid blocks found %" PRIu64 "!!!\n", nWritten - nVerified);
#endif
    }
}

void writeConcurrentlyAndVerify(const Option &opt)
{
    /* Prepare */
    const uint32_t bs = opt.bs;
    cybozu::util::File file(opt.targetPath, O_RDWR | O_DIRECT);

    /* Decide target area size. */
    const uint64_t sizeB = opt.sizeB();
    const uint64_t devSizeB = cybozu::util::getBlockDeviceSize(file.fd()) / bs;
    if (devSizeB < opt.offsetB() + sizeB) {
        throw cybozu::Exception(__func__)
            << "specified area is out of range" << devSizeB << opt.offsetB() << sizeB;
    }

    /* Write zero to the target range. */
    std::cout << "zero-clear" << std::endl;
    AlignedArray zero(bs, true);
    file.lseek(opt.offsetB() * bs);
    for (uint64_t i = 0; i < sizeB; i++) {
        file.write(zero.data(), zero.size());
        if (i % 16 == 0) {
            ::printf(".");
            ::fflush(::stdout);
            if (i % 1024 == 0) {
                ::printf("%" PRIu64 "\n", i);
            }
        }
    }

    /* Prepare resources shared by all threads. */
    std::cout << "prepare resources" << std::endl;
    std::atomic<uint64_t> ioIdGen(1); // 0 means invalid.
    RangeMutex mu(sizeB);
    std::atomic<bool> shouldStop(false);

    /* Prepare writer threads and run concurrently. */
    /* Prepare blocks and fill randomly. */
    cybozu::util::Random<uint64_t> rand;
    std::vector<AlignedArray> blksV;
    std::vector<std::shared_ptr<Worker> > thV;
    for (size_t i = 0; i < opt.numThreads; i++) {
        blksV.emplace_back(opt.size, false);
#if 0 // fill randomly.
        rand.fill(blksV.back().data(), opt.size);
#else // fill fixed value per thread.
        const uint32_t x = i + 1;
        assert(sizeof(x) <= opt.size);
        const size_t n = opt.size / sizeof(x);
        for (size_t j = 0; j < n; j++) {
            ::memcpy(blksV.back().data() + (sizeof(x) * j), &x, sizeof(x));
        }
#endif
        thV.push_back(std::make_shared<Worker>(
                          opt, blksV.back().data(), mu, ioIdGen, shouldStop));
    }
    if (opt.isVerbose) {
        for (size_t thId = 0; thId < opt.numThreads; thId++) {
            ::printf("%zu", thId);
            for (size_t i = 0; i < sizeB; i++) {
                const char *data = blksV[thId].data() + (i * bs);
                ::printf(" %08x", cybozu::util::calcChecksum(data, bs, opt.salt));
            }
            ::printf("\n");
        }
    }
    cybozu::thread::ThreadRunnerSet thSet;
    for (std::shared_ptr<Worker> &w : thV) {
        thSet.add(w);
    }
    std::cout << "start" << std::endl;
    thSet.start();
    util::sleepMs(opt.periodMs);
    shouldStop = true;
    std::cout << "stop" << std::endl;
    thSet.join();
    std::cout << "done" << std::endl;

    /* Determine who writes each block finally. */
    std::vector<size_t> threadIdV(sizeB, size_t(-1)); // -1 means no one writes.
    for (size_t i = 0; i < sizeB; i++) {
        uint64_t maxIoId = 0;
        size_t thId = 0;
        for (size_t j = 0; j < opt.numThreads; j++) {
            const uint64_t ioId = thV[j]->getIoIdV()[i];
            if (maxIoId < ioId) {
                maxIoId = ioId;
                thId = j;
            }
        }
        if (maxIoId > 0) {
            threadIdV[i] = thId;
        }
    }

    for (size_t i = 0; i < opt.numVerify; i++) {
        std::cout << "verify " << i << std::endl;
        verify(opt, sizeB, threadIdV, file, blksV);
        if (i < opt.numVerify - 1 && opt.verifyIntervalMs > 0) {
            util::sleepMs(opt.verifyIntervalMs);
        }
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    LOGs.debug() << "minIoSize" << opt.minIoSize;
    LOGs.debug() << "maxIoSize" << opt.maxIoSize;
    LOGs.debug() << "blockSize" << opt.bs;

    writeConcurrentlyAndVerify(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("write_overlapped_and_verify")
