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

#include <getopt.h>

#include "thread_util.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"
#include "walb/common.h"
#include "walb/block_size.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    unsigned int bs_; /* block size [byte] */
    uint64_t offset_; /* [byte]. */
    uint64_t size_; /* [byte]. */
    unsigned int minIoSize_; /* [byte]. */
    unsigned int maxIoSize_; /* [byte]. */
    unsigned int counts_; /* The number of write IOs per thread. */
    unsigned int numThreads_; /* The number of threads. */
    bool isVerbose_;
    bool isHelp_;
    std::string targetPath_; /* device or file path. */
    std::vector<std::string> args_;

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
        , isHelp_(false)
        , targetPath_()
        , args_() {
        parse(argc, argv);
    }

    unsigned int blockSize() const { return bs_; }
    uint64_t offsetB() const { return offset_ / bs_; }
    uint64_t sizeB() const { return size_ / bs_; }
    unsigned int minIoB() const { return minIoSize_ / bs_; }
    unsigned int maxIoB() const { return maxIoSize_ / bs_; }
    unsigned int counts() const { return counts_; }
    unsigned int numThreads() const { return numThreads_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }
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

    void print() const {
        FILE *fp = ::stderr;
        ::fprintf(fp, "blockSize: %u\n"
                  "offsetB: %" PRIu64 "\n"
                  "sizeB: %" PRIu64 "\n"
                  "minIoB: %u\n"
                  "maxIoB: %u\n"
                  "counts: %u\n"
                  "numThreads: %u\n"
                  "verbose: %d\n"
                  "isHelp: %d\n"
                  "targetPath: %s\n"
                  , blockSize(), offsetB(), sizeB()
                  , minIoB(), maxIoB(), counts(), numThreads()
                  , isVerbose(), isHelp(), targetPath().c_str());
        int i = 0;
        for (const auto& s : args_) {
            ::fprintf(fp, "arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (blockSize() == 0) {
            throwError("blockSize must be non-zero.");
        }
        if (blockSize() % LOGICAL_BLOCK_SIZE != 0) {
            throwError("blockSize must be multiples of 512.");
        }
        if (minIoB() == 0) {
            throwError("minIoSize must be > 0.");
        }
        if (maxIoB() == 0) {
            throwError("maxIoSize must be > 0.");
        }
        if (maxIoB() < minIoB()) {
            throwError("minIoSize must be <= maxIoSize.");
        }
        if (counts() == 0) {
            throwError("counts must be > 0.");
        }
        if (numThreads() == 0) {
            throwError("numThreads must be > 0.");
        }
        if (targetPath().size() == 0) {
            throwError("specify target device or file.");
        }
    }

    class Error : public std::runtime_error {
    public:
        explicit Error(const std::string &msg)
            : std::runtime_error(msg) {}
    };

private:
    /* Option ids. */
    enum Opt {
        BLOCKSIZE = 1,
        OFFSET,
        SIZE,
        MINIOSIZE,
        MAXIOSIZE,
        COUNTS,
        NUM_THREADS,
        VERBOSE,
        HELP,
    };

    void throwError(const char *format, ...) const {
        va_list args;
        std::string msg;
        va_start(args, format);
        try {
            msg = cybozu::util::formatStringV(format, args);
        } catch (...) {}
        va_end(args);
        throw Error(msg);
    }

    template <typename IntType>
    IntType str2int(const char *str) const {
        return static_cast<IntType>(cybozu::util::fromUnitIntString(str));
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"blockSize", 1, 0, Opt::BLOCKSIZE},
                {"offset", 1, 0, Opt::OFFSET},
                {"size", 1, 0, Opt::SIZE},
                {"minIoSize", 1, 0, Opt::MINIOSIZE},
                {"maxIoSize", 1, 0, Opt::MAXIOSIZE},
                {"counts", 1, 0, Opt::COUNTS},
                {"numThreads", 1, 0, Opt::NUM_THREADS},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "b:o:s:n:x:c:t:vh",
                                  long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::BLOCKSIZE:
            case 'b':
                bs_ = str2int<unsigned int>(optarg);
                break;
            case Opt::OFFSET:
            case 'o':
                offset_ = str2int<uint64_t>(optarg);
                break;
            case Opt::SIZE:
            case 's':
                size_ = str2int<uint64_t>(optarg);
                break;
            case Opt::MINIOSIZE:
            case 'n':
                minIoSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::MAXIOSIZE:
            case 'x':
                maxIoSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::COUNTS:
            case 'c':
                counts_ = str2int<unsigned int>(optarg);
                break;
            case Opt::NUM_THREADS:
            case 't':
                numThreads_ = str2int<unsigned int>(optarg);
                break;
            case Opt::VERBOSE:
            case 'v':
                isVerbose_ = true;
                break;
            case Opt::HELP:
            case 'h':
                isHelp_ = true;
                break;
            default:
                throwError("Unknown option.");
            }
        }

        while(optind < argc) {
            args_.push_back(std::string(argv[optind++]));
        }

        if (!args_.empty()) {
            targetPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "write_overlapped_and_verify: issud overlapped write IOs and verify the written data.\n"
            "Usage: write_overlapped_and_verify [options] [DEVICE|FILE]\n"
            "Options:\n"
            "  -b, --blockSize SIZE:  block size [byte]. (default: %u)\n"
            "  -o, --offset OFFSET:   start offset [byte]. (default: 0)\n"
            "  -s, --size SIZE:       target size [byte]. (default: device size)\n"
            "  -n, --minIoSize SIZE:  minimum IO size [byte]. (default: %u)\n"
            "  -x, --maxIoSize SIZE:  maximum IO size [byte]. (default: %u)\n"
            "  -c, --counts N:        number of write IOs per threads (default: 100)\n"
            "  -t, --numThreads N:    number of threads (default: 2)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n",
            LOGICAL_BLOCK_SIZE, LOGICAL_BLOCK_SIZE, LOGICAL_BLOCK_SIZE * 64);
    }
};

/**
 * A pair of ioAddr and ioBlocks generator.
 */
class RandomIoSpecGenerator
{
private:
    cybozu::util::Rand<uint64_t> rand_;
    const uint64_t offsetB_;
    const uint64_t sizeB_;
public:
    RandomIoSpecGenerator(uint64_t offsetB, unsigned int sizeB)
        : rand_()
        , offsetB_(offsetB)
        , sizeB_(sizeB) {
        assert(0 < sizeB);
    }
    ~RandomIoSpecGenerator() noexcept {}
    DISABLE_COPY_AND_ASSIGN(RandomIoSpecGenerator);

    void get(uint64_t &ioAddr, unsigned int &ioBlocks) {
        ioAddr = rand_.get() % sizeB_ + offsetB_;
        uint64_t sizeB = sizeB_ - (ioAddr - offsetB_);
        assert(0 < sizeB);
        ioBlocks = 1;
        if (sizeB == 1) {
            ioBlocks = 1;
        } else {
            ioBlocks = rand_.get() % (sizeB - 1) + 1;
        }
        assert(offsetB_ <= ioAddr);
        assert(ioAddr + ioBlocks <= offsetB_ + sizeB_);
    }
};

class Worker
    : public cybozu::thread::Runnable
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

    virtual ~Worker() noexcept override {}

    virtual void operator()() override {
        const unsigned int bs = config_.blockSize();
        uint64_t ioAddr;
        unsigned int ioBlocks;
        try {
            for (size_t i = 0; i < config_.counts(); i++) {
                ioSpecGen_.get(ioAddr, ioBlocks);
                bd_.write(ioAddr * bs, ioBlocks * bs,
                          &dataPtr_.get()[(ioAddr - config_.offsetB()) * bs]);
                for (uint64_t i = ioAddr - config_.offsetB();
                     i < ioAddr - config_.offsetB() + ioBlocks; i++) {
                    bmp_[i] = true;
                }
            }
            done();
        } catch (...) {
            throwErrorLater(std::current_exception());
        }
    }

    const std::vector<bool> &getBmp() const {
        return bmp_;
    }
};

static bool writeConcurrentlyAndVerify(Config &config)
{
    /* Prepare */
    const unsigned int bs = config.blockSize();
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
    cybozu::util::Rand<uint64_t> rand;
    rand.fill(blocks0.get(), sizeB * bs);

    /* Prepare writer threads and run concurrently. */
    std::vector<std::shared_ptr<Worker> > v;
    for (size_t i = 0; i < config.numThreads(); i++) {
        v.push_back(std::make_shared<Worker>(config, blocks0));
    }
    cybozu::thread::ThreadRunnerSet thSet;
    for (std::shared_ptr<Worker> &w : v) {
        thSet.add(cybozu::thread::ThreadRunner(*w));
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

int main(int argc, char* argv[])
{
    try {
        Config config(argc, argv);
        /* config.print(); */
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();
        if (!writeConcurrentlyAndVerify(config)) {
            throw std::runtime_error("The written data could not be read.");
        }
        return 0;

    } catch (Config::Error& e) {
        ::printf("Command line error: %s\n\n", e.what());
        Config::printHelp();
        return 1;
    } catch (std::runtime_error& e) {
        LOGe("Error: %s\n", e.what());
        return 1;
    } catch (std::exception& e) {
        LOGe("Exception: %s\n", e.what());
        return 1;
    } catch (...) {
        LOGe("Caught other error.\n");
        return 1;
    }
}

/* end file. */
