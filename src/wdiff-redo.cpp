/**
 * @file
 * @brief Redo walb diff on a block device.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <cstdio>

#include <getopt.h>

#include "util.hpp"
#include "walb_diff.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    std::string devPath_;
    std::string inWdiffPath_;
    bool isDiscard_; /* issue discard IO for discard diffs. */
    bool isZeroDiscard_; /* issue all-zero IOs for discard diffs. */
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : devPath_()
        , inWdiffPath_("-")
        , isDiscard_(false)
        , isZeroDiscard_(false)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string &devPath() const { return devPath_; }
    const std::string &inWdiffPath() const { return inWdiffPath_; }
    bool isDiscard() const { return isDiscard_; }
    bool isZeroDiscard() const { return isZeroDiscard_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("devPath: %s\n"
                 "discard: %d\n"
                 "zerodiscard: %d\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 devPath().c_str(),
                 isDiscard(), isZeroDiscard(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto &s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (devPath_.empty()) {
            throwError("Specify a block device path.");
        }
        if (inWdiffPath_.empty()) {
            throwError("Specify input wdiff.");
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
        INPUT_WDIFF_PATH = 1,
        DISCARD,
        ZERO_DISCARD,
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

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"inWdiff", 1, 0, Opt::INPUT_WDIFF_PATH},
                {"discard", 0, 0, Opt::DISCARD},
                {"zerodiscard", 0, 0, Opt::ZERO_DISCARD},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "i:d:z:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::INPUT_WDIFF_PATH:
            case 'i':
                inWdiffPath_ = std::string(optarg);
                break;
            case Opt::DISCARD:
            case 'd':
                isDiscard_ = true;
                break;
            case Opt::ZERO_DISCARD:
            case 'z':
                isZeroDiscard_ = true;
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
            devPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "wdiff-redo: redo wdiff file on a block device.\n"
            "Usage: wdiff-redo [options] DEVICE_PATH\n"
            "Options:\n"
            "  -i, --inWdiff PATH:   input wdiff path. '-' for stdin. (default: '-')\n"
            "  -d, --discard:        issue discard IOs for discard diffs.\n"
            "  -z, --zerodiscard:    issue all-zero IOs for discard diffs.\n"
            "  -v, --verbose:        verbose messages to stderr.\n"
            "  -h, --help:           show this message.\n");
    }
};

using DiffRecPtr = std::shared_ptr<walb::diff::WalbDiffRecord>;
using DiffIoPtr = std::shared_ptr<walb::diff::BlockDiffIo>;
using DiffHeaderPtr = std::shared_ptr<walb::diff::WalbDiffFileHeader>;

/**
 * Diff IO executor interface.
 */
class DiffIoExecutor
{
public:
    virtual ~DiffIoExecutor() noexcept {}

    /**
     * Submit a diff IO.
     *
     * @ioAddr [logical block]
     * @ioBlocks [logical block]
     * @ioP IO to execute.
     *
     * RETURN:
     *   false if the IO can not be executable.
     */
    virtual bool submit(uint64_t ioAddr, uint16_t ioBlocks, DiffIoPtr ioP) = 0;

    /**
     * Wait for all submitted IOs permanent.
     */
    virtual void sync() = 0;
};

/**
 * Simple diff IO executor.
 */
class SimpleDiffIoExecutor /* final */
    : public DiffIoExecutor
{
private:
    cybozu::util::BlockDevice bd_;

public:
    SimpleDiffIoExecutor(const std::string &name, int flags)
        : bd_(name, flags) {
        if (!(flags & O_RDWR)) {
            throw RT_ERR("The flag must have O_RDWR.");
        }
    }
    ~SimpleDiffIoExecutor() noexcept override {}

    bool submit(uint64_t ioAddr, uint16_t ioBlocks, DiffIoPtr ioP) override {
        if (!ioP) { return false; }
        size_t oft = ioAddr * LOGICAL_BLOCK_SIZE;
        size_t size = ioBlocks * LOGICAL_BLOCK_SIZE;

        /* boundary check. */
        if (bd_.getDeviceSize() < oft + size) { return false; }

        std::shared_ptr<char> p = ioP->get();
        //::printf("issue %zu %zu %p\n", oft, size, p.get()); /* debug */
        bd_.write(oft, size, p.get());
        return true;
    }

    void sync() override {
        bd_.fdatasync();
    }
};

/**
 * Provide all-zero memory.
 */
template <typename T>
class ZeroMemory /* final */
{
private:
    std::vector<T> v_;

public:
    ZeroMemory(size_t size)
        : v_(size, 0) {}
    ~ZeroMemory() noexcept {}

    void grow(size_t newSize) {
        if (v_.size() < newSize) {
            size_t oldSize = v_.size();
            v_.resize(newSize);
            ::memset(&v_[oldSize], 0, newSize - oldSize);
        }
    }

    std::shared_ptr<T> makePtr() {
        return std::shared_ptr<T>(&v_[0], [](T *){});
    }
};

/**
 * Statistics.
 */
struct Statistics
{
    uint64_t nIoNormal;
    uint64_t nIoDiscard;
    uint64_t nIoAllZero;
    uint64_t nBlocks;

    Statistics()
        : nIoNormal(0)
        , nIoDiscard(0)
        , nIoAllZero(0)
        , nBlocks(0) {}

    void print() const {
        ::printf("nIoTotal:     %" PRIu64 "\n"
                 "  nIoNormal:  %" PRIu64 "\n"
                 "  nIoDiscard: %" PRIu64 "\n"
                 "  nIoAllZero: %" PRIu64 "\n"
                 "nBlocks:      %" PRIu64 "\n"
                 , nIoNormal + nIoDiscard + nIoAllZero
                 , nIoNormal, nIoDiscard, nIoAllZero, nBlocks);
    }
};

/**
 * Wdiff redo manager.
 */
class WdiffRedoManger
{
private:
    const Config &config_;
    Statistics inStat_, outStat_;
    SimpleDiffIoExecutor ioExec_;
    ZeroMemory<char> zeroMem_;

public:
    WdiffRedoManger(const Config &config)
        : config_(config)
        , inStat_()
        , outStat_()
        , ioExec_(config.devPath(), O_RDWR)
        , zeroMem_(1 << 20) /* 1MB */ {}

    /**
     * Execute a diff Io.
     */
    void executeDiffIo(DiffRecPtr recP, DiffIoPtr ioP) {
        const uint64_t ioAddr = recP->ioAddress();
        const uint16_t ioBlocks = recP->ioBlocks();
        bool isSuccess = false;
        if (recP->isAllZero()) {
            isSuccess = executeZeroIo(ioAddr, ioBlocks);
            if (isSuccess) { outStat_.nIoAllZero++; }
            inStat_.nIoAllZero++;
        } else if (recP->isDiscard()) {
            if (config_.isDiscard()) {
                isSuccess = executeDiscardIo(ioAddr, ioBlocks);
            } else if (config_.isZeroDiscard()) {
                isSuccess = executeZeroIo(ioAddr, ioBlocks);
            } else {
                /* Do nothing */
            }
            if (isSuccess) { outStat_.nIoDiscard++; }
            inStat_.nIoDiscard++;
        } else {
            /* Normal IO. */
            assert(recP->isNormal());
            assert(ioP);
            isSuccess = ioExec_.submit(ioAddr, ioBlocks, ioP);
            if (isSuccess) { outStat_.nIoNormal++; }
            inStat_.nIoNormal++;
        }
        if (isSuccess) {
            outStat_.nBlocks += ioBlocks;
        } else {
            ::printf("Failed to redo: ");
            recP->printOneline();
        }
        inStat_.nBlocks += ioBlocks;
    }

    /**
     * Execute redo.
     */
    void run() {
        /* Read a wdiff file and redo IOs in it. */
        walb::diff::WalbDiffReader wdiffR(0);
        DiffHeaderPtr wdiffH = wdiffR.readHeader();
        wdiffH->print();
        std::pair<DiffRecPtr, DiffIoPtr> p = wdiffR.readDiff();
        DiffRecPtr recP = p.first;
        DiffIoPtr ioP = p.second;
        while (recP) {
            if (!recP->isValid()) {
                ::printf("Invalid record: ");
                recP->printOneline();
            }
            /* redo */
            executeDiffIo(recP, ioP);

            p = wdiffR.readDiff();
            recP = p.first; ioP = p.second;
        }
        ::printf("Input statistics:\n");
        inStat_.print();
        ::printf("Output statistics:\n");
        outStat_.print();
    }

private:
    bool executeZeroIo(uint64_t ioAddr, uint16_t ioBlocks) {
        DiffIoPtr ioP(new walb::diff::BlockDiffIo(ioBlocks));
        size_t size = ioBlocks * LOGICAL_BLOCK_SIZE;
        zeroMem_.grow(size);
        ioP->putCompressed(zeroMem_.makePtr(), size);
        return ioExec_.submit(ioAddr, ioBlocks, ioP);
    }

    bool executeDiscardIo(UNUSED uint64_t ioAddr, UNUSED uint16_t ioBlocks) {
        /* TODO: issue discard command. */
        return false;
    }
};

int main(int argc, char *argv[])
{
    try{
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WdiffRedoManger m(config);
        m.run();
        return 0;
    } catch (Config::Error &e) {
        ::printf("Comman line error: %s\n\n", e.what());
        Config::printHelp();
        return 1;
    } catch (std::runtime_error &e) {
        ::printf("%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        ::printf("%s\n", e.what());
        return 1;
    } catch (...) {
        ::printf("caught other error.\n");
        return 1;
    }
}

/* end of file. */
