/**
 * @file
 * @brief Redo walb diff on a block device.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <cstdio>
#include "cybozu/option.hpp"

#include "util.hpp"
#include "walb_diff_file.hpp"

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

public:
    Config(int argc, char* argv[])
        : devPath_()
        , inWdiffPath_("-")
        , isDiscard_(false)
        , isZeroDiscard_(false)
        , isVerbose_(false) {
        parse(argc, argv);
    }

    const std::string &devPath() const { return devPath_; }
    const std::string &inWdiffPath() const { return inWdiffPath_; }
    bool isDiscard() const { return isDiscard_; }
    bool isZeroDiscard() const { return isZeroDiscard_; }
    bool isVerbose() const { return isVerbose_; }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wdiff-redo: redo wdiff file on a block device.");
        opt.appendOpt(&inWdiffPath_, "-", "i", "PATH: input wdiff path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isDiscard_, "d", ": issue discard IOs for discard diffs.");
        opt.appendBoolOpt(&isZeroDiscard_, "z", ": issue all-zero IOs for discard diffs.");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&devPath_, "DEVICE_PATH");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

using namespace walb;

/**
 * Simple diff IO executor.
 */
class SimpleDiffIoExecutor
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

    bool submit(uint64_t ioAddr, uint16_t ioBlocks, const DiffIo& io) {
        assert(!io.isCompressed());
        size_t oft = ioAddr * LOGICAL_BLOCK_SIZE;
        size_t size = ioBlocks * LOGICAL_BLOCK_SIZE;
        assert(io.getSize() == size);

        /* boundary check. */
        if (bd_.getDeviceSize() < oft + size) { return false; }

        bd_.write(oft, size, io.get());
        return true;
    }

    void sync() {
        bd_.fdatasync();
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
public:
    WdiffRedoManger(const Config &config)
        : config_(config)
        , inStat_()
        , outStat_()
        , ioExec_(config.devPath(), O_RDWR) {}

    /**
     * Execute a diff Io.
     */
    void executeDiffIo(const walb::DiffRecord& rec, const DiffIo& io) {
        const uint64_t ioAddr = rec.io_address;
        const uint16_t ioBlocks = rec.io_blocks;
        bool isSuccess = false;
        if (rec.isAllZero()) {
            isSuccess = executeZeroIo(ioAddr, ioBlocks);
            if (isSuccess) { outStat_.nIoAllZero++; }
            inStat_.nIoAllZero++;
        } else if (rec.isDiscard()) {
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
            assert(rec.isNormal());
            isSuccess = ioExec_.submit(ioAddr, ioBlocks, io);
            if (isSuccess) { outStat_.nIoNormal++; }
            inStat_.nIoNormal++;
        }
        if (isSuccess) {
            outStat_.nBlocks += ioBlocks;
        } else {
            ::printf("Failed to redo: ");
            rec.printOneline();
        }
        inStat_.nBlocks += ioBlocks;
    }

    /**
     * Execute redo.
     */
    void run() {
        /* Read a wdiff file and redo IOs in it. */
        cybozu::util::File file;
        if (config_.inWdiffPath() != "-") {
            file.open(config_.inWdiffPath(), O_RDONLY);
        } else {
            file.setFd(0);
        }
        walb::diff::Reader wdiffR(file.fd());
        DiffFileHeader wdiffH;
        wdiffR.readHeader(wdiffH);
        wdiffH.print();

        walb::DiffRecord rec;
        DiffIo io;
        while (wdiffR.readAndUncompressDiff(rec, io)) {
            if (!rec.isValid()) {
                ::printf("Invalid record: ");
                rec.printOneline();
            }
            if (!io.isValid()) {
                ::printf("Invalid io: ");
                io.printOneline();
            }
            executeDiffIo(rec, io);
        }
        ::printf("Input statistics:\n");
        inStat_.print();
        ::printf("Output statistics:\n");
        outStat_.print();
    }

private:
    bool executeZeroIo(uint64_t ioAddr, uint16_t ioBlocks) {
        DiffIo io(ioBlocks);
        io.data.resize(ioBlocks * LOGICAL_BLOCK_SIZE);
        return ioExec_.submit(ioAddr, ioBlocks, io);
    }

    bool executeDiscardIo(UNUSED uint64_t ioAddr, UNUSED uint16_t ioBlocks) {
        /* TODO: issue discard command. */
        return false;
    }
};

int main(int argc, char *argv[])
    try
{
    Config config(argc, argv);
    walb::util::setLogSetting("-", false);
    WdiffRedoManger m(config);
    m.run();
} catch (std::exception &e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "caught other error.\n");
    return 1;
}

/* end of file. */
