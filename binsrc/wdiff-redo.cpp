/**
 * @file
 * @brief Redo walb diff on a block device.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "util.hpp"
#include "bdev_util.hpp"
#include "walb_diff_file.hpp"

using namespace walb;

class Option
{
private:
    std::string devPath_;
    std::string inWdiffPath_;
    bool doDiscard_; /* issue discard IO for discard diffs. */
    bool doZeroDiscard_; /* issue all-zero IOs for discard diffs. */
    bool isVerbose_;

public:
    Option(int argc, char* argv[])
        : devPath_()
        , inWdiffPath_("-")
        , doDiscard_(false)
        , doZeroDiscard_(false)
        , isVerbose_(false) {
        parse(argc, argv);
    }

    const std::string &devPath() const { return devPath_; }
    const std::string &inWdiffPath() const { return inWdiffPath_; }
    bool doDiscard() const { return doDiscard_; }
    bool doZeroDiscard() const { return doZeroDiscard_; }
    bool isVerbose() const { return isVerbose_; }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wdiff-redo: redo wdiff file on a block device.");
        opt.appendOpt(&inWdiffPath_, "-", "i", "PATH: input wdiff path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&doDiscard_, "d", ": issue discard IOs for discard diffs.");
        opt.appendBoolOpt(&doZeroDiscard_, "z", ": issue all-zero IOs for discard diffs.");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&devPath_, "DEVICE_PATH");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

class SimpleDiffIoExecutor
{
private:
    cybozu::util::File file_;
    uint64_t devSize_;

public:
    SimpleDiffIoExecutor(const std::string &name, int flags)
        : file_(name, flags), devSize_(cybozu::util::getBlockDeviceSize(file_.fd())) {
        if (!(flags & O_RDWR)) {
            throw RT_ERR("The flag must have O_RDWR.");
        }
    }
    bool write(uint64_t ioAddr, uint32_t ioBlocks, const void *data) {
        assert(data);
        return issueIo(ioAddr, ioBlocks, data);
    }
    bool discard(uint64_t ioAddr, uint32_t ioBlocks) {
        return issueIo(ioAddr, ioBlocks, nullptr);
    }
    void sync() {
        file_.fdatasync();
    }
private:
    bool issueIo(uint64_t ioAddr, uint32_t ioBlocks, const void *data = nullptr) {
        size_t oft = ioAddr * LOGICAL_BLOCK_SIZE;
        size_t size = ioBlocks * LOGICAL_BLOCK_SIZE;

        /* boundary check. */
        if (devSize_ < oft + size) return false;

        if (data) {
            file_.pwrite(data, size, oft);
        } else {
            cybozu::util::issueDiscard(file_.fd(), ioAddr, ioBlocks);
        }
        return true;
    }
};

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

class WdiffRedoManager
{
private:
    const Option &opt_;
    Statistics inStat_, outStat_;
    SimpleDiffIoExecutor ioExec_;
public:
    WdiffRedoManager(const Option &opt)
        : opt_(opt)
        , inStat_()
        , outStat_()
        , ioExec_(opt.devPath(), O_RDWR) {}
    void run() {
        /* Read a wdiff file and redo IOs in it. */
        cybozu::util::File file;
        if (opt_.inWdiffPath() == "-") {
            file.setFd(0);
        } else {
            file.open(opt_.inWdiffPath(), O_RDONLY);
        }
        BothDiffReader reader;
        IndexedDiffCache cache;
        cache.setMaxSize(32 * MEBI);
        reader.setCache(cache);
        reader.setFile(std::move(file));

        uint64_t addr;
        uint32_t blks;
        DiffRecType rtype;
        AlignedArray data;
        while (reader.read(addr, blks, rtype, data)) {
            executeDiffIo(addr, blks, rtype, data.data());
        }
        ::printf("Input statistics:\n");
        inStat_.print();
        ::printf("Output statistics:\n");
        outStat_.print();
    }

private:
    bool executeZeroIo(uint64_t ioAddr, uint32_t ioBlocks) {
        static AlignedArray zero;
        zero.resize(ioBlocks * LOGICAL_BLOCK_SIZE, true);
        return ioExec_.write(ioAddr, ioBlocks, zero.data());
    }
    bool executeDiscardIo(uint64_t ioAddr, uint32_t ioBlocks) {
        return ioExec_.discard(ioAddr, ioBlocks);
    }
    void executeDiffIo(uint64_t ioAddr, uint32_t ioBlocks, DiffRecType rtype, const void *data) {
        bool isSuccess = false;
        if (rtype == DiffRecType::ALLZERO) {
            isSuccess = executeZeroIo(ioAddr, ioBlocks);
            if (isSuccess) { outStat_.nIoAllZero++; }
            inStat_.nIoAllZero++;
        } else if (rtype == DiffRecType::DISCARD) {
            if (opt_.doDiscard()) {
                isSuccess = executeDiscardIo(ioAddr, ioBlocks);
            } else if (opt_.doZeroDiscard()) {
                isSuccess = executeZeroIo(ioAddr, ioBlocks);
            } else {
                /* Do nothing */
            }
            if (isSuccess) { outStat_.nIoDiscard++; }
            inStat_.nIoDiscard++;
        } else {
            /* Normal IO. */
            assert(rtype == DiffRecType::NORMAL);
            isSuccess = ioExec_.write(ioAddr, ioBlocks, data);
            if (isSuccess) { outStat_.nIoNormal++; }
            inStat_.nIoNormal++;
        }
        if (isSuccess) {
            outStat_.nBlocks += ioBlocks;
        } else {
            ::printf("Failed to redo: %" PRIu64 " %" PRIu32 " %s\n"
                     , ioAddr, ioBlocks, toStr(rtype));

        }
        inStat_.nBlocks += ioBlocks;
    }
};

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    WdiffRedoManager m(opt);
    m.run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-redo")
