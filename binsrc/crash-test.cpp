/**
 * Crash test of walb.
 */
#include <thread>
#include <atomic>
#include <string>
#include <vector>
#include <cassert>
#include <cstdio>
#include <chrono>
#include <unistd.h>
#include "thread_util.hpp"
#include "walb_logger.hpp"
#include "walb_util.hpp"
#include "walb_types.hpp"
#include "fileio.hpp"
#include "random.hpp"
#include "cybozu/option.hpp"

using namespace walb;

const size_t CACHE_LINE_SIZE = 64;

struct Counter
{
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> cnt;
    Counter() : cnt(0) {}
    Counter(const Counter& rhs) : cnt(rhs.cnt.load()) {}
};

alignas(CACHE_LINE_SIZE) std::vector<size_t> wcntFlushV_;
alignas(CACHE_LINE_SIZE) std::vector<Counter> wcntV_;
alignas(CACHE_LINE_SIZE) std::atomic<bool> quit_(false);

size_t getIntFromBuffer(const AlignedArray& buf)
{
    assert(buf.size() >= sizeof(size_t));
    size_t value;
    ::memcpy(&value, buf.data(), sizeof(value));
    return value;
}

void setIntToBuffer(AlignedArray& buf, size_t value, size_t offset = 0)
{
    assert(buf.size() >= offset + sizeof(size_t));
    ::memcpy(buf.data() + offset, &value, sizeof(value));
}

struct Command
{
    virtual void setupOption(cybozu::Option&) = 0;
    virtual void run() = 0;
};

struct WriteCommand : Command
{
    std::string bdevPath;

    size_t flushIntervalMs;
    size_t ioIntervalMs;
    size_t blockSize;
    size_t nr_;
    size_t timeoutSec;
    bool isOverlap_;

    void setupOption(cybozu::Option& opt) override {
        opt.appendOpt(&flushIntervalMs, 100, "fi", "flush interval [ms]");
        opt.appendOpt(&ioIntervalMs, 5, "ii", "write IO interval [ms]");
        opt.appendOpt(&blockSize, 512, "bs", "block size [byte]");
        opt.appendOpt(&nr_, 1, "nr", "number of blocks(threads)");
        opt.appendOpt(&timeoutSec, 10, "to", "timeout [sec]");
        opt.appendBoolOpt(&isOverlap_, "ol",
                          "overlap test (-nr option ignored. 7blocks/6threads will be used.)");

        opt.appendParam(&bdevPath, "BDEV_PATH", "block device path");
    }
    void run() override {
        bool timeout = false;
        cybozu::thread::ThreadRunnerSet thSet;

        if (isOverlap_) {
            LOGi("overlap test");
            nr_ = 6;
        } else {
            LOGi("normal test");
        }

        wcntV_.resize(nr_);
        for (size_t i = 0; i < nr_; i++) {
            if (isOverlap_) {
                wcntV_[i].cnt = i;
            } else {
                wcntV_[i].cnt = 0;
            }
        }
        wcntFlushV_.resize(nr_, 0);
        thSet.add([this]() { flushWork(); });
        for (size_t i = 0; i < nr_; i++) {
            thSet.add([i,this]() { writeWork(i); });
        }

        const double t0 = cybozu::util::getTime();
        thSet.start();
        while (!quit_) {
            const double t1 = cybozu::util::getTime();
            if (t1 - t0 > static_cast<double>(timeoutSec)) {
                quit_ = true;
                timeout = true;
                break;
            }
            util::sleepMs(ioIntervalMs);
        }
        bool error = false;
        std::vector<std::exception_ptr> epV = thSet.join();
        for (size_t i = 0; i < epV.size(); i++) {
            std::exception_ptr& ep = epV[i];
            if (!ep) continue;
            error = true;
            try {
                std::rethrow_exception(ep);
            } catch (std::exception& e) {
                LOGs.error() << "thread error" << i << e.what();
            }
        }
        if (timeout) throw cybozu::Exception("timeout");
        if (error) throw cybozu::Exception("error");

        for (size_t i = 0; i < nr_; i++) {
            ::printf("%2zu %10zu %10zu\n", i, wcntFlushV_[i], wcntV_[i].cnt.load());
        }
    }
private:
    void flushWork() {
        cybozu::util::File file(bdevPath, O_RDWR | O_DIRECT);
        std::vector<size_t> v(nr_);

        const int range = flushIntervalMs / 2;
        cybozu::util::Random<int> rand(-range, range);

        while (!quit_) {
            for (size_t i = 0; i < nr_; i++) {
                v[i] = wcntV_[i].cnt;
            }
            try {
                file.fdatasync();
            } catch (...) {
                quit_ = true;
                return;
            }
            for (size_t i = 0; i < nr_; i++) {
                wcntFlushV_[i] = v[i];
            }
            util::sleepMs(flushIntervalMs + rand());
        }
    }
    void setupNormalTest(size_t id, off_t& off, size_t& size) {
        off = id * blockSize;
        size = blockSize;
    }
    void setupOverlapTest(size_t id, off_t& off, size_t& size) {
        switch (id) {
            /*
             * block   0
             * case 0: x
             * case 1: x
             */
        case 0:
        case 1:
            off = 0;
            size = 1;
            break;

            /*
             * block   123
             * case 2: xx
             * case 3:  xx
             */
        case 2:
            off = 1;
            size = 2;
            break;
        case 3:
            off = 2;
            size = 2;
            break;

            /*
             * block   456
             * case 4: xxx
             * case 5:  x
             */
        case 4:
            off = 4;
            size = 3;
            break;
        case 5:
            off = 5;
            size = 1;
            break;
        default:
            throw cybozu::Exception(__func__) << "bad id" << id;
        }
        off *= blockSize;
        size *= blockSize;
    }
    void setIntToBlocks(AlignedArray& buf, size_t value) {
        assert(buf.size() % blockSize == 0);
        const size_t n = buf.size() / blockSize;
        for (size_t off = 0; off < n; off++) {
            setIntToBuffer(buf, value, off * blockSize);
        }
    }
    void writeWork(size_t id) {
        cybozu::util::File file(bdevPath, O_RDWR | O_DIRECT);

        const int range = ioIntervalMs / 2;
        cybozu::util::Random<int> rand(-range, range);

        std::atomic<size_t> &wcnt = wcntV_[id].cnt;
        off_t off;
        size_t size;
        size_t diff;
        if (isOverlap_) {
            assert(wcnt == id);
            setupOverlapTest(id, off, size);
            diff = 6;
        } else {
            assert(wcnt == 0);
            setupNormalTest(id, off, size);
            diff = 1;
        }
        AlignedArray buf(size);
        assert(size >= sizeof(size_t));

        while (!quit_) {
            setIntToBlocks(buf, wcnt + diff);
            try {
                file.pwrite(buf.data(), size, off);
            } catch (...) {
                quit_ = true;
                wcnt += diff; // the last IO may have succeeded.
                break;
            }
            wcnt += diff;
            util::sleepMs(ioIntervalMs + rand());
        }
    }
} writeCommand_;

struct ReadCommand : Command
{
    std::string bdevPath;
    size_t blockSize;
    size_t nr_;
    bool isOverlap_;

    void setupOption(cybozu::Option& opt) override {
        opt.appendOpt(&blockSize, 512, "bs", "block size [byte]");
        opt.appendOpt(&nr_, 1, "nr", "number of blocks");
        opt.appendBoolOpt(&isOverlap_, "ol",
                          "overlap test (-nr option ignored. 7blocks/6threads will be used.)");

        opt.appendParam(&bdevPath, "BDEV_PATH", "block device path");
    }
    void run() override {
        const size_t size = blockSize;
        AlignedArray buf(size);
        cybozu::util::File file(bdevPath, O_RDONLY | O_DIRECT);

        if (isOverlap_) nr_ = 7;

        for (size_t i = 0; i < nr_; i++) {
            const off_t off = i * size;
            file.pread(buf.data(), size, off);
            const size_t wcnt = getIntFromBuffer(buf);
            ::printf("%2zu %10zu\n", i, wcnt);
        }
    }
} readCommand_;

struct VerifyCommand : Command
{
    bool isOverlap_;
    std::string writeOut;
    std::string readOut;

    void setupOption(cybozu::Option& opt) override {
        opt.appendBoolOpt(&isOverlap_, "ol", "overlap test");

        opt.appendParam(&writeOut, "WRITE_OUT", "output file of write command");
        opt.appendParam(&readOut, "READ_OUT", "output file of read command");
    }

    using Tuple = std::vector<size_t>;
    using TupleVec = std::vector<Tuple>;

    static Tuple parseTuple(const StrVec &rec, size_t nr) {
        if (rec.size() != nr) {
            throw cybozu::Exception("invalid number of columns") << rec.size() << nr;
        }
        std::vector<size_t> ret;
        for (const std::string& s : rec) {
            ret.push_back(static_cast<size_t>(cybozu::atoi(s)));
        }
        return ret;
    }
    static TupleVec readTupleVec(const std::string &path, size_t nr) {
        std::string buf;
        cybozu::util::readAllFromFile(path, buf);

        StrVec v0 = cybozu::util::splitString(buf, "\r\n");
        cybozu::util::removeEmptyItemFromVec(v0);

        TupleVec ret;
        for (const std::string& s : v0) {
            StrVec v1 = cybozu::util::splitString(s, " \t");
            cybozu::util::removeEmptyItemFromVec(v1);
            ret.push_back(parseTuple(v1, nr));
        }
        return ret;
    }
    struct Record {
        size_t tid;
        size_t wcntFlush;
        size_t rcnt;
        size_t wcntLatest;

        void set(Tuple &wTpl, Tuple& rTpl) {
            assert(wTpl.size() == 3);
            assert(rTpl.size() == 2);
            tid = wTpl[0];
            wcntFlush = wTpl[1];
            rcnt = rTpl[1];
            wcntLatest = wTpl[2];
        }
        bool isValid() const {
            return wcntFlush <= rcnt && rcnt <= wcntLatest;
        }
        void print() const {
            const size_t diff0 = rcnt - wcntFlush;
            const size_t diff1 = wcntLatest - rcnt;
            ::printf("%2s %2zu %10zu %10zu %10zu +%04zu +%04zu\n"
                     , (isValid() ? "OK" : "NG")
                     , tid, wcntFlush, rcnt, wcntLatest, diff0, diff1);
        }
    };
    void run() override {
        /*
         * write tuple: (thread id, wcntFlush, wcntLatest)
         * read tuple:  (block id, rcnt)
         */
        TupleVec w = readTupleVec(writeOut, 3);
        TupleVec r = readTupleVec(readOut, 2);
        if (!isOverlap_ && w.size() != r.size()) {
            throw cybozu::Exception("nr of records differ") << w.size() << r.size();
        }
        size_t nrInvalid = 0;
        for (size_t i = 0; i < r.size(); i++) {
            Record rec;
            if (isOverlap_) {
                const size_t tid = r[i][1] % 6;
                rec.set(w[tid], r[i]);
            } else {
                if (w[i][0] != r[i][0]) {
                    throw cybozu::Exception("thread id and block id must be the same")
                        << w[i][0] << r[i][0];
                }
                rec.set(w[i], r[i]);
            }
            rec.print();
            if (!rec.isValid()) nrInvalid++;
        }
        ::printf("number of invalid records: %zu\n", nrInvalid);
        if (nrInvalid != 0) {
            throw cybozu::Exception("invalid record found.");
        }
    }
} verifyCommand_;

struct {
    const char *name;
    Command *cmd;
    const char *shortHelp;
} cmdTbl_[] = {
    {"write",  &writeCommand_,  "write [<opt>] BDEV_PATH"},
    {"read",   &readCommand_,   "read [<opt>] BDEV_PATH"},
    {"verify", &verifyCommand_, "verify WRITE_OUTPUT READ_OUTPUT"},
};

void setOpt0Usage(cybozu::Option& opt0)
{
    std::string usage =
        "Usage: crash-test [<opt>] <command> [<opt>] [<args>]\n\n"
        "Command list:\n";
    for (auto& cmd : cmdTbl_) {
        usage += "  ";
        usage += cmd.shortHelp;
        usage += "\n";
    }
    usage +=
        "\n""Option list:\n"
        "  -debug  show debug messages to stderr.\n"
        "  -h      show this help.\n\n"
        "Run 'crash-test <command> -h' for more information on a specific command.";
    opt0.setUsage(usage);
}

void showHelpAndExit(cybozu::Option& opt)
{
    opt.usage();
    ::exit(1);
}

int doMain(int argc, char* argv[])
{
    bool isDebug;
    cybozu::Option opt0, opt1;
    opt0.appendBoolOpt(&isDebug, "debug");
    opt0.appendHelp("h");
    for (auto& c : cmdTbl_) {
        opt0.appendDelimiter(c.name);
    }
    setOpt0Usage(opt0);
    if (!opt0.parse(argc, argv)) {
        showHelpAndExit(opt0);
    }
    util::setLogSetting("-", isDebug);
    const int cmdPos = opt0.getNextPositionOfDelimiter();
    if (cmdPos == 0) {
        showHelpAndExit(opt0);
    }
    const std::string cmdName = argv[cmdPos - 1];
    Command *cmd = nullptr;
    for (auto& c : cmdTbl_) {
        if (c.name == cmdName) {
            cmd = c.cmd;
            break;
        }
    }
    if (!cmd) {
        showHelpAndExit(opt0);
    }
    cmd->setupOption(opt1);
    opt1.appendHelp("h");
    if (!opt1.parse(argc, argv, cmdPos)) {
        showHelpAndExit(opt1);
    }
    cmd->run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("crash-test")
