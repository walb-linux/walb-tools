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
#include "cybozu/option.hpp"

using namespace walb;

struct Counter
{
    size_t wcntFlush;
    size_t wcntLatest;
};

std::vector<Counter> wcntV_;
std::atomic<size_t> fcnt_(0);
std::atomic<bool> quit_(false);

size_t getIntFromBuffer(const AlignedArray& buf)
{
    assert(buf.size() >= sizeof(size_t));
    size_t value;
    ::memcpy(&value, buf.data(), sizeof(value));
    return value;
}

void setIntToBuffer(AlignedArray& buf, size_t value)
{
    assert(buf.size() >= sizeof(size_t));
    ::memcpy(buf.data(), &value, sizeof(value));
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
    size_t nrThreads;
    size_t timeoutSec;

    void setupOption(cybozu::Option& opt) override {
        opt.appendOpt(&flushIntervalMs, 100, "fi", "flush interval [ms]");
        opt.appendOpt(&ioIntervalMs, 5, "ii", "write IO interval [ms]");
        opt.appendOpt(&blockSize, 512, "bs", "block size [byte]");
        opt.appendOpt(&nrThreads, 1, "th", "number of threads");
        opt.appendOpt(&timeoutSec, 10, "to", "timeout [sec]");

        opt.appendParam(&bdevPath, "BDEV_PATH", "block device path");
    }
    void run() override {
        bool timeout = false;
        cybozu::thread::ThreadRunnerSet thSet;

        wcntV_.resize(nrThreads);
        thSet.add([this]() { flushWork(); });
        for (size_t i = 0; i < nrThreads; i++) {
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

        for (size_t i = 0; i < wcntV_.size(); i++) {
            ::printf("%2zu %10zu %10zu\n", i, wcntV_[i].wcntFlush, wcntV_[i].wcntLatest);
        }
    }
private:
    void flushWork() {
        cybozu::util::File file(bdevPath, O_RDWR | O_DIRECT);

        while (!quit_) {
            fcnt_++;
            try {
                file.fdatasync();
            } catch (...) {
                quit_ = true;
                return;
            }
            util::sleepMs(flushIntervalMs);
        }
    }
    void writeWork(size_t id) {
        cybozu::util::File file(bdevPath, O_RDWR | O_DIRECT);

        size_t fcntPrev = fcnt_;
        size_t wcntFlush = 0;
        size_t wcntLatest = 0;

        const off_t off = id * blockSize;
        const size_t size = blockSize;
        AlignedArray buf(size);

        assert(size >= sizeof(size_t));

        while (!quit_) {
            const size_t fcntLatest = fcnt_;
            if (fcntLatest != fcntPrev) {
                wcntFlush = wcntLatest;
                fcntPrev = fcntLatest;
            }
            wcntLatest++;
            setIntToBuffer(buf, wcntLatest);
            try {
                file.pwrite(buf.data(), size, off);
            } catch (...) {
                quit_ = true;
                break;
            }
            util::sleepMs(ioIntervalMs);
        }
        wcntV_[id].wcntFlush = wcntFlush;
        wcntV_[id].wcntLatest = wcntLatest;
    }
} writeCommand_;

struct ReadCommand : Command
{
    std::string bdevPath;
    size_t blockSize;
    size_t nrThreads;

    void setupOption(cybozu::Option& opt) override {
        opt.appendOpt(&blockSize, 512, "bs", "block size [byte]");
        opt.appendOpt(&nrThreads, 1, "th", "number of threads");

        opt.appendParam(&bdevPath, "BDEV_PATH", "block device path");
    }
    void run() override {
        const size_t size = blockSize;
        AlignedArray buf(size);
        cybozu::util::File file(bdevPath, O_RDONLY | O_DIRECT);

        for (size_t i = 0; i < nrThreads; i++) {
            const off_t off = i * size;
            file.pread(buf.data(), size, off);
            const size_t wcnt = getIntFromBuffer(buf);
            ::printf("%2zu %10zu\n", i, wcnt);
        }
    }
} readCommand_;

struct VerifyCommand : Command
{
    std::string writeOut;
    std::string readOut;

    void setupOption(cybozu::Option& opt) override {
        opt.appendParam(&writeOut, "WRITE_OUT", "output file of write command");
        opt.appendParam(&readOut, "READ_OUT", "output file of read command");
    }
    using Records = std::vector<StrVec>;
    Records readRecords(const std::string &path) {
        std::string buf;
        cybozu::util::readAllFromFile(path, buf);

        StrVec v0 = cybozu::util::splitString(buf, "\r\n");
        cybozu::util::removeEmptyItemFromVec(v0);

        Records ret;
        for (const std::string& s : v0) {
            StrVec v1 = cybozu::util::splitString(s, " \t");
            cybozu::util::removeEmptyItemFromVec(v1);
            ret.push_back(std::move(v1));
        }
        return ret;
    }
    struct Record {
        size_t id;
        size_t wcntFlush;
        size_t rcnt;
        size_t wcntLatest;

        void set(StrVec &writeRec, StrVec& readRec) {
            if (writeRec.size() != 3) {
                throw cybozu::Exception("invalid write record") << writeRec.size();
            }
            if (readRec.size() != 2) {
                throw cybozu::Exception("invalid write record") << writeRec.size();
            }
            if (writeRec[0] != readRec[0]) {
                throw cybozu::Exception("invalid thread id") << writeRec[0] << readRec[0];
            }
            id = cybozu::atoi(writeRec[0]);
            wcntFlush = cybozu::atoi(writeRec[1]);
            rcnt = cybozu::atoi(readRec[1]);
            wcntLatest = cybozu::atoi(writeRec[2]);
        }
        bool isValid() const {
            if (wcntFlush > rcnt) return false;
            if (rcnt > wcntLatest) return false;
            return true;
        }
        void print() const {
            ::printf("%2s %2zu %10zu %10zu %10zu\n"
                     , (isValid() ? "OK" : "NG")
                     , id, wcntFlush, rcnt, wcntLatest);
        }
    };
    void run() override {
        Records w = readRecords(writeOut);
        Records r = readRecords(readOut);
        if (w.size() != r.size()) {
            throw cybozu::Exception("nr of records differ") << w.size() << r.size();
        }
        size_t nrInvalid = 0;
        for (size_t i = 0; i < w.size(); i++) {
            Record rec;
            rec.set(w[i], r[i]);
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
    bool found = false;
    Command *cmd = nullptr;
    for (auto& c : cmdTbl_) {
        if (c.name == cmdName) {
            cmd = c.cmd;
            found = true;
            break;
        }
    }
    if (!found) {
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
