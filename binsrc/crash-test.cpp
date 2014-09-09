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

struct Option
{
    std::string command;
    std::string bdevPath;

    size_t flushIntervalMs;
    size_t ioIntervalMs;
    size_t blockSize;
    size_t nrThreads;
    size_t timeoutSec;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;

        opt.appendOpt(&flushIntervalMs, 100, "fi", "flush interval [ms]");
        opt.appendOpt(&ioIntervalMs, 5, "ii", "write IO interval [ms]");
        opt.appendOpt(&blockSize, 512, "bs", "block size [byte]");
        opt.appendOpt(&nrThreads, 1, "th", "number of threads");
        opt.appendOpt(&timeoutSec, 10, "to", "timeout [sec]");

        opt.appendParam(&command, "COMMAND", "write or verify");
        opt.appendParam(&bdevPath, "BDEV_PATH", "block device path");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

void flushWork(const Option& opt)
{
    cybozu::util::File file(opt.bdevPath, O_RDWR | O_DIRECT);

    while (!quit_) {
        fcnt_++;
        try {
            file.fdatasync();
        } catch (...) {
            quit_ = true;
            return;
        }
        util::sleepMs(opt.flushIntervalMs);
    }
}

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

void writeWork(size_t id, const Option& opt)
{
    cybozu::util::File file(opt.bdevPath, O_RDWR | O_DIRECT);

    size_t fcntPrev = fcnt_;
    size_t wcntFlush = 0;
    size_t wcntLatest = 0;

    const off_t off = id * opt.blockSize;
    const size_t size = opt.blockSize;
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
        util::sleepMs(opt.ioIntervalMs);
    }
    wcntV_[id].wcntFlush = wcntFlush;
    wcntV_[id].wcntLatest = wcntLatest;
}

void doWrite(const Option& opt)
{
    bool timeout = false;
    cybozu::thread::ThreadRunnerSet thSet;

    wcntV_.resize(opt.nrThreads);
    thSet.add([&]() { flushWork(opt); });
    for (size_t i = 0; i < opt.nrThreads; i++) {
        thSet.add([i,&opt]() { writeWork(i, opt); });
    }

    const double t0 = cybozu::util::getTime();
    thSet.start();
    while (!quit_) {
        const double t1 = cybozu::util::getTime();
        if (t1 - t0 > static_cast<double>(opt.timeoutSec)) {
            quit_ = true;
            timeout = true;
            break;
        }
        util::sleepMs(opt.ioIntervalMs);
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

void doVerify(const Option& opt)
{
    const size_t size = opt.blockSize;
    AlignedArray buf(size);
    cybozu::util::File file(opt.bdevPath, O_RDONLY | O_DIRECT);

    for (size_t i = 0; i < opt.nrThreads; i++) {
        const off_t off = i * size;
        file.pread(buf.data(), size, off);
        const size_t wcnt = getIntFromBuffer(buf);
        ::printf("%2zu %10zu\n", i, wcnt);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    if (opt.command == "write") {
        doWrite(opt);
    } else if (opt.command == "verify") {
        doVerify(opt);
    } else {
        throw cybozu::Exception("comand name must be write or verify.");
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("crash-test")
