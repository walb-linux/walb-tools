/**
 * @file
 * @brief Write random data with throughput control.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio2_util.hpp"
#include "walb_util.hpp"
#include "thread_util.hpp"
#include "easy_signal.hpp"
#include "random.hpp"

#include <chrono>

using namespace walb;
using Timespec = cybozu::Timespec;
using TimespecDiff = cybozu::TimespecDiff;

struct Option
{
    std::string bdevPath;
    bool dontUseAio;
    bool isDebug;
    std::string logPath;
    size_t maxThroughputMb;
    size_t intervalMb;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("writer: block device writer.");
        opt.appendParam(&bdevPath, "BLOCK_DEVICE_PATH");
        opt.appendOpt(&maxThroughputMb, 0, "throughput", ": max throughput [MB/s] (default unlimited)");
        opt.appendOpt(&intervalMb, 2, "interval", ": time calculation interval [MB] (default 2)");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");
        opt.appendOpt(&logPath, "-", "l", ": log output path. (default: stderr)");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

class SyncWriter
{
    cybozu::util::File file_;
    uint32_t pbs_;
    uint64_t devPb_;
    uint64_t aheadLsid_; // lsid_ % devPb_ = offsetPb.
    uint64_t doneLsid_;
    std::list<AlignedArray> queue_;
    uint32_t key_;

public:
    void open(const std::string& bdevPath, size_t /* queueSize */) {
        file_.open(bdevPath, O_RDWR | O_DIRECT);
        pbs_ = cybozu::util::getPhysicalBlockSize(file_.fd());
        devPb_ = cybozu::util::getBlockDeviceSize(file_.fd()) / pbs_;
        cybozu::util::flushBufferCache(file_.fd());
        // file_.lseek(0);
        aheadLsid_ = 0;
        doneLsid_ = 0;
        key_ = 0;
    }
    void reset(uint64_t lsid) {
        assert(queue_.empty());
        aheadLsid_ = lsid;
        doneLsid_ = lsid;
        file_.lseek((aheadLsid_ % devPb_) * pbs_);
    }
    uint64_t tailPb() const {
        uint64_t offsetPb = aheadLsid_ % devPb_;
        return devPb_ - offsetPb;
    }
    uint32_t prepare(AlignedArray&& buf) {
        assert(buf.size() % pbs_ == 0);
        const uint64_t pb = buf.size() / pbs_;
        assert(pb <= tailPb());
        queue_.push_back(std::move(buf));
        aheadLsid_ += pb;
        return key_++;
    }
    void submit() {
        while (!queue_.empty()) {
            AlignedArray& buf = queue_.front();
            writeBuf(buf.data(), buf.size());
            queue_.pop_front();
        }
        assert(aheadLsid_ == doneLsid_);
    }
    void wait(uint) {
        // Do nothing.
    }
    void sync() {
        file_.fdatasync();
    }
    uint32_t pbs() const {
        return pbs_;
    }
    uint64_t devPb() const {
        return devPb_;
    }
private:
    void writeBuf(const void* data, size_t size) {
        uint64_t pb = size / pbs_;
        assert(pb <= devPb_ - doneLsid_ % devPb_);
        file_.write(data, pb * pbs_);
        doneLsid_ += pb;
        if (doneLsid_ % devPb_ == 0) file_.lseek(0);
    }
};

class AsyncWriter
{
    cybozu::util::File file_;
    uint32_t pbs_;
    uint64_t devPb_;
    uint64_t aheadLsid_; // lsid_ % devPb_ = offsetPb.
    Aio2 aio_;

public:
    void open(const std::string& bdevPath, size_t queueSize) {
        file_.open(bdevPath, O_RDWR | O_DIRECT);
        pbs_ = cybozu::util::getPhysicalBlockSize(file_.fd());
        devPb_ = cybozu::util::getBlockDeviceSize(file_.fd()) / pbs_;
        cybozu::util::flushBufferCache(file_.fd());
        // file_.lseek(0);
        aheadLsid_ = 0;
        aio_.init(file_.fd(), queueSize * 2);
    }
    void reset(uint64_t lsid) {
        aheadLsid_ = lsid;
    }
    uint64_t tailPb() const {
        const uint64_t offsetPb = aheadLsid_ % devPb_;
        return devPb_ - offsetPb;
    }
    uint32_t prepare(AlignedArray&& buf) {
        assert(buf.size() % pbs_ == 0);
        const uint64_t givenPb = buf.size() / pbs_;
        assert(givenPb <= tailPb());
        const uint64_t offset = aheadLsid_ % devPb_ * pbs_;
        const uint32_t aioKey = aio_.prepareWrite(offset, std::move(buf));
        aheadLsid_ += givenPb;
        return aioKey;
    }
    void submit() {
        aio_.submit();
    }
    /**
     * This will be called from another thread.
     */
    void wait(uint32_t aioKey) {
        aio_.waitFor(aioKey);
    }
    void sync() {
        file_.fdatasync();
    }
    uint32_t pbs() const {
        return pbs_;
    }
    uint64_t devPb() const {
        return devPb_;
    }
};

struct PbRecord
{
    uint64_t lsid;
    char data[16];

    template <typename Rand>
    void set(uint64_t lsid, Rand& rand) {
        this->lsid = lsid;
        rand.fill(data, 16);
    }
};

template <typename Rand>
AlignedArray prepareData(uint32_t pbs, uint32_t pb, uint64_t lsid, Rand& rand)
{
    AlignedArray buf(pb * pbs, true);
    for (size_t i = 0; i < pb; i++) {
        PbRecord *rec = (PbRecord *)(buf.data() + i * pbs);
        rec->set(lsid + i, rand);
    }
    return buf;
}

struct IoRecord
{
    uint64_t lsid;
    size_t ioSizeB;
    uint32_t aioKey;

    friend inline std::ostream& operator<<(std::ostream& os, const IoRecord& rec) {
        os << rec.lsid << "\t" << rec.ioSizeB;
        return os;
    }
};

class ThroughputMonitor
{
    struct Rec {
        size_t ioSizeB;
        Timespec ts;
    };

    const size_t intervalB_;
    const size_t monitorMs_;
    std::deque<Rec> queue_;

    size_t totalB_;
    size_t localB_;

    mutable std::mutex mutex_;
    using AutoLock = std::lock_guard<std::mutex>;

public:
    /**
     * internalB: [bytes]
     * monitorMs: [ms]
     */
    ThroughputMonitor(size_t intervalB, size_t monitorMs)
        : intervalB_(intervalB)
        , monitorMs_(monitorMs)
        , queue_()
        , totalB_(0)
        , localB_(0)
        , mutex_() {
        assert(monitorMs > 0);
        assert(intervalB_ > 0);
    }
    void add(size_t ioSizeB) { // bytes.
        AutoLock lk(mutex_);
        localB_ += ioSizeB;
        if (queue_.empty() || localB_ >= intervalB_) {
            queue_.push_back(Rec{localB_, cybozu::getNowAsTimespec()});
            totalB_ += localB_;
            localB_ = 0;
            gc();
        }
    }
    size_t getThroughput() const {
        AutoLock lk(mutex_);
        Timespec ts0, ts1;
        if (queue_.empty()) return 0;
        ts0 = queue_.front().ts;
        ts1 = cybozu::getNowAsTimespec();
        TimespecDiff diff = ts1 - ts0;
        if (diff == 0) return 0;
        return size_t((totalB_ + localB_) / diff.getAsDouble());
    }
private:
    void gc() {
        if (queue_.size() < 2) return;
        TimespecDiff diff = queue_.back().ts - queue_.front().ts;
        TimespecDiff monitorDiff;
        monitorDiff.setAsDouble(monitorMs_ / (double)1000);
        while (diff > monitorDiff) {
            totalB_ -= queue_.front().ioSizeB;
            queue_.pop_front();
            if (queue_.size() < 2) return;
            diff = queue_.back().ts - queue_.front().ts;
        }
    }
};


using Queue = cybozu::thread::BoundedQueue<IoRecord>;
std::atomic<bool> failed_(false);

template <typename Writer>
void doWriteSubmit(Writer& writer, uint64_t aheadPb, const std::atomic<uint64_t>& readPb, Queue& outQ,
                   ThroughputMonitor &thMon, size_t maxThroughput)
try {
    const uint32_t pbs = writer.pbs();
    const uint32_t maxIoPb = 64 * KIBI / pbs;
    uint64_t lsid = 0;
    writer.reset(lsid % writer.devPb());
    cybozu::util::Random<uint64_t> rand;
    uint64_t writtenPb = 0;
    std::queue<IoRecord> tmpQ;

    while (!cybozu::signal::gotSignal()) {
        if (failed_) return;
        if (readPb + aheadPb < writtenPb) {
            util::sleepMs(1); // backpressure.
            continue;
        }
        const uint32_t pb = std::min<uint64_t>(writer.tailPb(), 1 + rand() % maxIoPb);
        AlignedArray buf = prepareData(pbs, pb, lsid, rand);
        const uint32_t aioKey = writer.prepare(std::move(buf));
        tmpQ.push(IoRecord{lsid, pb * pbs, aioKey});
        if (tmpQ.size() >= 8 || rand() % 10 == 0) {
            writer.submit();
            while (!tmpQ.empty()) {
                const IoRecord& rec = tmpQ.front();
                LOGs.debug() << "write" << rec;
                thMon.add(rec.ioSizeB);
                outQ.push(rec);
                tmpQ.pop();
            }
            if (rand() % 1000 == 0) writer.sync();
            while (thMon.getThroughput() > maxThroughput) {
                util::sleepMs(1);
            }
        }
        lsid += pb;
        writtenPb += pb;
    }
    if (!tmpQ.empty()) {
        writer.submit();
            while (!tmpQ.empty()) {
                const IoRecord& rec = tmpQ.front();
                LOGs.debug() << "write" << rec;
                outQ.push(rec);
                tmpQ.pop();
            }
    }
    outQ.sync();
} catch (...) {
    outQ.fail();
    failed_ = true;
    throw;
}

template <typename Writer>
void doWriteWait(Writer& writer, std::atomic<uint64_t>& readPb, Queue& inQ)
try {
    const uint32_t pbs = writer.pbs();
    IoRecord ioRec;
    while (inQ.pop(ioRec)) {
        if (failed_) return;
        writer.wait(ioRec.aioKey);
        readPb += ioRec.ioSizeB / pbs;
    }
} catch (...) {
    inQ.fail();
    failed_ = true;
    throw;
}

void doMonitor(std::atomic<uint64_t>& readPb, size_t intervalS, uint64_t devPb, ThroughputMonitor &thMon)
try {
    LOGs.info() << "starting..." << intervalS;
    const double interval = double(intervalS);
    double t0 = cybozu::util::getTime();
    while (!cybozu::signal::gotSignal() && !failed_) {
        util::sleepMs(100);
        double t1 = cybozu::util::getTime();
        if (t1 - t0 > interval) {
            const uint64_t pb = readPb.load();
            const size_t throughput = thMon.getThroughput();
            LOGs.info() << "progress" << pb << pb / devPb << throughput;
            t0 = t1;
        }
    }
    LOGs.info() << "terminate...";
} catch (...) {
    failed_ = true;
    throw;
}

uint32_t getPbs(const std::string& bdevPath)
{
    cybozu::util::File file(bdevPath, O_RDONLY);
    return cybozu::util::getPhysicalBlockSize(file.fd());
}

template <typename Writer>
void writeAndVerify(const Option& opt)
{
    const uint32_t pbs = getPbs(opt.bdevPath);
    const size_t queueSize = 4 * MEBI / pbs;
    Queue queue(queueSize);
    Writer writer;
    writer.open(opt.bdevPath, queueSize);
    std::atomic<uint64_t> readPb(0);
    const uint64_t aheadPb = 128 * MEBI / pbs;
    const uint64_t devPb = writer.devPb();
    LOGs.info() << "devPb" << devPb;
    LOGs.info() << "pbs" << pbs;
    ThroughputMonitor thMon(1000000, 3000);
    const size_t maxThroughput = opt.maxThroughputMb * 1000 * 1000;

    cybozu::thread::ThreadRunnerSet thS;
    thS.add([&]() { doWriteSubmit<Writer>(writer, aheadPb, readPb, queue, thMon, maxThroughput); });
    thS.add([&]() { doWriteWait<Writer>(writer, readPb, queue); });
    thS.add([&]() { doMonitor(readPb, 5, devPb, thMon); } );
    thS.start();

    std::vector<std::exception_ptr> epV = thS.join();
    for (std::exception_ptr ep : epV) {
        if (ep) std::rethrow_exception(ep);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting(opt.logPath, opt.isDebug);
    cybozu::signal::setSignalHandler({SIGINT, SIGQUIT, SIGTERM});

    if (opt.dontUseAio) {
        writeAndVerify<SyncWriter>(opt);
    } else {
        writeAndVerify<AsyncWriter>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("writer-verifier")
