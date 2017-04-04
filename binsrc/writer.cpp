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
    size_t ioSizeB;
    size_t aheadSizeB;
    size_t maxThroughputMb;
    size_t monitorIntervalSec;
    size_t fillPct;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("writer: block device writer.");
        opt.appendParam(&bdevPath, "BLOCK_DEVICE_PATH");
        opt.appendOpt(&maxThroughputMb, 0, "throughput", ": max throughput [MB/s] (default unlimited)");
        opt.appendOpt(&ioSizeB, 64 * KIBI, "iosize", ": io size [bytes] (default 64K)");
        opt.appendOpt(&aheadSizeB, 64 * MEBI, "ahead", ": ahead size [bytes] (default 64M)");
        opt.appendOpt(&monitorIntervalSec, 10, "monintvl", ": monitor interval [sec] (default 10)");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");
        opt.appendOpt(&fillPct, 100, "fill", ": filling percentage (default: 0)");
        opt.appendOpt(&logPath, "-", "l", ": log output path. (default: stderr)");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (monitorIntervalSec == 0) throw cybozu::Exception("-monintvl must not be 0.");
        if (fillPct > 100) {
            throw cybozu::Exception("-fill must between 0 and 100") << fillPct;
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
AlignedArray prepareData(uint32_t pbs, uint32_t pb, uint64_t lsid, Rand& rand, size_t fillPct)
{
    assert(fillPct <= 100);

    AlignedArray buf(pb * pbs, false);
    const size_t fillSize = fillPct * buf.size() / 100;
    rand.fill(buf.data(), fillSize);
    ::memset(buf.data() + fillSize, 0, buf.size() - fillSize);

#if 0
    for (size_t i = 0; i < pb; i++) {
        PbRecord *rec = (PbRecord *)(buf.data() + i * pbs);
        rec->set(lsid + i, rand);
    }
#else
    (void)lsid;
#endif

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

    size_t intervalB_;
    const size_t monitorMs_;
    std::deque<Rec> queue_;

    size_t totalB_;
    size_t localB_;

    mutable std::mutex mutex_;
    using AutoLock = std::lock_guard<std::mutex>;

public:
    /**
     * intervalB: [bytes] (initial value)
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
    /**
     * RETURNS:
     *   0 if throughput does not calculated, else throughput.
     */
    size_t add(size_t ioSizeB) { // bytes.
        AutoLock lk(mutex_);
        localB_ += ioSizeB;
        if (queue_.empty() || localB_ >= intervalB_) {
            const Timespec now = cybozu::getNowAsTimespec();
            if (!queue_.empty()) adjustInterval(now - queue_.back().ts);
            queue_.push_back(Rec{localB_, now});
            totalB_ += localB_;
            localB_ = 0;
            gc();
            return getThroughputDetail(now);
        } else {
            return 0;
        }
    }
    size_t getThroughput() const {
        AutoLock lk(mutex_);
        const Timespec now = cybozu::getNowAsTimespec();
        return getThroughputDetail(now);
    }
private:
    void adjustInterval(const TimespecDiff& diff) {
        const size_t UPPER_LIMIT_SIZE = 100 * MEGA;
        const size_t LOWER_LIMIT_SIZE = MEGA;
        const double d = diff.getAsDouble();
        if (d < 0.01) { //10ms
            size_t intvl;
            if (d < 0.005) { //5ms
                intvl = intervalB_ * 2;
            } else {
                intvl = intervalB_ * 11 / 10;
            }
            intervalB_ = std::min(intvl, UPPER_LIMIT_SIZE);
            //LOGs.info() << __func__ << intervalB_;
        } else if (d > 0.02) { //20ms
            size_t intvl;
            if (d > 0.05) { //50ms
                intvl = intervalB_ / 2;
            } else {
                intvl = intervalB_ * 9 / 10;
            }
            intervalB_ = std::max(intvl, LOWER_LIMIT_SIZE);
            //LOGs.info() << __func__ << intervalB_;
        }
    }
    size_t getThroughputDetail(const Timespec& now) const {
        if (queue_.empty()) return 0;
        const TimespecDiff diff = now - queue_.front().ts;
        if (diff == 0) return 0;
        return size_t((totalB_ + localB_) / diff.getAsDouble());
    }
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
                   ThroughputMonitor &thMon, size_t maxThroughput, size_t fillPct, uint32_t ioPb)
try {
    const uint32_t pbs = writer.pbs();
    uint64_t lsid = 0;
    writer.reset(lsid % writer.devPb());
    cybozu::util::Xoroshiro128Plus rand(::time(0));

    uint64_t writtenPb = 0;

    while (!cybozu::signal::gotSignal()) {
        if (failed_) return;
        if (readPb + aheadPb < writtenPb) {
            util::sleepMs(1); // backpressure.
            continue;
        }
        const uint32_t pb = std::min<uint64_t>(writer.tailPb(), ioPb);
        AlignedArray buf = prepareData(pbs, pb, lsid, rand, fillPct);
        const uint32_t aioKey = writer.prepare(std::move(buf));
        IoRecord rec{lsid, pb * pbs, aioKey};
        writer.submit();
        LOGs.debug() << "write" << rec;
        const size_t throughput = thMon.add(rec.ioSizeB);
        outQ.push(rec);
#if 0
        if (rand() % 10000 == 0) writer.sync();
#endif
        if (maxThroughput > 0 && throughput > 0 && throughput > maxThroughput) {
            do {
                util::sleepMs(1);
            } while (thMon.getThroughput() > maxThroughput);
        }
        lsid += pb;
        writtenPb += pb;
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
    const uint32_t ioPb = opt.ioSizeB / pbs;
    if (ioPb == 0) throw cybozu::Exception("too small io size") << opt.ioSizeB;
    const uint64_t aheadPb = opt.aheadSizeB / pbs;
    if (aheadPb < ioPb) throw cybozu::Exception("too small ahead size") << opt.aheadSizeB;
    const size_t queueSize = aheadPb / ioPb + 1;
    Queue queue(queueSize);
    Writer writer;
    writer.open(opt.bdevPath, queueSize);
    const uint64_t devPb = writer.devPb();
    LOGs.info() << "devPb" << devPb;
    LOGs.info() << "pbs" << pbs;
    std::atomic<uint64_t> readPb(0);
    ThroughputMonitor thMon(MEGA, 3000);
    const size_t maxThroughput = opt.maxThroughputMb * MEGA;

    cybozu::thread::ThreadRunnerSet thS;
    thS.add([&]() { doWriteSubmit<Writer>(writer, aheadPb, readPb, queue, thMon,
                                          maxThroughput, opt.fillPct, ioPb); });
    thS.add([&]() { doWriteWait<Writer>(writer, readPb, queue); });
    thS.add([&]() { doMonitor(readPb, opt.monitorIntervalSec, devPb, thMon); } );
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
