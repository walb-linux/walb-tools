/**
 * @file
 * @brief Writer writes data and read/verifier reads and verify written data.
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

using namespace walb;

struct Option
{
    std::string bdevPath;
    bool dontUseAio;
    bool isDebug;
    size_t aheadSize;
    std::string logPath;
    size_t sleepMs;
    size_t intervalS;
    bool isRandom;
    size_t nrThreads;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("writer-verifier: block device test tool.");
        opt.appendParam(&bdevPath, "BLOCK_DEVICE_PATH");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");
        opt.appendOpt(&aheadSize, 16 * MEBI, "ahead", ": ahead size of write position to read position [bytes]");
        opt.appendOpt(&sleepMs, 0, "s", ": sleep milliseconds for each read. (default: 0)");
        opt.appendOpt(&intervalS, 60, "i", ": interval seconds between monitoring messages. (default: 60)");
        opt.appendOpt(&logPath, "-", "l", ": log output path. (default: stderr)");
        opt.appendBoolOpt(&isRandom, "r", ": random writes instead sequential ones.");
        opt.appendOpt(&nrThreads, 1, "t", ": number of threads (for random workload only).");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

std::string csum2str(uint32_t csum)
{
    return cybozu::util::formatString("%08x", csum);
}

/**
 * Record for each IO management.
 */
struct IoRecord
{
    uint64_t lsid; // lsid % devPb = offsetPb.
    uint32_t sizePb;
    uint32_t csum;
    uint32_t aioKey;

    friend inline std::ostream& operator<<(std::ostream& os, const IoRecord& rec) {
        os << rec.lsid << "\t" << rec.sizePb << "\t" << csum2str(rec.csum);
        return os;
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

class SyncReader
{
    cybozu::util::File file_;
    uint32_t pbs_;
    uint64_t devPb_;
    uint64_t lsid_; // lsid_ % devPb_ = offsetPb.

public:
    void open(const std::string& bdevPath, size_t /* queueSize */) {
        file_.open(bdevPath, O_RDONLY | O_DIRECT);
        pbs_ = cybozu::util::getPhysicalBlockSize(file_.fd());
        devPb_ = cybozu::util::getBlockDeviceSize(file_.fd()) / pbs_;
        cybozu::util::flushBufferCache(file_.fd());
        // file_.lseek(0);
        lsid_ = 0;
    }
    /**
     * You must call this before read().
     */
    void reset(uint64_t lsid) {
        lsid_ = lsid;
        file_.lseek(lsid % devPb_ * pbs_);
    }
    void read(void *data, size_t size) {
        assert(size % pbs_ == 0);
        uint64_t pb = size / pbs_;
        assert(pb <= devPb_ - lsid_ % devPb_);
        file_.read(data, size);
        lsid_ += pb;
        if (lsid_ % devPb_ == 0) file_.lseek(0);
    }
    uint32_t pbs() const {
        return pbs_;
    }
};

uint64_t exprId_;

/**
 * Record for the first 64 bytes in each physical block.
 */
struct PbRecord
{
    char data[64];

    void clear() {
        ::memset(&data[0], 0, 64);
    }
    uint64_t getLsid() const {
        try {
            return cybozu::atoi(&data[0], 32);
        } catch (...) {
            return uint64_t(-1);
        }
    }
    void setLsid(uint64_t lsid) {
        ::snprintf(&data[0], 32, "%" PRIu64 "", lsid);
    }
    uint64_t getExprId() const {
        uint64_t id;
        ::memcpy(&id, &data[32], sizeof(id));
        return id;
    }
    void setExprId(uint64_t id) {
        ::memcpy(&data[32], &id, sizeof(id));
    }
    template <typename Rand>
    void fillRand(Rand& rand) {
        rand.fill(&data[48], 16);
    }
};

template <typename Rand>
AlignedArray prepareData(uint32_t pbs, uint32_t pb, uint64_t lsid, Rand& rand, uint32_t& csum)
{
    AlignedArray buf(pb * pbs, true);
    for (size_t i = 0; i < pb; i++) {
        PbRecord *rec = reinterpret_cast<PbRecord *>(buf.data() + i * pbs);
        rec->clear();
        rec->setLsid(lsid + i);
        rec->setExprId(exprId_);
        rec->fillRand(rand);
    }
    csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
    return buf;
}

using Queue = cybozu::thread::BoundedQueue<IoRecord>;
std::atomic<bool> failed_(false);

template <typename Writer>
void doWrite(Writer& writer, uint64_t aheadPb, const std::atomic<uint64_t>& readPb, Queue& outQ)
try {
    const uint32_t pbs = writer.pbs();
    const uint32_t maxIoPb = 32 * KIBI / pbs;
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
        uint32_t csum;
        AlignedArray buf = prepareData(pbs, pb, lsid, rand, csum);
        const uint32_t aioKey = writer.prepare(std::move(buf));
        tmpQ.push(IoRecord{lsid, pb, csum, aioKey});
        if (tmpQ.size() >= 8 || rand() % 10 == 0) {
            writer.submit();
            while (!tmpQ.empty()) {
                const IoRecord& rec = tmpQ.front();
                LOGs.debug() << "write" << rec;
                outQ.push(rec);
                tmpQ.pop();
            }
            if (rand() % 1000 == 0) writer.sync();
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

template <typename Writer, typename Reader>
void doVerify(Writer& writer, Reader& reader, std::atomic<uint64_t>& readPb, size_t sleepMs, Queue& inQ)
try {
    const uint32_t pbs = reader.pbs();
    AlignedArray buf;
    IoRecord ioRec;
    reader.reset(0);
    while (inQ.pop(ioRec)) {
        if (failed_) return;
        buf.resize(ioRec.sizePb * pbs);
        writer.wait(ioRec.aioKey);
        reader.read(buf.data(), buf.size());
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
        for (size_t i = 0; i < ioRec.sizePb; i++) {
            const PbRecord *pbRec = reinterpret_cast<PbRecord *>(buf.data() + i * pbs);
            if (pbRec->getLsid() != ioRec.lsid + i || pbRec->getExprId() != exprId_) {
                LOGs.error() << "invalid record" << ioRec.lsid + i << pbRec->getLsid();
            }
        }
        LOGs.debug() << "read " << ioRec;
        if (ioRec.csum != csum) {
            LOGs.error() << "invalid csum" << ioRec << csum2str(csum);
        }
        readPb += ioRec.sizePb;
        if (sleepMs > 0) util::sleepMs(sleepMs);
    }
} catch (...) {
    inQ.fail();
    failed_ = true;
    throw;
}

void doMonitor(std::atomic<uint64_t>& readPb, size_t intervalS, uint64_t devPb)
try {
    LOGs.info() << "starting..." << intervalS;
    const double interval = double(intervalS);
    double t0 = cybozu::util::getTime();
    while (!cybozu::signal::gotSignal() && !failed_) {
        util::sleepMs(100);
        double t1 = cybozu::util::getTime();
        if (t1 - t0 > interval) {
            const uint64_t pb = readPb.load();
            LOGs.info() << "progress" << pb << pb / devPb;
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

template <typename Writer, typename Reader>
void writeAndVerify(const Option& opt)
{
    const uint32_t pbs = getPbs(opt.bdevPath);
    const size_t queueSize = 2 * MEBI / pbs;
    Queue queue(queueSize);
    Writer writer;
    writer.open(opt.bdevPath, queueSize);
    Reader reader;
    reader.open(opt.bdevPath, queueSize);
    std::atomic<uint64_t> readPb(0);
    const uint64_t aheadPb = opt.aheadSize / pbs;
    const uint64_t devPb = writer.devPb();
    LOGs.info() << "devPb" << devPb;
    LOGs.info() << "pbs" << pbs;

    cybozu::thread::ThreadRunnerSet thS;
    thS.add([&]() { doWrite<Writer>(writer, aheadPb, readPb, queue); });
    thS.add([&]() { doVerify<Writer, Reader>(writer, reader, readPb, opt.sleepMs, queue); });
    thS.add([&]() { doMonitor(readPb, opt.intervalS, devPb); } );
    thS.start();

    std::vector<std::exception_ptr> epV = thS.join();
    for (std::exception_ptr ep : epV) {
        if (ep) std::rethrow_exception(ep);
    }
}


/**
 * Record for each IO management for random workload.
 */
struct IoRecord2
{
    uint64_t lsid; // This is used for progress.
    uint64_t offPb;
    uint32_t sizePb;
    uint32_t csum;
    uint32_t aioKey;

    friend inline std::ostream& operator<<(std::ostream& os, const IoRecord2& rec) {
        os << rec.lsid << "\t" << rec.offPb << "\t" << rec.sizePb << "\t" << csum2str(rec.csum);
        return os;
    }
};

using Queue2 = cybozu::thread::BoundedQueue<IoRecord2>;

class NonOverlappedRanges
{
    /**
     * key: offPb
     * value: int (BEGIN or END or BOTH).
     */
    enum {
        BEGIN = 0, END = 1, BOTH = 2,
    };
    using Map = std::map<uint64_t, int>;

    using AutoLock = std::lock_guard<std::mutex>;
    mutable std::mutex mutex_;
    Map map_;
public:
    bool tryInsert(uint64_t bgn, uint64_t end) {
        assert(bgn < end);
        AutoLock lk(mutex_);
        if (existsNolock(bgn, end)) return false;
        insertElem(bgn, BEGIN);
        insertElem(end, END);
        return true;
    }
    void erase(uint64_t bgn, uint64_t end) {
        assert(bgn < end);
        AutoLock lk(mutex_);
        eraseElem(bgn, BEGIN);
        eraseElem(end, END);
    }
    /**
     * For test.
     */
    void verifyAll() const {
        AutoLock lk(mutex_);
        verifyAllNolock();
    }
private:
    void verifyAllNolock() const {
        int state = END;
        for (const auto pair : map_) {
            const int flag = pair.second;
            if (state == END) {
                if (flag != BEGIN) {
                    throw cybozu::Exception("flag must be BEGIN") << flag;
                }
                state = BEGIN;
            } else {
                assert(state == BEGIN);
                if (flag == BEGIN) {
                    throw cybozu::Exception("flag must not be BEGIN");
                }
                if (flag == END) {
                    state = END;
                } else {
                    assert(flag == BOTH);
                }
            }
        }
        if (state != END) {
            throw cybozu::Exception("flag must be END");
        }
    }
#if 0
    void printAllNolock() const {
        LOGs.info() << "<<<<<<<<<<<<<<<";
        for (const auto pair : map_) {
            const uint64_t pos = pair.first;
            const int flag = pair.second;
            LOGs.info() << pos << flag;
        }
        LOGs.info() << ">>>>>>>>>>>>>>>";
    }
#endif
    bool existsNolock(uint64_t bgn, uint64_t end) const {
        return !cond1(bgn) || !cond2(bgn, end);
    }
    bool cond1(uint64_t bgn) const {
        auto it = map_.find(bgn);
        if (it == map_.cend()) return true;
        return it->second == END;
    }
    bool cond2(uint64_t bgn, uint64_t end) const {
        auto it = map_.upper_bound(bgn);
        if (it == map_.cend()) return true;
        if (end > it->first) return false;
        return it->second == BEGIN;
    }
    void insertElem(uint64_t pos, int flag) {
        assert(flag == BEGIN || flag == END);
        auto it = map_.find(pos);
        if (it == map_.end()) {
            Map::iterator it;
            bool ret;
            std::tie(it, ret) = map_.emplace(pos, flag);
            assert(ret);
        } else {
            assert(it->second != BOTH);
            assert(it->second != flag);
            it->second = BOTH;
        }
    }
    void eraseElem(uint64_t pos, int flag) {
        assert(flag == BEGIN || flag == END);
        auto it = map_.find(pos);
        assert(it != map_.end());
        if (it->second == BOTH) {
            if (flag == BEGIN) {
                it->second = END;
            } else {
                it->second = BEGIN;
            }
        } else {
            assert(it->second == flag);
            map_.erase(it);
        }
    }
};

void doRandomWrite(
    Aio2& aio, uint32_t pbs, uint64_t devPb, uint64_t aheadPb,
    const std::atomic<uint64_t>& readPb, Queue2& outQ, NonOverlappedRanges& ranges)
try {
    const uint32_t maxIoPb = 32 * KIBI / pbs;
    uint64_t lsid = 0;
    cybozu::util::Random<uint64_t> rand;
    std::queue<IoRecord2> tmpQ;
    uint64_t writtenPb = 0;

    auto submitAll = [&tmpQ, &outQ, &aio]() {
        aio.submit();
        while (!tmpQ.empty()) {
            const IoRecord2& rec = tmpQ.front();
            LOGs.debug() << "write" << rec;
            outQ.push(rec);
            tmpQ.pop();
        }
    };

    while (!cybozu::signal::gotSignal()) {
        if (failed_) return;
        if (readPb + aheadPb < writtenPb) {
            util::sleepMs(1); // backpressure.
            continue;
        }
        const uint64_t offPb = rand() % devPb;
        const uint32_t pb = std::min<uint64_t>(devPb - offPb, 1 + rand() % maxIoPb);
        if (!ranges.tryInsert(offPb, offPb + pb)) continue;
        uint32_t csum;
        AlignedArray buf = prepareData(pbs, pb, lsid, rand, csum);
        const uint32_t aioKey = aio.prepareWrite(offPb * pbs, std::move(buf));
        tmpQ.push(IoRecord2{lsid, offPb, pb, csum, aioKey});
        if (tmpQ.size() >= 8 || rand() % 10 == 0) {
            submitAll();
        }
        lsid += pb;
        writtenPb += pb;
    }
    if (!tmpQ.empty()) {
        submitAll();
    }
    outQ.sync();
} catch (...) {
    outQ.fail();
    failed_ = true;
    throw;
}

void doRandomVerify(
    Aio2& aio, cybozu::util::File& fileR, uint32_t pbs, std::atomic<uint64_t>& readPb,
    size_t sleepMs, Queue2& inQ, NonOverlappedRanges& ranges)
try {
    AlignedArray buf;
    IoRecord2 ioRec;
    while (inQ.pop(ioRec)) {
        if (failed_) return;
        buf.resize(ioRec.sizePb * pbs);
        aio.waitFor(ioRec.aioKey);
        fileR.pread(buf.data(), buf.size(), ioRec.offPb * pbs);
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
        for (size_t i = 0; i < ioRec.sizePb; i++) {
            const PbRecord *pbRec = reinterpret_cast<PbRecord *>(buf.data() + i * pbs);
            if (pbRec->getLsid() != ioRec.lsid + i || pbRec->getExprId() != exprId_) {
                LOGs.error() << "invalid record" << ioRec.lsid + i << pbRec->getLsid();
            }
        }
        LOGs.debug() << "read " << ioRec;
        if (ioRec.csum != csum) {
            LOGs.error() << "invalid csum" << ioRec << csum2str(csum);
        }
        readPb += ioRec.sizePb;
        ranges.erase(ioRec.offPb, ioRec.offPb + ioRec.sizePb);
        if (sleepMs > 0) util::sleepMs(sleepMs);
    }
} catch (...) {
    inQ.fail();
    failed_ = true;
    throw;
}

template <typename Writer>
void randomWriteAndVerify(const Option& opt)
{
    const uint32_t pbs = getPbs(opt.bdevPath);
    const size_t queueSize = 2 * MEBI / pbs;
    Queue2 queue(queueSize);

    cybozu::util::File fileW(opt.bdevPath, O_RDWR | O_DIRECT);
    Aio2 aio;
    aio.init(fileW.fd(), queueSize * 2);
    cybozu::util::File fileR(opt.bdevPath, O_RDONLY | O_DIRECT);

    std::atomic<uint64_t> readPb(0);
    const uint64_t aheadPb = opt.aheadSize / pbs;
    const uint64_t devPb = cybozu::util::getBlockDeviceSize(fileR.fd()) / pbs;
    LOGs.info() << "devPb" << devPb;
    LOGs.info() << "pbs" << pbs;
    NonOverlappedRanges ranges;

    // TODO: support concurrent execution with opt.nrThreads > 1.
    if (opt.nrThreads != 1) {
        throw cybozu::Exception("multi-threads is not supported now");
    }

    cybozu::thread::ThreadRunnerSet thS;
    thS.add([&]() { doRandomWrite(aio, pbs, devPb, aheadPb, readPb, queue, ranges); });
    thS.add([&]() { doRandomVerify(aio, fileR, pbs, readPb, opt.sleepMs, queue, ranges); });
    thS.add([&]() { doMonitor(readPb, opt.intervalS, devPb); } );
    thS.start();

    std::vector<std::exception_ptr> epV = thS.join();
    for (std::exception_ptr ep : epV) {
        if (ep) std::rethrow_exception(ep);
    }
}

void testNonOverlappedRanges()
{
    util::setLogSetting("-", false);
    NonOverlappedRanges ranges;

    LOGs.info() << "start";

    const size_t devPb = 100;
    const size_t maxPb = 10;
    using Queue3 = cybozu::thread::BoundedQueue<std::pair<size_t, size_t> >;

    std::atomic<bool> running(true);

    size_t nrThreads = 4;
    std::vector<Queue3> queV(nrThreads);
    for (Queue3& q : queV) q.resize(100);

    auto work0 = [&](Queue3& que) {
        try {
            cybozu::util::Random<uint64_t> rand;
            for (size_t i = 0; i < 100000; i++) {
                size_t offPb = rand() % devPb;
                size_t pb = std::min(devPb - offPb, 1 + rand() % maxPb);
                if (!ranges.tryInsert(offPb, offPb + pb)) {
                    util::sleepMs(1);
                    continue;
                }
                que.push(std::make_pair(offPb, pb));
            }
            que.sync();
            running = false;
        } catch (...) {
            que.fail();
            running = false;
            throw;
        }
    };

    auto work1 = [&](Queue3& que) {
        try {
            std::pair<size_t, size_t> pair;
            while (que.pop(pair)) {
                const size_t offPb = pair.first;
                const size_t pb = pair.second;
                ranges.erase(offPb, offPb + pb);
            }
            running = false;
        } catch (...) {
            que.fail();
            running = false;
            throw;
        }
    };

    auto work2 = [&]() {
        size_t n = 0;
        while (running) {
            ranges.verifyAll();
            n++;
            util::sleepMs(100);
        }
        LOGs.info() << "verify times" << n;
    };

    cybozu::thread::ThreadRunnerSet thS;
    for (size_t i = 0; i < nrThreads; i++) {
        thS.add([&,i]() { work0(queV[i]); });
        thS.add([&,i]() { work1(queV[i]); });
    }
    thS.add(work2);
    thS.start();

    std::vector<std::exception_ptr> epV = thS.join();
    for (std::exception_ptr ep : epV) {
        if (ep) std::rethrow_exception(ep);
    }
}

int doMain(int argc, char* argv[])
{
#if 1
    Option opt(argc, argv);
    util::setLogSetting(opt.logPath, opt.isDebug);
    cybozu::signal::setSignalHandler({SIGINT, SIGQUIT, SIGTERM});
    exprId_ = ::time(0);

    if (opt.isRandom) {
        if (opt.dontUseAio) {
            randomWriteAndVerify<SyncWriter>(opt);
        } else {
            randomWriteAndVerify<AsyncWriter>(opt);
        }
    } else {
        if (opt.dontUseAio) {
            writeAndVerify<SyncWriter, SyncReader>(opt);
        } else {
            writeAndVerify<AsyncWriter, SyncReader>(opt);
        }
    }
#else
    testNonOverlappedRanges();
#endif
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("writer-verifier")
