/**
 * @file
 * @brief Writer writes data and read/verifier reads and verify written data.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio_util.hpp"
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
 * Thread-safe aio manager.
 */
class Aio2
{
    struct AioData {
        uint key;
        int type;
        struct iocb iocb;
        off_t oft;
        size_t size;
        AlignedArray buf;
        int err;

        void init(uint key, int type, off_t oft, AlignedArray&& buf) {
            this->key = key;
            this->type = type;
            ::memset(&iocb, 0, sizeof(iocb));
            this->oft = oft;
            this->size = buf.size();
            this->buf = std::move(buf);
            err = 0;
        }
    };

    int fd_;
    size_t queueSize_;
    io_context_t ctx_;

    using AioDataPtr = std::unique_ptr<AioData>;
    using Umap = std::unordered_map<uint, AioDataPtr>;

    mutable std::mutex mutex_;
    std::vector<AioDataPtr> submitQ_;
    Umap pendingIOs_;
    Umap completedIOs_;

    std::atomic_flag isInitialized_;
    std::atomic_flag isReleased_;
    std::atomic<uint> key_;
    std::atomic<size_t> nrIOs_;

    using AutoLock = std::lock_guard<std::mutex>;

public:
    Aio2()
        : fd_(0)
        , queueSize_(0)
        , mutex_()
        , submitQ_()
        , pendingIOs_()
        , completedIOs_()
        , isInitialized_(ATOMIC_FLAG_INIT)
        , isReleased_(ATOMIC_FLAG_INIT)
        , key_(0)
        , nrIOs_(0) {
    }
    /**
     * You must call this in the thread which will run the destructor.
     */
    void init(int fd, size_t queueSize) {
        if (isInitialized_.test_and_set()) {
            throw cybozu::Exception("Aio: do not call init() more than once");
        }
        assert(fd > 0);
        fd_ = fd;
        queueSize_ = queueSize;
        int err = ::io_queue_init(queueSize_, &ctx_);
        if (err < 0) {
            throw cybozu::Exception("Aio2 init failed") << cybozu::ErrorNo(-err);
        }
    }
    ~Aio2() noexcept try {
        if (isInitialized_.test_and_set()) {
            waitAll();
            release();
        }
    } catch (...) {
    }
    uint prepareRead(off_t oft, size_t size) {
        if (++nrIOs_ > queueSize_) {
            --nrIOs_;
            throw cybozu::Exception("prepareRead: queue is full");
        }
        const uint key = key_++;
        AioDataPtr iop(new AioData());
        iop->init(key, 0, oft, AlignedArray(size));
        ::io_prep_pread(&iop->iocb, fd_, iop->buf.data(), size, oft);
        iop->iocb.data = reinterpret_cast<void *>(key);
        pushToSubmitQ(std::move(iop));
        return key;
    }
    uint prepareWrite(off_t oft, AlignedArray&& buf) {
        if (++nrIOs_ > queueSize_) {
            --nrIOs_;
            throw cybozu::Exception("prepareWrite: queue is full");
        }
        const uint key = key_++;
        AioDataPtr iop(new AioData());
        const size_t size = buf.size();
        iop->init(key, 1, oft, std::move(buf));
        ::io_prep_pwrite(&iop->iocb, fd_, iop->buf.data(), size, oft);
        iop->iocb.data = reinterpret_cast<void *>(key);
        pushToSubmitQ(std::move(iop));
        return key;
    }
    void submit() {
        std::vector<AioDataPtr> submitQ;
        {
            AutoLock lk(mutex_);
            submitQ = std::move(submitQ_);
            submitQ_.clear();
        }
        const size_t nr = submitQ.size();
        std::vector<struct iocb *> iocbs(nr);
        for (size_t i = 0; i < nr; i++) {
            iocbs[i] = &submitQ[i]->iocb;
        }
        {
            AutoLock lk(mutex_);
            for (size_t i = 0; i < nr; i++) {
                AioDataPtr iop = std::move(submitQ[i]);
                const uint key = iop->key;
                pendingIOs_.emplace(key, std::move(iop));
            }
        }
        size_t done = 0;
        while (done < nr) {
            int err = ::io_submit(ctx_, nr - done, &iocbs[done]);
            if (err < 0) {
                throw cybozu::Exception("Aio submit failed") << cybozu::ErrorNo(-err);
            }
            done += err;
        }
    }
    AlignedArray waitFor(uint key) {
        {
            AutoLock lk(mutex_);
            if (completedIOs_.find(key) == completedIOs_.cend() &&
                pendingIOs_.find(key) == pendingIOs_.cend()) {
                throw cybozu::Exception("waitFor: key not found") << key;
            }
        }
        AioDataPtr iop;
        while (!popCompleted(key, iop)) {
            waitDetail();
        }
        verifyNoError(*iop);
        AlignedArray buf = iop->buf;
        --nrIOs_;
        return buf;
    }
private:
    void release() {
        if (isReleased_.test_and_set()) return;
        int err = ::io_queue_release(ctx_);
        if (err < 0) {
            throw cybozu::Exception("Aio: release failed") << cybozu::ErrorNo(-err);
        }
    }
    void pushToSubmitQ(AioDataPtr&& iop) {
        AutoLock lk(mutex_);
        submitQ_.push_back(std::move(iop));
    }
    bool popCompleted(uint key, AioDataPtr& iop) {
        AutoLock lk(mutex_);
        Umap::iterator it = completedIOs_.find(key);
        if (it == completedIOs_.end()) return false;
        iop = std::move(it->second);
        assert(iop->key == key);
        completedIOs_.erase(it);
        return true;
    }
    size_t waitDetail(size_t minNr = 1) {
        size_t maxNr = nrIOs_;
        if (maxNr < minNr) maxNr = minNr;
        std::vector<struct io_event> ioEvents(maxNr);
        int nr = ::io_getevents(ctx_, minNr, maxNr, &ioEvents[0], NULL);
        if (nr < 0) {
            throw cybozu::Exception("io_getevents failed") << cybozu::ErrorNo(-nr);
        }
        AutoLock lk(mutex_);
        for (int i = 0; i < nr; i++) {
            const uint key = getKeyFromEvent(ioEvents[i]);
            Umap::iterator it = pendingIOs_.find(key);
            assert(it != pendingIOs_.end());
            AioDataPtr& iop = it->second;
            assert(iop->key == key);
            iop->err = ioEvents[i].res;
            completedIOs_.emplace(key, std::move(iop));
            pendingIOs_.erase(it);
        }
        return nr;
    }
    static uint getKeyFromEvent(struct io_event &event) {
        struct iocb &iocb = *static_cast<struct iocb *>(event.obj);
        return static_cast<uint>(reinterpret_cast<uintptr_t>(iocb.data));
    }
    void verifyNoError(const AioData& io) const {
        if (io.err == 0) {
            throw cybozu::util::EofError();
        }
        if (io.err < 0) {
            throw cybozu::Exception("Aio: IO failed") << io.key << cybozu::ErrorNo(-io.err);
        }
        assert(io.iocb.u.c.nbytes == static_cast<uint>(io.err));
    }
    void waitAll() {
        for (;;) {
            size_t size;
            {
                AutoLock lk(mutex_);
                size = pendingIOs_.size();
            }
            if (size == 0) break;
            try {
                waitDetail();
            } catch (...) {
                break;
            }
        }
    }
};

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
    uint key_;

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
    uint prepare(AlignedArray&& buf) {
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
    std::list<AlignedArray> queue_;
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
        assert(queue_.empty());
        aheadLsid_ = lsid;
    }
    uint64_t tailPb() const {
        const uint64_t offsetPb = aheadLsid_ % devPb_;
        return devPb_ - offsetPb;
    }
    uint prepare(AlignedArray&& buf) {
        assert(buf.size() % pbs_ == 0);
        const uint64_t givenPb = buf.size() / pbs_;
        assert(givenPb <= tailPb());
        const uint64_t offset = aheadLsid_ % devPb_ * pbs_;
        const uint aioKey = aio_.prepareWrite(offset, std::move(buf));
        aheadLsid_ += givenPb;
        return aioKey;
    }
    void submit() {
        aio_.submit();
    }
    /**
     * This will be called from another thread.
     */
    void wait(uint aioKey) {
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
        assert(size % pbs_);
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

/**
 * Record for the first 64 bytes in each physical block.
 */
struct PbRecord
{
    char data[64];

    static uint64_t exprId;


    void clear() {
        ::memset(&data[0], 0, 64);
    }
    uint64_t getLsid() const {
        return cybozu::atoi(&data[0], 32);
    }
    void setLsid(uint64_t lsid) {
        ::snprintf(&data[0], 32, "%" PRIu64 "", lsid);
    }
    template <typename Rand>
    void fillRand(Rand& rand) {
        rand.fill(&data[32], 32);
    }
};

using Queue = cybozu::thread::BoundedQueue<IoRecord>;

template <typename Writer>
void doWrite(Writer& writer, uint64_t aheadPb, const std::atomic<uint64_t>& readPb, Queue& outQ)
try {
    const uint32_t pbs = writer.pbs();
    const size_t maxIoPb = 32 * KIBI / pbs;
    uint64_t lsid = 0;
    writer.reset(lsid % writer.devPb());
    cybozu::util::Random<uint64_t> rand;
    AlignedArray buf;
    uint64_t writtenPb = 0;
    std::queue<IoRecord> tmpQ;

    while (!cybozu::signal::gotSignal()) {
        if (readPb + aheadPb < writtenPb) {
            util::sleepMs(1); // backpressure.
            continue;
        }
        const uint64_t pb = std::min(writer.tailPb(), 1 + rand() % maxIoPb);
        buf.resize(pb * pbs, true);
        for (size_t i = 0; i < pb; i++) {
            PbRecord *rec = (PbRecord *)(buf.data() + i * pbs);
            rec->clear();
            rec->setLsid(lsid + i);
            rec->fillRand(rand);
        }
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
        const uint32_t aioKey = writer.prepare(std::move(buf));
        tmpQ.push(IoRecord{lsid, uint32_t(pb), csum, aioKey});
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
        buf.clear();
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
        buf.resize(ioRec.sizePb * pbs);
        writer.wait(ioRec.aioKey);
        reader.read(buf.data(), buf.size());
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
        for (size_t i = 0; i < ioRec.sizePb; i++) {
            const PbRecord *pbRec = (PbRecord *)(buf.data() + i * pbs);
            if (pbRec->getLsid() != ioRec.lsid + i) {
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
    throw;
}

void doMonitor(std::atomic<uint64_t>& readPb, size_t intervalS, uint64_t devPb)
{
    LOGs.info() << "starting..." << intervalS;
    const double interval = double(intervalS);
    double t0 = cybozu::util::getTime();
    while (!cybozu::signal::gotSignal()) {
        util::sleepMs(100);
        double t1 = cybozu::util::getTime();
        if (t1 - t0 > interval) {
            const uint64_t pb = readPb.load();
            LOGs.info() << "progress" << pb << pb / devPb;
            t0 = t1;
        }
    }
    LOGs.info() << "terminate...";
}

template <typename Writer, typename Reader>
void writeAndVerify(const Option& opt)
{
    uint32_t pbs;
    {
        cybozu::util::File file(opt.bdevPath, O_RDONLY);
        pbs = cybozu::util::getPhysicalBlockSize(file.fd());
    }
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

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting(opt.logPath, opt.isDebug);
    cybozu::signal::setSignalHandler({SIGINT, SIGQUIT, SIGTERM});
    if (opt.dontUseAio) {
        writeAndVerify<SyncWriter, SyncReader>(opt);
    } else {
        writeAndVerify<AsyncWriter, SyncReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("writer-verifier")
