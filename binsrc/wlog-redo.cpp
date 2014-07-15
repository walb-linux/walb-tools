/**
 * @file
 * @brief Redo walb log.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "bdev_util.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

//#define USE_DEBUG_TRACE

using namespace walb;

struct Option
{
    std::string ddevPath;
    std::string inWlogPath;
    bool isDiscard;
    bool isZeroDiscard;
    bool dontUseAio;
    bool isVerbose;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wlog-redo: redo wlog on a block device.");
        opt.appendOpt(&inWlogPath, "-", "i", "PATH: input wlog path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isDiscard, "d", "issue discard for discard logs.");
        opt.appendBoolOpt(&isZeroDiscard, "z", "zero-clear for discard logs.");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendParam(&ddevPath, "DEVICE_PATH");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (isDiscard && isZeroDiscard) {
            throw RT_ERR("Do not specify both -d and -z together.");
        }
    }
    bool isFromStdin() const { return inWlogPath == "-"; }
};

struct Statistics
{
    size_t normalLb;      // amount of normal-issued IOs (not including zero-discard).
    size_t discardLb;     // amount of discarded IOs (including zero-discard).
    size_t writtenLb;     // amount of written (issued, including zero-discard) IOs.
    size_t overwrittenLb; // amount of overwritten (not issued) IOs.
    size_t clippedLb;     // amount of clipped (not issued) IOs.
    size_t paddingPb;     // amount of padding [physical block].

    Statistics()
        : normalLb(0)
        , discardLb(0)
        , overwrittenLb(0)
        , clippedLb(0)
        , paddingPb(0) {
    }
    void print() const {
        ::printf("normalLb:      %10zu\n"
                 "discardLb:     %10zu\n"
                 "writtenLb:     %10zu\n"
                 "overwrittenLb: %10zu\n"
                 "clippedLb:     %10zu\n"
                 "paddingPb:     %10zu\n"
                 , normalLb, discardLb
                 , writtenLb, overwrittenLb
                 , clippedLb, paddingPb);
    }
} g_st;

/**
 * Io data.
 */
class Io
{
private:
    uint64_t offset_; // [bytes].
    size_t size_; // [bytes].
    std::deque<AlignedArray> blocks_;

public:
    uint32_t aioKey; // IO identifier inside aio.
    uint32_t nOverlapped; // To serialize overlapped IOs. 0 means ready to submit.

    enum State {
        Init, Overwritten, Submitted
    } state;
    explicit Io(uint64_t offset, size_t size = 0)
        : offset_(offset), size_(size)
        , blocks_()
        , aioKey(0)
        , nOverlapped(-1)
        , state(Init) {}
    Io(uint64_t offset, size_t size, AlignedArray &&block)
        : Io(offset, size) {
        setBlock(std::move(block));
    }

    uint64_t offset() const { return offset_; }
    size_t size() const { return size_; }
    const std::deque<AlignedArray>& blocks() const { return blocks_; }
    const char *data() const { return blocks_.front().data(); }
    bool empty() const { return blocks().empty(); }

    void setBlock(AlignedArray &&b) {
        assert(blocks_.empty());
        blocks_.push_back(std::move(b));
    }
    void print(::FILE *p = ::stdout) const {
        ::fprintf(p, "IO offset: %zu size: %zu aioKey: %u "
                  "state: %d\n",
                  offset_, size_, aioKey, state);
        for (auto &b : blocks_) {
            ::fprintf(p, "  block %p\n", b.data());
        }
    }

    /**
     * Can an IO be merged to this.
     */
    bool canMerge(const Io& rhs) const {
        /* They must have data buffers. */
        if (blocks_.empty() || rhs.blocks_.empty()) {
            return false;
        }

        /* Check Io targets and buffers are adjacent. */
        if (offset_ + size_ != rhs.offset_) {
            //::fprintf(::stderr, "offset mismatch\n"); //debug
            return false;
        }

        /* Check buffers are contiguous. */
        const char *p0 = blocks_.front().data();
        const char *p1 = rhs.blocks_.front().data();
        return p0 + size_ == p1;
    }

    /**
     * Try merge an IO.
     *
     * RETURN:
     *   true if merged, or false.
     */
    bool tryMerge(Io& rhs) {
        if (!canMerge(rhs)) {
            return false;
        }
        size_ += rhs.size_;
        while (!rhs.empty()) {
            blocks_.push_back(std::move(rhs.blocks_.front()));
            rhs.blocks_.pop_front();
        }
        return true;
    }

    /**
     * RETURN:
     *   true if overlapped.
     */
    bool isOverlapped(const Io& rhs) const {
        const size_t off0 = offset_;
        const size_t off1 = rhs.offset_;
        const size_t size0 = size_;
        const size_t size1 = rhs.size_;
        return off0 < off1 + size1 && off1 < off0 + size0;
    }

    /**
     * RETURN:
     *   true if the IO is fully overwritten by rhs.
     */
    bool isOverwrittenBy(const Io& rhs) const {
        const size_t off0 = offset_;
        const size_t off1 = rhs.offset_;
        const size_t size0 = size_;
        const size_t size1 = rhs.size_;
        return off1 <= off0 && off0 + size0 <= off1 + size1;
    }
};

#ifdef USE_DEBUG_TRACE
struct StrVec : std::vector<std::string> {
    void put() const {
        for (const std::string& s : *this) {
            printf("%s -> ", s.c_str());
        }
        printf("\n");
    }
};
std::ostream& operator<<(std::ostream& os, const StrVec& v) {
    for (const std::string& s : v) {
        os << s << " -> ";
    }
    os << std::endl;
    return os;
}
#endif
struct Debug {
#ifdef USE_DEBUG_TRACE
    typedef std::set<const Io*> IoSet;
    typedef std::map<const Io*, StrVec> IoMap;
    IoSet ioQ;
    IoSet readyQ;
    IoMap ioMap;
    void addIoQ(const Io& io)
    {
        ioMap[&io].push_back(__func__);
        verifyAdd(ioQ, io, __func__);
        verifyNotExist(readyQ, io, __func__);
    }
    void addReadyQ(const Io& io)
    {
        verifyNumOverlapped(io, __func__);
        ioMap[&io].push_back(__func__);
        verifyExist(ioQ, io, __func__);
        verifyAdd(readyQ, io, __func__);
    }
    void delIoQ(const Io& io)
    {
        verifyNumOverlapped(io, __func__);
        ioMap[&io].push_back(__func__);
        verifyDel(ioQ, io, __func__);
        verifyNotExist(readyQ, io, "dellIoQ 1");
    }
    void delReadyQ(const Io& io)
    {
        verifyNumOverlapped(io, __func__);
        ioMap[&io].push_back(__func__);
        verifyExist(ioQ, io, __func__);
        verifyDel(readyQ, io, __func__);
    }
    void verifyAdd(IoSet& ioSet, const Io& io, const char *msg)
    {
        if (!ioSet.insert(&io).second) {
            printf("ERR verifyAdd %s %p\n", msg, &io);
            throw cybozu::Exception(__func__) << ioMap[&io];
        }
    }
    void verifyDel(IoSet& ioSet, const Io& io, const char *msg)
    {
        if (ioSet.erase(&io) != 1) {
            printf("ERR verifyDel %s %p\n", msg, &io);
            throw cybozu::Exception(__func__) << ioMap[&io];
        }
    }
    void verifyNotExist(const IoSet& ioSet, const Io& io, const char *msg)
    {
        if (ioSet.find(&io) != ioSet.end()) {
            printf("ERR verifyNotExist %s\n", msg);
            throw cybozu::Exception(__func__) << ioMap[&io];
        }
    }
    void verifyExist(const IoSet& ioSet, const Io& io, const char *msg)
    {
        if (ioSet.find(&io) == ioSet.end()) {
            printf("ERR verifyExist %s\n", msg);
            throw cybozu::Exception(__func__) << ioMap[&io];
        }
    }
    void verifyNumOverlapped(const Io& io, const char *msg) {
        if (io.nOverlapped != 0) {
            throw cybozu::Exception(msg) << "nOverlapped" << io.nOverlapped;
        }
    }

#else
    void addIoQ(const Io&) { }
    void addReadyQ(const Io&) { }
    void delIoQ(const Io&) { }
    void delReadyQ(const Io&) { }
#endif
} g_debug;

/**
 * Convert [byte] to [physical block].
 */
inline uint32_t bytesToPb(uint32_t bytes, uint32_t pbs) {
    assert(bytes % LOGICAL_BLOCK_SIZE == 0);
    const uint32_t lb = bytes / LOGICAL_BLOCK_SIZE;
    return ::capacity_pb(pbs, lb);
}

/**
 *  Io state
 *  (file-sink) -> fetched -> pending -> ready -> submitted -> (completed)
 *                            <--       processing      -->
 *
 * the contents of list_
 * begin                   ---   fetchedBegin_    --- end
 * <--- submitted + ready + pending  ---><--- fetched --->
 *           processing
 *           processingPb                    fetchedPb (pb unit)
 */
class IoQueue
{
    const size_t maxPb_; // the max size of each state
    const uint32_t pbs_;
    size_t processingPb_; // total pb of pending/ready/submitted
    size_t fetchedPb_;
    typedef std::list<Io> List;
    List list_;
    List::iterator fetchedBegin_;
    static const size_t maxIoSize_ = 1024 * 1024; //1MB.
    /**
     * Try to merge src to dst.
     */
    bool tryMerge(Io& dst, Io& src) {
        if (maxIoSize_ < dst.size() + src.size()) {
            return false;
        }
        return dst.tryMerge(src);
    }

public:
    explicit IoQueue(size_t maxPb_, uint32_t pbs)
        : maxPb_(maxPb_)
        , pbs_(pbs)
        , processingPb_(0)
        , fetchedPb_(0)
        , fetchedBegin_(list_.end())
    {
    }
    void add(Io &&io) {
        assert(io.size() == pbs_);
        fetchedPb_++;
        if (hasFetched() && tryMerge(*fetchedBegin_, io)) {
            return;
        }
        list_.push_back(std::move(io));
        if (fetchedBegin_ == list_.end()) {
            --fetchedBegin_;
        }
    }
    bool hasFetched() const { return fetchedBegin_ != list_.end(); }
    bool hasProcessing() const { return list_.begin() != fetchedBegin_; }
    bool isFull() const { return processingPb_ > maxPb_; }
    bool shouldProcess() const { return fetchedPb_ > maxPb_; }

    Io& nextFetched() {
        Io &io = *fetchedBegin_++;
        const size_t pb = bytesToPb(io.size(), pbs_);
        processingPb_ += pb;
        fetchedPb_ -= pb;
        g_debug.addIoQ(io);
        return io;
    }

    void waitForAllSubmitted(cybozu::aio::Aio &aio) noexcept {
        for (List::iterator it = list_.begin(); it != fetchedBegin_; ++it) {
            Io& io = *it;
            if (io.state == Io::Submitted) {
                try {
                    aio.waitFor(io.aioKey);
                } catch (...) {}
            }
        }
    }
    Io& getFront() {
        assert(hasProcessing());
        return list_.front();
    }
    void popFront() {
        Io& io = list_.front();
        g_debug.delIoQ(io);
        processingPb_ -= bytesToPb(io.size(), pbs_);
        list_.pop_front();
    }
};

struct IoSetLess
{
    bool operator()(const Io *a, const Io *b) const {
        return a->offset() < b->offset();
    }
};

using IoSet = std::multiset<Io*, IoSetLess>;

/**
 * IO queue to store IOs are ready to submit.
 * The IOs are sorted by offset for good performance.
 */
class ReadyQueue
{
private:
    IoSet ioSet_;

public:
    void push(Io *iop) {
        ioSet_.insert(iop);
        g_debug.addReadyQ(*iop);
    }
    size_t size() const {
        return ioSet_.size();
    }
    void submit(cybozu::aio::Aio &aio) {
        size_t nBulk = 0;
        while (!ioSet_.empty()) {
            Io* iop = *ioSet_.begin();
            ioSet_.erase(ioSet_.begin());
            g_debug.delReadyQ(*iop);

            assert(iop->state != Io::Submitted);
            if (iop->state == Io::Overwritten) continue;
            iop->state = Io::Submitted;

            /* Prepare aio. */
            assert(iop->nOverlapped == 0);
            iop->aioKey = aio.prepareWrite(
                iop->offset(), iop->size(), iop->data());
            assert(iop->aioKey > 0);
            nBulk++;
        }
        if (nBulk > 0) {
            aio.submit();
        }
    }
    void forceComplete(Io &io, cybozu::aio::Aio &aio) {
        if (io.state == Io::Init) {
            /* The IO is not still submitted. */
            assert(io.nOverlapped == 0);
            submit(aio);
        } else if (io.state == Io::Overwritten) {
            tryErase(io);
        }
        if (io.state == Io::Submitted) {
            assert(io.aioKey > 0);
            aio.waitFor(io.aioKey);
            g_st.writtenLb += io.size() >> 9;
        } else {
            assert(io.state == Io::Overwritten);
            g_st.overwrittenLb += io.size() >> 9;
        }
    }
    bool empty() const {
        return ioSet_.empty();
    }
private:
    void tryErase(Io& io) {
        IoSet::iterator i, e;
        std::tie(i, e) = ioSet_.equal_range(&io);
        while (i != e) {
            if (*i == &io) {
                ioSet_.erase(i);
                g_debug.delReadyQ(io);
                return;
            }
            ++i;
        }
    }
};

/**
 * In order to serialize overlapped IOs execution.
 * IOs must be FIFO. (add() and del()).
 */
class OverlappedSerializer
{
private:
    IoSet set_;
    size_t maxSize_;
public:
    OverlappedSerializer()
        : set_(), maxSize_(0) {}
    /**
     * Insert to the overlapped data.
     *
     * (1) count overlapped IOs.
     * (2) set iop->nOverlapped to the number of overlapped IOs.
     */
    void add(Io& io) {
        io.nOverlapped = 0;
        forEachOverlapped(io, [&](Io &ioX) {
                io.nOverlapped++;
                if (ioX.isOverwrittenBy(io) && ioX.state == Io::Init) {
                    ioX.state = Io::Overwritten;
                }
            });

        set_.insert(&io);
        if (maxSize_ < io.size()) maxSize_ = io.size();
    }
    /**
     * Delete from the overlapped data.
     *
     * (1) Delete from the overlapping data.
     * (2) Decrement the overlapping IOs in the data.
     * (3) IOs where iop->nOverlapped became 0 will be added to the ioQ.
     *     You can submit them just after returned.
     */
    void delIoAndPushReadyIos(Io& io, ReadyQueue& readyQ) {
        assert(io.nOverlapped == 0);
        erase(io);
        if (set_.empty()) maxSize_ = 0;

        forEachOverlapped(io, [&](Io &ioX) {
                ioX.nOverlapped--;
                if (ioX.nOverlapped == 0 && ioX.state == Io::Init) {
                    readyQ.push(&ioX);
                }
            });
    }
    bool empty() const { return set_.empty(); }
private:
    template <typename Func>
    void forEachOverlapped(const Io &io, Func func) {
        uint64_t key0 = 0;
        if (maxSize_ < io.offset()) {
            key0 = io.offset() - maxSize_;
        }
        const uint64_t key1 = io.offset() + io.size();
        Io io0(key0);
        IoSet::iterator it = set_.lower_bound(&io0);
        while (it != set_.end() && (*it)->offset() < key1) {
            Io* iop = *it;
            if (io.isOverlapped(*iop)) {
                func(*iop);
            }
            ++it;
        }
    }
    void erase(Io &io) {
        IoSet::iterator i, e;
        std::tie(i, e) = set_.equal_range(&io);
        while (i != e) {
            if (*i == &io) {
                set_.erase(i);
                return;
            }
        }
        assert(false);
    }
};

void verifyApplicablePbs(uint32_t wlogPbs, uint32_t devPbs)
{
    if (devPbs <= wlogPbs && wlogPbs % devPbs == 0) {
        return;
    }
    throw cybozu::Exception(__func__)
        << "Physical block size does not match"
        << wlogPbs << devPbs;
}

class AsyncLogApplyer
{
private:
    const Option& opt_;
    cybozu::util::File ddevFile_;
    const uint64_t devSize_;
    const uint32_t pbs_;
    const size_t maxPb_;
    cybozu::aio::Aio aio_;

    IoQueue ioQ_; /* FIFO. */
    ReadyQueue readyQ_; /* ready to submit. */

    OverlappedSerializer overlapped_;
    bool isSuccess_;
public:
    AsyncLogApplyer(const Option& opt, size_t bufferSize)
        : opt_(opt)
        , ddevFile_(opt.ddevPath, O_RDWR | O_DIRECT)
        , devSize_(cybozu::util::getBlockDeviceSize(ddevFile_.fd()))
        , pbs_(cybozu::util::getPhysicalBlockSize(ddevFile_.fd()))
        , maxPb_(getQueueSizeStatic(bufferSize, pbs_))
        , aio_(ddevFile_.fd(), maxPb_)
        , ioQ_(maxPb_, pbs_)
        , readyQ_()
        , overlapped_()
        , isSuccess_(false)
    {}
    ~AsyncLogApplyer() {
        if (!isSuccess_) ioQ_.waitForAllSubmitted(aio_);
    }
    /**
     * Read logs from wlogFile and apply them to the device.
     */
    void run(cybozu::util::File &wlogFile) {
        log::FileHeader wh;
        wh.readFrom(wlogFile);
        verifyApplicablePbs(wh.pbs(), pbs_);

        const uint64_t beginLsid = wh.beginLsid();
        uint64_t lsid = beginLsid;

        const uint32_t salt = wh.salt();
        LogPackHeader packH(pbs_, salt);
        while (packH.readFrom(wlogFile)) {
            if (opt_.isVerbose) {
                std::cout << packH.str() << std::endl;
            }
            for (size_t i = 0; i < packH.nRecords(); i++) {
                const LogRecord &rec = packH.record(i);
                LogBlockShared blockS(pbs_);
                if (rec.hasData()) {
                    blockS.read(wlogFile, rec.ioSizePb(pbs_));
                    log::verifyLogChecksum(rec, blockS, salt);
                } else {
                    blockS.resize(0);
                }
                redoPackIo(rec, blockS);
            }
            lsid = packH.nextLogpackLsid();
        }

        readyQ_.submit(aio_);
        waitForAllProcessingIos();

        /* Sync device. */
        ddevFile_.fdatasync();
        isSuccess_ = true;

        ::printf("Applied lsid range [%" PRIu64 ", %" PRIu64 ")\n", beginLsid, lsid);
        g_st.print();
    }
private:
    /**
     * Redo a discard log by issuing discard command.
     */
    void redoDiscard(const LogRecord& rec) {
        assert(opt_.isDiscard);
        assert(rec.isDiscard());

        /* Wait for all IO done. */
        waitForAllProcessingIos();

        /* Issue the corresponding discard IOs. */
        g_st.discardLb += rec.ioSizeLb();
        g_st.writtenLb += rec.ioSizeLb();
        cybozu::util::issueDiscard(ddevFile_.fd(), rec.offset, rec.ioSizeLb());
    }
    void waitForAllProcessingIos() {
        while (ioQ_.hasProcessing()) {
            waitForAnIoCompletion();
        }
        assert(overlapped_.empty());
    }
    template<class T>
    void verifyNotExist(const T& q, const Io *iop, const char *msg)
    {
        for (const Io* p : q) {
            if (p == iop) {
                ::printf("ERR found !!! %s\n", msg);
                ::exit(1);
            }
        }
    }
    /**
     * Wait for an IO completion.
     * If not submitted, submit before waiting.
     */
    void waitForAnIoCompletion() {
        Io& io = ioQ_.getFront();
        readyQ_.forceComplete(io, aio_);
        overlapped_.delIoAndPushReadyIos(io, readyQ_);
        ioQ_.popFront();
    }
    void processIos() {
        while (ioQ_.isFull()) {
            waitForAnIoCompletion();
        }

        while (ioQ_.hasFetched()) {
            Io& io = ioQ_.nextFetched();
            overlapped_.add(io);
            if (io.nOverlapped == 0) {
                readyQ_.push(&io);
            }
        }
        if (readyQ_.size() > maxPb_) {
            readyQ_.submit(aio_);
        }
    }
    /**
     * Redo normal IO for a logpack data.
     * Zero-discard also uses this method.
     */
    void redoNormalIo(const LogRecord &rec, LogBlockShared& blockS) {
        assert(rec.isExist());
        assert(!rec.isPadding());
        assert(opt_.isZeroDiscard || !rec.isDiscard());

        /* Create IOs. */
        uint32_t remaining = rec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
        uint64_t off = rec.offset * LOGICAL_BLOCK_SIZE;
        const uint32_t ioSizePb = rec.ioSizePb(pbs_);
        for (size_t i = 0; i < ioSizePb; i++) {
            if (ioQ_.shouldProcess()) {
                processIos();
            }
            AlignedArray block;
            if (rec.isDiscard()) {
                block.resize(pbs_);
            } else {
                block = std::move(blockS.getBlock(i));
            }
            const size_t size = std::min(pbs_, remaining);

            Io io(off, size, std::move(block));
            off += size;
            remaining -= size;
            /* Clip if the IO area is out of range in the device. */
            if (io.offset() + io.size() <= devSize_) {
                ioQ_.add(std::move(io));
                if (rec.isDiscard()) {
                    g_st.discardLb += size >> 9;
                } else {
                    g_st.normalLb += size >> 9;
                }
            } else {
                if (opt_.isVerbose) {
                    ::printf("CLIPPED\t\t%" PRIu64 "\t%zu\n",
                             io.offset(), io.size());
                }
                g_st.clippedLb += size >> 9;
            }
        }
        assert(remaining == 0);
        processIos();

        if (opt_.isVerbose) {
            ::printf("CREATE\t\t%" PRIu64 "\t%u\n",
                     rec.offset, rec.ioSizeLb());
        }
    }
    /**
     * Redo a logpack Io.
     */
    void redoPackIo(const LogRecord &rec, LogBlockShared& blockS) {
        assert(rec.isExist());

        if (rec.isPadding()) {
            /* Do nothing. */
            g_st.paddingPb += rec.ioSizePb(pbs_);
            return;
        }

        if (rec.isDiscard()) {
            if (opt_.isDiscard) {
                redoDiscard(rec);
                return;
            }
            if (!opt_.isZeroDiscard) {
                /* Ignore discard logs. */
                g_st.discardLb += rec.ioSizeLb();
                return;
            }
            /* zero-discard will use redoNormalIo(). */
        }
        redoNormalIo(rec, blockS);
    }
    static size_t getQueueSizeStatic(size_t bufferSize, size_t blockSize) {
        if (bufferSize <= blockSize) {
            throw RT_ERR("Buffer size must be > blockSize.");
        }
        size_t qs = bufferSize / blockSize;
        if (qs == 0) {
            throw RT_ERR("Queue size is must be positive.");
        }
        return qs;
    }
};

class SimpleBdevWriter
{
private:
    cybozu::util::File bdevFile_;

    struct Io2 {
        uint64_t offsetLb; // [logical block]
        uint32_t sizeLb; // [logical block]
        AlignedArray block;
    };

    std::queue<Io2> ioQ_;

public:
    explicit SimpleBdevWriter(int fd)
        : bdevFile_(fd), ioQ_() {
    }
    void prepare(uint64_t offsetLb, uint32_t sizeLb, AlignedArray &&block, bool isDiscard = false) {
        ioQ_.push({offsetLb, sizeLb, std::move(block)});
        if (isDiscard) {
            g_st.discardLb += sizeLb;
        } else {
            g_st.normalLb += sizeLb;
        }
    }
    void submit() {
        while (!ioQ_.empty()) {
            Io2 &io = ioQ_.front();
            bdevFile_.pwrite(io.block.data(), io.sizeLb << 9, io.offsetLb << 9);
            ioQ_.pop();
            g_st.writtenLb += io.sizeLb;
        }
    }
    void discard(uint64_t offsetLb, uint32_t sizeLb) {
        cybozu::util::issueDiscard(bdevFile_.fd(), offsetLb, sizeLb);
        g_st.discardLb += sizeLb;
        g_st.writtenLb += sizeLb;
    }
    void waitForAll() {
        // do nothing.
    }
};

template <typename BdevWriter>
class LogApplyer
{
private:
    const Option &opt_;
    cybozu::util::File ddevFile_;
    const uint64_t devSizeLb_; // [logical block]
    BdevWriter ddevWriter_;

public:
    explicit LogApplyer(const Option &opt)
        : opt_(opt)
        , ddevFile_(opt_.ddevPath, O_RDWR | O_DIRECT)
        , devSizeLb_(cybozu::util::getBlockDeviceSize(ddevFile_.fd()) << 9)
        , ddevWriter_(ddevFile_.fd()) {
    }
    void run(cybozu::util::File &wlogFile) {
        log::FileHeader wh;
        wh.readFrom(wlogFile);
        const uint32_t pbs = wh.pbs();
        const uint32_t salt = wh.salt();
        verifyApplicablePbs(pbs, cybozu::util::getPhysicalBlockSize(ddevFile_.fd()));

        const uint64_t beginLsid = wh.beginLsid();
        uint64_t lsid = beginLsid;
        LogPackHeader packH(pbs, salt);
        while (readLogPackHeader(wlogFile, packH, lsid)) {
            if (opt_.isVerbose) std::cout << packH.str() << std::endl;
            LogBlockShared blockS;
            for (size_t i = 0; i < packH.nRecords(); i++) {
                if (!readLogIo(wlogFile, packH, i, blockS)) {
                    throw cybozu::Exception(__func__) << "invalid log IO" << i << packH;
                }
                redoLogIo(packH, i, std::move(blockS));
                blockS.clear();
            }
            lsid = packH.nextLogpackLsid();
        }
        ddevWriter_.waitForAll();
        ddevFile_.fdatasync();

        ::printf("Applied lsid range [%" PRIu64 ", %" PRIu64 ")\n", beginLsid, lsid);
        g_st.print();
    }
private:
    void redoLogIo(const LogPackHeader &packH, size_t idx, LogBlockShared &&blockS) {
        const LogRecord &rec = packH.record(idx);
        assert(rec.isExist());

        if (rec.isPadding()) {
            /* Do nothing. */
            g_st.paddingPb += rec.ioSizePb(packH.pbs());
            return;
        }
        if (rec.isDiscard()) {
            if (opt_.isDiscard) {
                redoDiscard(rec);
                return;
            }
            if (!opt_.isZeroDiscard) {
                /* Ignore discard logs. */
                g_st.discardLb += rec.ioSizeLb();
                return;
            }
            /* zero-discard will use redoNormalIo(). */
        }
        redoNormalIo(packH, idx, std::move(blockS));
    }
    void redoDiscard(const LogRecord &rec) {
        assert(opt_.isDiscard);
        assert(rec.isDiscard());

        ddevWriter_.discard(rec.offset, rec.ioSizeLb());
    }
    void redoNormalIo(const LogPackHeader &packH, size_t idx, LogBlockShared &&blockS) {
        const LogRecord &rec = packH.record(idx);
        const uint32_t pbs = packH.pbs();
        assert(!rec.isPadding());
        assert(opt_.isZeroDiscard || !rec.isDiscard());

        const uint32_t ioSizePb = rec.ioSizePb(pbs);
        const uint32_t pbsLb = ::n_lb_in_pb(pbs);
        uint64_t offLb = rec.offset;
        uint32_t remainingLb = rec.ioSizeLb();
        for (size_t i = 0; i < ioSizePb; i++) {
            AlignedArray block;
            if (rec.isDiscard()) {
                block.resize(pbs); // zero-cleared.
            } else {
                block = std::move(blockS.getBlock(i));
            }
            const uint32_t sizeLb = std::min(pbsLb, remainingLb);

            /* Clip if the IO area is out of range in the device. */
            if (offLb + sizeLb <= devSizeLb_) {
                ddevWriter_.prepare(offLb, sizeLb, std::move(block), rec.isDiscard());
            } else {
                if (opt_.isVerbose) {
                    ::printf("CLIPPED\t\t%" PRIu64 "\t%u\n", offLb, sizeLb);
                }
                g_st.clippedLb += sizeLb;
            }
            offLb += sizeLb;
            remainingLb -= sizeLb;
        }
        assert(remainingLb == 0);
        ddevWriter_.submit();

        if (opt_.isVerbose) {
            ::printf("CREATE\t\t%" PRIu64 "\t%u\n",
                     rec.offset, rec.ioSizeLb());
        }
    }
};

void setupInputFile(cybozu::util::File &wlogFile, const Option &opt)
{
    if (opt.isFromStdin()) {
        wlogFile.setFd(0);
    } else {
        wlogFile.open(opt.inWlogPath, O_RDONLY);
    }
}

int doMain(int argc, char* argv[])
{
    const size_t BUFFER_SIZE = 4 * MEBI; /* 4MB. */
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File wlogFile;
    setupInputFile(wlogFile, opt);
    if (opt.dontUseAio) {
        LogApplyer<SimpleBdevWriter> applyer(opt);
        applyer.run(wlogFile);
    } else {
        AsyncLogApplyer applyer(opt, BUFFER_SIZE);
        applyer.run(wlogFile);
    }
    wlogFile.close();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-redo")
