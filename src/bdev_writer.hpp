#pragma once
#include <list>
#include <cassert>
#include <cstdio>
#include "walb_types.hpp"
#include "aio_util.hpp"

//#define USE_DEBUG_TRACE

namespace walb {

namespace bdev_writer_local {

/**
 * Io data.
 */
class Io
{
private:
    uint64_t offset_; // [bytes].
    size_t size_; // [bytes].
    std::list<AlignedArray> blocks_;

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
    const char *data() const { return blocks_.front().data(); }
    bool empty() const { return blocks_.empty(); }

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
 *  Io state
 *  (file-sink) -> fetched -> pending -> ready -> submitted -> (completed)
 *                            <--       processing      -->
 *
 * the contents of list_
 * begin                   ---   fetchedBegin_    --- end
 * <--- submitted + ready + pending  ---><--- fetched --->
 *           processing
 *         processingSize [byte]           fetchedSize [byte]
 */
class IoQueue
{
    size_t processingSize_; // total size of pending/ready/submitted [byte].
    size_t fetchedSize_; // [byte].
    typedef std::list<Io> List;
    List list_;
    List::iterator fetchedBegin_;
    static const size_t maxIoSize_ = MEBI; // 1 MiB.
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
    explicit IoQueue()
        : processingSize_(0)
        , fetchedSize_(0)
        , fetchedBegin_(list_.end())
    {
    }
    void add(Io &&io) {
        assert(io.size() > 0);
        fetchedSize_ += io.size();
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
    size_t getProcessingSize() const { return processingSize_; }

    Io& nextFetched() {
        Io &io = *fetchedBegin_++;
        processingSize_ += io.size();
        fetchedSize_ -= io.size();
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
        processingSize_ -= io.size();
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
    size_t totalSize_;

public:
    ReadyQueue() : ioSet_(), totalSize_(0) {}
    void push(Io *iop) {
        assert(iop);
        ioSet_.insert(iop);
        g_debug.addReadyQ(*iop);
        totalSize_ += iop->size();
    }
    size_t size() const {
        return ioSet_.size();
    }
    size_t totalSize() const {
        return totalSize_;
    }
    void submit(cybozu::aio::Aio &aio) {
        size_t nBulk = 0;
        while (!ioSet_.empty()) {
            Io* iop = *ioSet_.begin();
            ioSet_.erase(ioSet_.begin());
            g_debug.delReadyQ(*iop);
            totalSize_ -= iop->size();

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
        } else {
            assert(io.state == Io::Overwritten);
        }
    }
    bool empty() const {
        const bool ret = ioSet_.empty();
        if (ret) {
            assert(ioSet_.size() == 0);
            assert(totalSize_ == 0);
        }
        return ret;
    }
private:
    void tryErase(Io& io) {
        IoSet::iterator i, e;
        std::tie(i, e) = ioSet_.equal_range(&io);
        while (i != e) {
            if (*i == &io) {
                ioSet_.erase(i);
                g_debug.delReadyQ(io);
                totalSize_ -= io.size();
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
    void forEachOverlapped(const Io &io, Func &&func) {
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

} // namespace bdev_writer_local

struct WriteIoStatistics
{
    size_t normalNr;
    size_t discardNr;
    size_t writtenNr;
    size_t overwrittenNr;
    size_t clippedNr;

    uint64_t normalLb;
    uint64_t discardLb;
    uint64_t writtenLb;
    uint64_t overwrittenLb;
    uint64_t clippedLb;

    WriteIoStatistics() {
        clear();
    }
    void clear() {
        normalNr = 0;
        discardNr = 0;
        writtenNr = 0;
        overwrittenNr = 0;
        clippedNr = 0;
        normalLb = 0;
        discardLb = 0;
        writtenLb = 0;
        overwrittenLb = 0;
        clippedLb = 0;
    }
    void addNormal(size_t sizeLb) {
        normalNr++;
        normalLb += sizeLb;
    }
    void addDiscard(size_t sizeLb) {
        discardNr++;
        discardLb += sizeLb;
    }
    void addWritten(size_t sizeLb) {
        writtenNr++;
        writtenLb += sizeLb;
    }
    void addOverwritten(size_t sizeLb) {
        overwrittenNr++;
        overwrittenLb += sizeLb;
    }
    void addClipped(size_t sizeLb) {
        clippedNr++;
        clippedLb += sizeLb;
    }
    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "normal:      %10zu ( %10" PRIu64 " LB)\n"
                  "discard:     %10zu ( %10" PRIu64 " LB)\n"
                  "written:     %10zu ( %10" PRIu64 " LB)\n"
                  "overwritten: %10zu ( %10" PRIu64 " LB)\n"
                  "clipped:     %10zu ( %10" PRIu64 " LB)\n"
                  , normalNr, normalLb
                  , discardNr, discardLb
                  , writtenNr, writtenLb
                  , overwrittenNr, overwrittenLb
                  , clippedNr, clippedLb);
    }
    void printOneline(::FILE *fp = ::stdout) const {
        std::stringstream ss;
        ss << *this;
        ::fprintf(fp, "%s\n", ss.str().c_str());
    }
    friend inline std::ostream& operator<<(std::ostream& os, const WriteIoStatistics& stat) {
        os << "nrN " << stat.normalNr
           << " nrD " << stat.discardNr
           << " nrW " << stat.writtenNr
           << " nrO " << stat.overwrittenNr
           << " nrC " << stat.clippedNr
           << "  "
           << "lbN " << stat.normalLb
           << " lbD " << stat.discardLb
           << " lbW " << stat.writtenLb
           << " lbO " << stat.overwrittenLb
           << " lbC " << stat.clippedLb;
        return os;
    }
};

class SimpleBdevWriter
{
private:
    cybozu::util::File bdevFile_;
    uint64_t bdevSizeLb_;

    struct Io2 {
        uint64_t offLb; // [logical block]
        size_t sizeLb; // [logical block]
        AlignedArray block;
    };
    std::queue<Io2> ioQ_;

    WriteIoStatistics stat_;

public:
    explicit SimpleBdevWriter(int fd)
        : bdevFile_(fd)
        , bdevSizeLb_(cybozu::util::getBlockDeviceSize(fd) << 9)
        , ioQ_(), stat_() {
    }
    bool prepare(uint64_t offLb, size_t sizeLb, AlignedArray &&block) {
        if (isClipped(offLb, sizeLb)) return false;
        ioQ_.push({offLb, sizeLb, std::move(block)});
        stat_.addNormal(sizeLb);
        return true;
    }
    void submit() {
        while (!ioQ_.empty()) {
            Io2 &io = ioQ_.front();
            bdevFile_.pwrite(io.block.data(), io.sizeLb << 9, io.offLb << 9);
            stat_.addWritten(io.sizeLb);
            ioQ_.pop();
        }
    }
    bool discard(uint64_t offLb, size_t sizeLb) {
        if (isClipped(offLb, sizeLb)) return false;
        cybozu::util::issueDiscard(bdevFile_.fd(), offLb, sizeLb);
        stat_.addDiscard(sizeLb);
        stat_.addWritten(sizeLb);
        return true;
    }
    void waitForAll() {
        // do nothing.
    }
    const WriteIoStatistics &getStat() const {
        return stat_;
    }
private:
    bool isClipped(uint64_t offLb, size_t sizeLb) {
        if (offLb + sizeLb > bdevSizeLb_) {
            stat_.addClipped(sizeLb);
            return true;
        }
        return false;
    }
};

class AsyncBdevWriter
{
private:
    cybozu::util::File bdevFile_;
    uint64_t bdevSizeLb_;
    const size_t bufferSize_;

    cybozu::aio::Aio aio_;
    bdev_writer_local::IoQueue ioQ_; /* FIFO. */
    bdev_writer_local::ReadyQueue readyQ_; /* ready to submit. */
    bdev_writer_local::OverlappedSerializer overlapped_;

    WriteIoStatistics stat_;
public:
    explicit AsyncBdevWriter(int fd, size_t bufferSize = 4 * MEBI)
        : bdevFile_(fd)
        , bdevSizeLb_(cybozu::util::getBlockDeviceSize(fd) << 9)
        , bufferSize_(bufferSize)
        , aio_(fd, bufferSize >> 9)
        , ioQ_()
        , readyQ_()
        , overlapped_()
        , stat_() {
    }
    ~AsyncBdevWriter() noexcept {
        ioQ_.waitForAllSubmitted(aio_);
    }
    bool prepare(uint64_t offLb, size_t sizeLb, AlignedArray &&block) {
        if (isClipped(offLb, sizeLb)) return false;
        ioQ_.add(bdev_writer_local::Io(offLb << 9, sizeLb << 9, std::move(block)));
        stat_.addNormal(sizeLb);
        return true;
    }
    /**
     * Causion: this may not submit IOs really.
     * Call waitForAll() to force submit.
     */
    void submit() {
        processIos(false);
    }
    bool discard(uint64_t offLb, uint32_t sizeLb) {
        if (isClipped(offLb, sizeLb)) return false;
        // This is not clever method to serialize IOs.
        waitForAll();
        cybozu::util::issueDiscard(bdevFile_.fd(), offLb, sizeLb);
        stat_.addDiscard(sizeLb);
        stat_.addWritten(sizeLb);
        return true;
    }
    void waitForAll() {
        processIos(true);
        waitForAllProcessingIos();
    }
    const WriteIoStatistics &getStat() const {
        return stat_;
    }
private:
    void waitForAllProcessingIos() {
        while (ioQ_.hasProcessing()) {
            waitForAnIoCompletion();
        }
        assert(overlapped_.empty());
        assert(readyQ_.empty());
    }
    void waitForAnIoCompletion() {
        bdev_writer_local::Io& io = ioQ_.getFront();
        readyQ_.forceComplete(io, aio_);
        if (io.state == bdev_writer_local::Io::Submitted) {
            stat_.addWritten(io.size() >> 9);
        } else {
            stat_.addOverwritten(io.size() >> 9);
        }
        overlapped_.delIoAndPushReadyIos(io, readyQ_);
        ioQ_.popFront();
    }
    void processIos(bool force) {
        while (ioQ_.getProcessingSize() >= bufferSize_ / 2) {
            waitForAnIoCompletion();
        }
        while (ioQ_.hasFetched()) {
            bdev_writer_local::Io& io = ioQ_.nextFetched();
            overlapped_.add(io);
            if (io.nOverlapped == 0) {
                readyQ_.push(&io);
            }
        }
        if (force || readyQ_.totalSize() >= bufferSize_ / 2) {
            readyQ_.submit(aio_);
        }
    }
    bool isClipped(uint64_t offLb, size_t sizeLb) {
        if (offLb + sizeLb > bdevSizeLb_) {
            stat_.addClipped(sizeLb);
            return true;
        }
        return false;
    }
};

} // namespace walb
