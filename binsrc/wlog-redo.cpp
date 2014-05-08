/**
 * @file
 * @brief Redo walb log.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <queue>
#include <memory>
#include <deque>
#include <algorithm>
#include <utility>
#include <set>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

//#define USE_DEBUG_TRACE
/**
 * Command line configuration.
 */
class Config
{
private:
    std::string ddevPath_;
    std::string inWlogPath_;
    bool isDiscard_;
    bool isZeroDiscard_;
    bool isVerbose_;

public:
    Config(int argc, char* argv[])
        : ddevPath_()
        , inWlogPath_("-")
        , isDiscard_(false)
        , isZeroDiscard_(false)
        , isVerbose_(false) {
        parse(argc, argv);
    }

    const std::string& ddevPath() const { return ddevPath_; }
    const std::string& inWlogPath() const { return inWlogPath_; }
    bool isFromStdin() const { return inWlogPath_ == "-"; }
    bool isDiscard() const { return isDiscard_; }
    bool isZeroDiscard() const { return isZeroDiscard_; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (isDiscard() && isZeroDiscard()) {
            throw RT_ERR("Do not specify both -d and -z together.");
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlredo: redo wlog on a block device.");
        opt.appendOpt(&inWlogPath_, "-", "i", "PATH: input wlog path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isDiscard_, "d", "issue discard for discard logs.");
        opt.appendBoolOpt(&isZeroDiscard_, "z", "zero-clear for discard logs.");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&ddevPath_, "DEVICE_PATH");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

struct Statistics
{
    size_t nClipped;
    size_t nDiscard;
    size_t nPadding;
    size_t nWritten;
    size_t nOverwritten;
    Statistics()
        : nClipped(0)
        , nDiscard(0)
        , nPadding(0)
        , nWritten(0)
        , nOverwritten(0) {}
    void print() const {
        ::printf("nClipped: %zu\n"
                 "nDiscard: %zu\n"
                 "nPadding: %zu\n",
                 nClipped, nDiscard, nPadding);
        ::printf("nWritten: %zu\n"
                 "nOverwritten: %zu\n",
                 nWritten, nOverwritten);
    }
} g_st;

using AlignedArray = walb::AlignedArray;

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
            g_st.nWritten++;
        } else {
            assert(io.state == Io::Overwritten);
            g_st.nOverwritten++;
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

/**
 * To apply walb log.
 */
class WalbLogApplyer
{
private:
    const Config& config_;
    cybozu::util::BlockDevice bd_;
    const size_t pbs_;
    const size_t maxPb_;
    cybozu::aio::Aio aio_;
    walb::log::FileHeader wh_;

    IoQueue ioQ_; /* FIFO. */
    ReadyQueue readyQ_; /* ready to submit. */

    OverlappedSerializer overlapped_;
    bool isSuccess_;
public:
    WalbLogApplyer(
        const Config& config, size_t bufferSize)
        : config_(config)
        , bd_(config.ddevPath().c_str(), O_RDWR | O_DIRECT)
        , pbs_(bd_.getPhysicalBlockSize())
        , maxPb_(getQueueSizeStatic(bufferSize, pbs_))
        , aio_(bd_.getFd(), maxPb_)
        , wh_()
        , ioQ_(maxPb_, pbs_)
        , readyQ_()
        , overlapped_()
        , isSuccess_(false)
    {}

    ~WalbLogApplyer() {
        if (!isSuccess_) ioQ_.waitForAllSubmitted(aio_);
    }

    /**
     * Read logs from inFd and apply them to the device.
     */
    void readAndApply(int inFd) {
        /* Read walblog header. */
        cybozu::util::File fileR(inFd);
        wh_.readFrom(fileR);
        if (!canApply()) {
            throw RT_ERR("This walblog can not be applied to the device.");
        }

        uint64_t beginLsid = wh_.beginLsid();
        uint64_t redoLsid = beginLsid;

        const uint32_t salt = wh_.salt();
        walb::LogPackHeader packH(pbs_, salt);
        while (packH.readFrom(fileR)) {
            if (config_.isVerbose()) packH.printShort();
            for (size_t i = 0; i < packH.nRecords(); i++) {
                const walb::LogRecord &rec = packH.record(i);
                walb::LogBlockShared blockS(pbs_);
                if (rec.hasData()) {
                    blockS.read(fileR, rec.ioSizePb(pbs_));
                    walb::log::verifyLogChecksum(rec, blockS, salt);
                } else {
                    blockS.resize(0);
                }
                redoPackIo(rec, blockS);
            }
            redoLsid = packH.nextLogpackLsid();
        }

        readyQ_.submit(aio_);
        waitForAllProcessingIos();

        /* Sync device. */
        bd_.fdatasync();
        isSuccess_ = true;

        ::printf("Applied lsid range [%" PRIu64 ", %" PRIu64 ")\n", beginLsid, redoLsid);
        g_st.print();
    }

private:
    bool canApply() const {
        const struct walblog_header &h = wh_.header();
        bool ret = pbs_ <= h.physical_bs &&
            h.physical_bs % pbs_ == 0;
        if (!ret) {
            LOGe("Physical block size does not match %u %zu.\n",
                 h.physical_bs, pbs_);
        }
        return ret;
    }

    u32 salt() const {
        return wh_.header().log_checksum_salt;
    }

    /**
     * Redo a discard log by issuing discard command.
     */
    void redoDiscard(const walb::LogRecord& rec) {
        assert(config_.isDiscard());
        assert(rec.isDiscard());

        /* Wait for all IO done. */
        waitForAllProcessingIos();

        /* Issue the corresponding discard IOs. */
        uint64_t offsetAndSize[2];
        offsetAndSize[0] = rec.offset * LOGICAL_BLOCK_SIZE;
        offsetAndSize[1] = rec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
        int ret = ::ioctl(bd_.getFd(), BLKDISCARD, &offsetAndSize);
        if (ret) {
            throw RT_ERR("discard command failed.");
        }
        g_st.nDiscard += rec.ioSizePb(pbs_);
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
                printf("ERR found !!! %s\n", msg);
                exit(1);
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
    void redoNormalIo(const walb::LogRecord &rec, walb::LogBlockShared& blockS) {
        assert(rec.isExist());
        assert(!rec.isPadding());
        assert(config_.isZeroDiscard() || !rec.isDiscard());

        /* Create IOs. */
        size_t remaining = rec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
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
            if (io.offset() + io.size() <= bd_.getDeviceSize()) {
                ioQ_.add(std::move(io));
                if (rec.isDiscard()) { g_st.nDiscard++; }
            } else {
                if (config_.isVerbose()) {
                    ::printf("CLIPPED\t\t%" PRIu64 "\t%zu\n",
                             io.offset(), io.size());
                }
                g_st.nClipped++;
            }
        }
        assert(remaining == 0);
        processIos();

        if (config_.isVerbose()) {
            ::printf("CREATE\t\t%" PRIu64 "\t%u\n",
                     rec.offset, rec.ioSizeLb());
        }
    }

    /**
     * Redo a logpack Io.
     */
    void redoPackIo(const walb::LogRecord &rec, walb::LogBlockShared& blockS) {
        assert(rec.isExist());
        const uint32_t ioSizePb = rec.ioSizePb(pbs_);

        if (rec.isPadding()) {
            /* Do nothing. */
            g_st.nPadding += ioSizePb;
            return;
        }

        if (rec.isDiscard()) {
            if (config_.isDiscard()) {
                redoDiscard(rec);
                return;
            }
            if (!config_.isZeroDiscard()) {
                /* Ignore discard logs. */
                g_st.nDiscard += ioSizePb;
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

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);

    const size_t BUFFER_SIZE = 4 * 1024 * 1024; /* 4MB. */
    Config config(argc, argv);
    config.check();

    WalbLogApplyer wlApp(config, BUFFER_SIZE);
    if (config.isFromStdin()) {
        wlApp.readAndApply(0);
    } else {
        cybozu::util::File file(config.inWlogPath(), O_RDONLY);
        wlApp.readAndApply(file.fd());
        file.close();
    }
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
