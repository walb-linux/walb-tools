#include "bdev_writer.hpp"
#include "cybozu/exception.hpp"

namespace walb {

namespace bdev_writer_local {


struct Debug g_debug;


void Io::print(::FILE *p) const
{
    ::fprintf(p, "IO offset: %" PRIu64 " size: %zu aioKey: %u "
              "state: %d\n",
              offset_, size_, aioKey, state);
    for (auto &b : blocks_) {
        ::fprintf(p, "  block %p\n", b.data());
    }
}


bool Io::canMerge(const Io& rhs) const
{
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


bool Io::tryMerge(Io& rhs)
{
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


void IoQueue::add(Io &&io)
{
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


Io& IoQueue::nextFetched()
{
    Io &io = *fetchedBegin_++;
    processingSize_ += io.size();
    fetchedSize_ -= io.size();
    g_debug.addIoQ(io);
    return io;
}


void IoQueue::waitForAllSubmitted(cybozu::aio::Aio &aio) noexcept
{
    for (List::iterator it = list_.begin(); it != fetchedBegin_; ++it) {
        Io& io = *it;
        if (io.state == Io::Submitted) {
            try {
                aio.waitFor(io.aioKey);
            } catch (...) {}
        }
    }
}


void ReadyQueue::submit(cybozu::aio::Aio &aio)
{
    size_t nBulk = 0;
    while (!ioSet_.empty() && !aio.isQueueFull()) {
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


void ReadyQueue::forceComplete(Io &io, cybozu::aio::Aio &aio)
{
    for (;;) {
        if (io.state == Io::Init) {
            /* The IO is not still submitted. */
            assert(io.nOverlapped == 0);
            submit(aio);
        } else if (io.state == Io::Overwritten) {
            tryErase(io);
            break;
        }
        if (io.state == Io::Submitted) {
            assert(io.aioKey > 0);
            waitFor(io, aio);
            break;
        }
        assert(io.state == Io::Init);
        waitAny(aio);
    }
}


bool ReadyQueue::empty() const
{
    const bool ret = ioSet_.empty();
    if (ret) {
        assert(ioSet_.size() == 0);
        assert(totalSize_ == 0);
    }
    return ret;
}


void ReadyQueue::tryErase(Io& io)
{
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


void ReadyQueue::waitFor(Io& io, cybozu::aio::Aio &aio)
{
    assert(io.state == Io::Submitted);
    auto it = doneSet_.find(io.aioKey);
    if (it == doneSet_.end()) {
        aio.waitFor(io.aioKey);
    } else {
        doneSet_.erase(it);
    }
}


void ReadyQueue::waitAny(cybozu::aio::Aio &aio)
{
    std::queue<uint32_t> q;
    aio.waitOneOrMore(q);
    while (!q.empty()) {
        doneSet_.insert(q.front());
        q.pop();
    }
}


void OverlappedSerializer::add(Io& io)
{
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


void OverlappedSerializer::delIoAndPushReadyIos(Io& io, ReadyQueue& readyQ)
{
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


void OverlappedSerializer::erase(Io &io)
{
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


void WriteIoStatistics::clear()
{
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


void WriteIoStatistics::print(::FILE *fp) const
{
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


std::ostream& operator<<(std::ostream& os, const WriteIoStatistics& stat)
{
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


bool AsyncBdevWriter::discard(uint64_t offLb, uint32_t sizeLb)
{
    if (isClipped(offLb, sizeLb)) return false;
    // This is not clever method to serialize IOs.
    waitForAll();
    cybozu::util::issueDiscard(bdevFile_.fd(), offLb, sizeLb);
    stat_.addDiscard(sizeLb);
    stat_.addWritten(sizeLb);
    return true;
}


void AsyncBdevWriter::waitForAllProcessingIos()
{
    while (ioQ_.hasProcessing()) {
        waitForAnIoCompletion();
    }
    assert(overlapped_.empty());
    assert(readyQ_.empty());
}


void AsyncBdevWriter::waitForAnIoCompletion()
{
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


void AsyncBdevWriter::processIos(bool force)
{
    for (;;) {
        while (hasManyProcessingIos()) {
            waitForAnIoCompletion();
        }
        while (ioQ_.hasFetched()) {
            bdev_writer_local::Io& io = ioQ_.nextFetched();
            overlapped_.add(io);
            if (io.nOverlapped == 0) {
                readyQ_.push(&io);
            }
        }
        if (force || hasEnoughReadyIos()) {
            readyQ_.submit(aio_);
        }
        if (!force || readyQ_.empty()) break;
    }
}

} // namespace walb
