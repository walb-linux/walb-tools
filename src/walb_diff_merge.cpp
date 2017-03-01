#include "walb_diff_merge.hpp"

namespace walb {

void DiffMerger::Wdiff::getAndRemoveIo(DiffIo &io)
{
    verifyNotEnd(__func__);

    /* for check */
    const uint64_t endIoAddr0 = rec_.endIoAddress();

    verifyFilled(__func__);
    io = std::move(io_);
    isFilled_ = false;
    fill();

    /* for check */
    if (!isEnd() && rec_.io_address < endIoAddr0) {
        throw RT_ERR("Invalid wdiff: IOs must be sorted and not overlapped each other.");
    }
}

void DiffMerger::Wdiff::fill() const
{
    if (isEnd_ || isFilled_) return;
    if (reader_.readAndUncompressDiff(rec_, io_, false)) {
        isFilled_ = true;
    } else {
        isEnd_ = true;
    }
}

void DiffMerger::mergeToFd(int outFd)
{
    prepare();
    DiffWriter writer(outFd);
    writer.writeHeader(wdiffH_);

    DiffRecIo d;
    while (getAndRemove(d)) {
        assert(d.isValid());
        writer.compressAndWriteDiff(d.record(), d.io().get());
    }

    writer.close();
    assert(wdiffs_.empty());
    assert(diffMem_.empty());
    statOut_.update(writer.getStat());
}

void DiffMerger::prepare()
{
    if (!isHeaderPrepared_) {
        if (wdiffs_.empty()) {
            throw cybozu::Exception(__func__) << "Wdiffs are not set.";
        }
        const cybozu::Uuid uuid = wdiffs_.back()->header().getUuid();
        if (shouldValidateUuid_) verifyUuid(uuid);

        wdiffH_.init();
        wdiffH_.setUuid(uuid);
        wdiffH_.setMaxIoBlocksIfNecessary(
            maxIoBlocks_ == 0 ? getMaxIoBlocks() : maxIoBlocks_);

        removeEndedWdiffs();
        doneAddr_ = getMinimumAddr();
        isHeaderPrepared_ = true;
    }
}

bool DiffMerger::getAndRemove(DiffRecIo &recIo)
{
    assert(isHeaderPrepared_);
    while (mergedQ_.empty()) {
        moveToDiffMemory();
        if (!moveToMergedQueue()) {
            assert(wdiffs_.empty());
            return false;
        }
    }
    recIo = std::move(mergedQ_.front());
    mergedQ_.pop();
    return true;
}

uint64_t DiffMerger::getMinimumAddr() const
{
    uint64_t addr = UINT64_MAX;
    WdiffPtrList::const_iterator it = wdiffs_.begin();
    while (it != wdiffs_.end()) {
        Wdiff &wdiff = **it;
        DiffRecord rec = wdiff.getFrontRec();
        addr = std::min(addr, rec.io_address);
        ++it;
    }
    return addr;
}

void DiffMerger::moveToDiffMemory()
{
    size_t nr = tryMoveToDiffMemory();
    if (nr == 0 && !wdiffs_.empty()) {
        // Retry with enlarged searchLen_.
        nr = tryMoveToDiffMemory();
    }
    if (!wdiffs_.empty()) {
        // It must progress.
        (void)nr;
        assert(nr > 0);
    }
}

namespace walb_diff_merge_local {

struct Range
{
    uint64_t bgn;
    uint64_t end;

    Range() : bgn(0), end(0) {
    }
    Range(uint64_t bgn, uint64_t end)
        : bgn(bgn), end(end) {
    }
    explicit Range(const DiffRecord &rec) {
        set(rec);
    }
    void set(const DiffRecord &rec) {
        bgn = rec.io_address;
        end = rec.endIoAddress();
    }
    bool isOverlapped(const Range &rhs) const {
        return bgn < rhs.end && rhs.bgn < end;
    }
    bool isLeftRight(const Range &rhs) const {
        return end <= rhs.bgn;
    }
    void merge(const Range &rhs) {
        bgn = std::min(bgn, rhs.bgn);
        end = std::max(end, rhs.end);
    }
    size_t size() const {
        assert(end - bgn <= SIZE_MAX);
        return end - bgn;
    }
    friend inline std::ostream &operator<<(std::ostream &os, const Range &range) {
        os << "(" << range.bgn << ", " << range.end << ")";
        return os;
    }
};

} // namespace walb_diff_merge_local

size_t DiffMerger::tryMoveToDiffMemory()
{
    using Range = walb_diff_merge_local::Range;

    size_t nr = 0;
    uint64_t nextDoneAddr = UINT64_MAX;
    uint64_t minAddr = UINT64_MAX;
    if (wdiffs_.empty()) {
        doneAddr_ = nextDoneAddr;
        return 0;
    }
    WdiffPtrList::iterator it = wdiffs_.begin();
    Range range(wdiffs_.front()->getFrontRec());
    while (it != wdiffs_.end()) {
        bool goNext = true;
        Wdiff &wdiff = **it;
        DiffRecord rec = wdiff.getFrontRec();
        minAddr = std::min(minAddr, rec.io_address);
        Range curRange(rec);
        while (shouldMerge(rec, nextDoneAddr)) {
            nr++;
            curRange.merge(Range(rec));
            DiffIo io;
            wdiff.getAndRemoveIo(io);
            mergeIo(rec, std::move(io));
            if (wdiff.isEnd()) {
                statIn_.update(wdiff.getStat());
                it = wdiffs_.erase(it);
                goNext = false;
                break;
            }
            rec = wdiff.getFrontRec();
        }
        if (range.isOverlapped(curRange)) {
            range.merge(curRange);
        } else if (curRange.isLeftRight(range)) {
            range = curRange;
        } else {
            assert(range.isLeftRight(curRange));
            // do nothing
        }
        if (goNext) {
            nextDoneAddr = std::min(nextDoneAddr, wdiff.currentAddress());
            ++it;
        }
    }
    if (minAddr != UINT64_MAX) {
        assert(minAddr == range.bgn);
    }
    searchLen_ = std::max(searchLen_, range.size());
#if 0 // debug code
    std::cout << "nr " << nr << " "
              << "doneAddr_ " << doneAddr_ << " "
              << "nextDoneAddr " << (nextDoneAddr == UINT64_MAX ? "-" : cybozu::itoa(nextDoneAddr)) << " "
              << "searchLen_ " << searchLen_ << " "
              << "minAddr " << minAddr << " "
              << "range " << range << std::endl;
#endif
    doneAddr_ = nextDoneAddr;
    return nr;
}

bool DiffMerger::moveToMergedQueue()
{
    if (diffMem_.empty()) return false;
    DiffMemory::Map& map = diffMem_.getMap();
    DiffMemory::Map::iterator i = map.begin();
    while (i != map.end()) {
        DiffRecIo& recIo = i->second;
        if (recIo.record().endIoAddress() > doneAddr_) break;
        mergedQ_.push(std::move(recIo));
        diffMem_.eraseFromMap(i);
    }
    return true;
}

void DiffMerger::removeEndedWdiffs()
{
    WdiffPtrList::iterator it = wdiffs_.begin();
    while (it != wdiffs_.end()) {
        const Wdiff &wdiff = **it;
        if (wdiff.isEnd()) {
            statIn_.update(wdiff.getStat());
            it = wdiffs_.erase(it);
        } else {
            ++it;
        }
    }
}

void DiffMerger::verifyUuid(const cybozu::Uuid &uuid) const
{
    for (const WdiffPtr &wdiffP : wdiffs_) {
        const cybozu::Uuid uuid1 = wdiffP->header().getUuid();
        if (uuid1 != uuid) {
            throw cybozu::Exception(__func__) << "uuid differ" << uuid1 << uuid;
        }
    }
}

uint32_t DiffMerger::getMaxIoBlocks() const
{
    uint32_t ret = 0;
    for (const WdiffPtr &wdiffP : wdiffs_) {
        const uint32_t m = wdiffP->header().getMaxIoBlocks();
        if (ret < m) ret = m;
    }
    return ret;
}

} //namespace walb
