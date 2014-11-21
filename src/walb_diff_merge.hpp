#pragma once
/**
 * @file
 * @brief walb diff merger.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <string>
#include <vector>
#include <queue>
#include <list>
#include <cassert>
#include <cstring>

#include "walb_diff_base.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_stat.hpp"
#include "fileio.hpp"

namespace walb {

/**
 * To merge walb diff files.
 *
 * Usage:
 *   (1) call setMaxIoBlocks() and setShouldValidateUuid() if necessary.
 *   (2) add wdiffs by calling addWdiff() or addWdiffs().
 *   (3a) call mergeToFd() to write out the merged diff data.
 *   (3b) call prepare(), then call header() and getAndRemove() multiple times for other purpose.
 */
class DiffMerger /* final */
{
private:
    class Wdiff {
    private:
        const std::string wdiffPath_;
        mutable DiffReader reader_;
        DiffFileHeader header_;
        mutable DiffRecord rec_;
        mutable DiffIo io_;
        mutable bool isFilled_;
        mutable bool isEnd_;
    public:
        explicit Wdiff(const std::string &wdiffPath)
            : wdiffPath_(wdiffPath)
            , reader_(cybozu::util::File(wdiffPath, O_RDONLY))
            , rec_()
            , io_()
            , isFilled_(false)
            , isEnd_(false) {
            reader_.readHeader(header_);
        }
        /**
         * You must open the file before calling this constructor.
         */
        explicit Wdiff(cybozu::util::File &&file)
            : wdiffPath_()
            , reader_(std::move(file))
            , rec_()
            , io_()
            , isFilled_(false)
            , isEnd_(false) {
            reader_.readHeader(header_);
        }
        const std::string &path() const { return wdiffPath_; }
        const DiffFileHeader &header() const { return header_; }
        DiffRecord getFrontRec() const {
            verifyNotEnd(__func__);
            fill();
            return rec_;
        }
        void getAndRemoveIo(DiffIo &io) {
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
        bool isEnd() const {
            fill();
            return isEnd_;
        }
        /**
         * RETURN:
         *   if the iterator has not reached the end,
         *   address of the current diff record.
         *   otherwise, UINT64_MAX.
         */
        uint64_t currentAddress() const {
            if (isEnd()) return UINT64_MAX;
            verifyFilled(__func__);
            return rec_.io_address;
        }
        const DiffStatistics& getStat() const {
            return reader_.getStat();
        }
    private:
        void fill() const {
            if (isEnd_ || isFilled_) return;
            if (reader_.readAndUncompressDiff(rec_, io_)) {
                isFilled_ = true;
            } else {
                isEnd_ = true;
            }
        }
#ifdef DEBUG
        void verifyNotEnd(const char *msg) const {
            if (isEnd()) throw cybozu::Exception(msg) << "reached to end";
        }
        void verifyFilled(const char *msg) const {
            if (!isFilled_) throw cybozu::Exception(msg) << "not filled";
        }
#else
        void verifyNotEnd(const char *) const {
        }
        void verifyFilled(const char *) const {
        }
#endif
    };
    bool shouldValidateUuid_;
    uint16_t maxIoBlocks_;

    DiffFileHeader wdiffH_;
    bool isHeaderPrepared_;

    using WdiffPtr = std::unique_ptr<Wdiff>;
    using WdiffPtrList = std::list<WdiffPtr>;
    WdiffPtrList wdiffs_;
    DiffMemory diffMem_;
    std::queue<DiffRecIo> mergedQ_;
    uint64_t doneAddr_;
    size_t searchLen_;
    /**
     * Diff recIos will be read from wdiffs_,
     * then added to diffMem_ (and merged inside it),
     * then pushed to mergedQ_ finally.
     *
     * The point of the algorithm is which wdiff will be chosen to get recIos.
     * See moveToDiffMemory() for detail.
     *
     * doneAddr_ is the minimum address in all the input wdiff streams.
     * There is no overlapped IOs which endAddr is <= doneAddr in all the streams.
     * so such IOs in diffMem_ can be put out safely.
     *
     * searchLen_ is required length [block size] as a buffer to merge wdiffs.
     * Ex. with the following diffs, we need to merge them at once.
     *
     * diff2     XXXXX
     * diff1        XXXXX
     * diff0          XXXXX
     * required  <-------->
     */

    /**
     * statIn: input wdiffs statistics.
     * statOut: output wdiff statistics.
     *     This is meaningful only when you use mergeToFd().
     */
    mutable DiffStatistics statIn_, statOut_;

public:
    DiffMerger()
        : shouldValidateUuid_(false)
        , maxIoBlocks_(0)
        , wdiffH_()
        , isHeaderPrepared_(false)
        , wdiffs_()
        , diffMem_()
        , mergedQ_()
        , doneAddr_(0)
        , searchLen_(0)
        , statIn_(), statOut_() {
    }
    /**
     * @maxIoBlocks Max io blocks in the output wdiff [logical block].
     *     0 means no limitation.
     */
    void setMaxIoBlocks(uint16_t maxIoBlocks) {
        maxIoBlocks_ = maxIoBlocks;
    }
    /**
     * @shouldValidateUuid validate that all wdiff's uuid are the same if true,
     */
    void setShouldValidateUuid(bool shouldValidateUuid) {
        shouldValidateUuid_ = shouldValidateUuid;
    }
    /**
     * Add a diff file.
     * Newer wdiff file must be added later.
     */
    void addWdiff(const std::string& wdiffPath) {
        wdiffs_.emplace_back(new Wdiff(wdiffPath));
    }
    /**
     * Add diff files.
     */
    void addWdiffs(const StrVec &wdiffPaths) {
        for (const std::string &s : wdiffPaths) {
            addWdiff(s);
        }
    }
    void addWdiffs(std::vector<cybozu::util::File> &&fileV) {
        for (cybozu::util::File &file : fileV) {
            wdiffs_.emplace_back(new Wdiff(std::move(file)));
        }
        fileV.clear();
    }
    /**
     * Merge input wdiff files and put them into output fd.
     * The last wdiff's uuid will be used for output wdiff.
     *
     * @outFd file descriptor for output wdiff.
     */
    void mergeToFd(int outFd) {
        prepare();
        DiffWriter writer(outFd);
        writer.writeHeader(wdiffH_);

        DiffRecIo d;
        while (getAndRemove(d)) {
            assert(d.isValid());
            writer.compressAndWriteDiff(d.record(), d.io().get());
        }

        writer.flush();
        assert(wdiffs_.empty());
        assert(diffMem_.empty());
        statOut_.update(writer.getStat());
    }
    /**
     * Prepare wdiff header and variables.
     */
    void prepare() {
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

            doneAddr_ = getMinimumAddr();

            removeEndedWdiffs();
            isHeaderPrepared_ = true;
        }
    }
    /**
     * Get header.
     */
    const DiffFileHeader &header() const {
        assert(isHeaderPrepared_);
        return wdiffH_;
    }
    /**
     * Get a DiffRecIo and remove it from the merger.
     * RETURN:
     *   false if there is no diffIo anymore.
     */
    bool getAndRemove(DiffRecIo &recIo) {
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
    const DiffStatistics& statIn() const {
        assert(wdiffs_.empty());
        return statIn_;
    }
    /**
     * Use this only if you used mergeToFd().
     */
    const DiffStatistics& statOut() const {
        assert(wdiffs_.empty());
        return statOut_;
    }
    std::string memUsageStr() const {
        return cybozu::itoa(searchLen_ * LBS / KIBI) + "KiB";
    }
private:
    uint64_t getMinimumAddr() const {
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
    void moveToDiffMemory() {
        size_t nr = tryMoveToDiffMemory();
        if (nr == 0 && !wdiffs_.empty()) {
            // Retry with enlarged searchLen_.
            nr = tryMoveToDiffMemory();
        }
        if (!wdiffs_.empty()) {
            // It must progress.
            assert(nr > 0);
        }
    }
    /**
     * Try to get Ios from wdiffs and add to wdiffMem_.
     *
     * minimum current address among wdiffs will be set to doneAddr_.
     * UINT64_MAX if there is no wdiffs.
     *
     * RETURN:
     *   the number of merged IOs.
     */
    size_t tryMoveToDiffMemory() {
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
            uint64_t nextAddr;
            if (goNext) {
                nextAddr = wdiff.currentAddress();
            } else {
                nextAddr = rec.endIoAddress();
            }
            nextDoneAddr = std::min(nextDoneAddr, nextAddr);
            if (goNext) ++it;
        }
        if (minAddr != UINT64_MAX) {
            assert(minAddr == range.bgn);
        }
        searchLen_ = std::max(searchLen_, range.size());
#if 0 // debug code
        std::cout << "doneAddr_ " << doneAddr_ << " "
                  << "nextDoneAddr " << nextDoneAddr << " "
                  << "searchLen_ " << searchLen_ << " "
                  << "minAddr " << minAddr << " "
                  << "range " << range << std::endl;
#endif
        doneAddr_ = nextDoneAddr;
        return nr;
    }
    bool shouldMerge(const DiffRecord& rec, uint64_t minAddr) const {
        return (rec.io_address < doneAddr_ + searchLen_)
            && (rec.endIoAddress() <= minAddr);
    }
    /**
     * Move all IOs which ioAddress + ioBlocks <= doneAddr
     * from diffMem_ to the mergedQ_.
     *
     * RETURN:
     *   false if there is no Io to move.
     */
    bool moveToMergedQueue() {
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
    void removeEndedWdiffs() {
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
    void mergeIo(const DiffRecord &rec, DiffIo &&io) {
        assert(!rec.isCompressed());
        diffMem_.add(rec, std::move(io), maxIoBlocks_);
    }
    void verifyUuid(const cybozu::Uuid &uuid) const {
        for (const WdiffPtr &wdiffP : wdiffs_) {
            const cybozu::Uuid uuid1 = wdiffP->header().getUuid();
            if (uuid1 != uuid) {
                throw cybozu::Exception(__func__) << "uuid differ" << uuid1 << uuid;
            }
        }
    }
    uint16_t getMaxIoBlocks() const {
        uint16_t ret = 0;
        for (const WdiffPtr &wdiffP : wdiffs_) {
            const uint16_t m = wdiffP->header().getMaxIoBlocks();
            if (ret < m) ret = m;
        }
        return ret;
    }
};

} //namespace walb
