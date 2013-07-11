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
#include <deque>
#include <cassert>
#include <cstring>

#include "walb_diff.hpp"

#ifndef WALB_DIFF_MERGE_HPP
#define WALB_DIFF_MERGE_HPP

namespace walb {
namespace diff {

/**
 * To merge walb diff files.
 */
class WalbDiffMerger /* final */
{
private:
    using DiffRecord = walb::diff::WalbDiffRecord;
    using DiffIo = walb::diff::BlockDiffIo;
    using DiffRecIo = walb::diff::DiffRecIo;

    class Wdiff {
    private:
        std::string wdiffPath_;
        mutable walb::diff::WalbDiffReader reader_;
        std::shared_ptr<walb::diff::WalbDiffFileHeader> headerP_;
        mutable DiffRecord rec_;
        mutable DiffIo io_;
        mutable bool isFilled_;
        mutable bool isEnd_;
    public:
        Wdiff(const std::string &wdiffPath)
            : wdiffPath_(wdiffPath), reader_(wdiffPath, O_RDONLY)
            , headerP_(reader_.readHeader())
            , rec_()
            , io_()
            , isFilled_(false)
            , isEnd_(false) {
        }
        const std::string &path() const { return wdiffPath_; }
        walb::diff::WalbDiffReader &reader() { return reader_; }
        walb::diff::WalbDiffFileHeader &header() { return *headerP_; }
        const DiffRecord &front() {
            fill();
            assert(isFilled_);
            return rec_;
        }
        void pop(DiffIo &io) {
            if (isEnd()) return;

            /* for check */
            uint64_t endIoAddr0 = rec_.endIoAddress();

            assert(isFilled_);
            io = std::move(io_);
            isFilled_ = false;
            fill();

            /* for check */
            if (!isEnd() && rec_.ioAddress() < endIoAddr0) {
                throw RT_ERR("Invalid wdiff: IOs must be sorted and not overlapped each other.");
            }
        }
        bool isEnd() const {
            fill();
            return isEnd_;
        }
        /**
         * RETURN:
         *   address of the current head diff record
         *   if the iterator has not reached the end,
         *   or maximum value.
         */
        uint64_t currentAddress() const {
            if (isEnd()) return uint64_t(-1);
            assert(isFilled_);
            return rec_.ioAddress();
        }
    private:
        void fill() const {
            if (isEnd_ || isFilled_) return;
            if (reader_.readAndUncompressDiff(rec_, io_)) {
                isFilled_ = true;
            } else {
                rec_.clearExists();
                io_ = DiffIo();
                isFilled_ = false;
                isEnd_ = true;
            }
        }
    };

    std::deque<std::shared_ptr<Wdiff> > wdiffs_;
    std::deque<std::shared_ptr<Wdiff> > doneWdiffs_;
    /* Wdiffs' lifetime must be the same as the WalbDiffMerger instance. */

    walb::diff::WalbDiffMemory wdiffMem_;
    struct walb_diff_file_header wdiffRawH_;
    WalbDiffFileHeader wdiffH_;
    bool isHeaderPrepared_;
    std::queue<DiffRecIo> mergedQ_;
    bool shouldValidateUuid_;
    uint16_t maxIoBlocks_;
    uint64_t doneAddr_;

public:
    WalbDiffMerger()
        : wdiffs_()
        , doneWdiffs_()
        , wdiffMem_()
        , wdiffRawH_()
        , wdiffH_(wdiffRawH_)
        , isHeaderPrepared_(false)
        , mergedQ_()
        , shouldValidateUuid_(false)
        , maxIoBlocks_(0)
        , doneAddr_(0) {
    }
    ~WalbDiffMerger() noexcept {
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
        std::shared_ptr<Wdiff> p(new Wdiff(wdiffPath));
        wdiffs_.push_back(p);
    }
    /**
     * Add diff files.
     */
    void addWdiffs(const std::vector<std::string> &wdiffPaths) {
        for (const std::string &s : wdiffPaths) {
            addWdiff(s);
        }
    }
    /**
     * Merge input wdiff files and put them into output fd.
     * The last wdiff's uuid will be used for output wdiff.
     *
     * @outFd file descriptor for output wdiff.
     */
    void mergeToFd(int outFd) {
        prepare();
        walb::diff::WalbDiffWriter wdiffWriter(outFd);
        wdiffWriter.writeHeader(wdiffH_);

        DiffRecIo d;
        while (pop(d)) {
            assert(d.isValid());
            wdiffWriter.compressAndWriteDiff(d.record(), d.io());
        }

        wdiffWriter.flush();
        assert(wdiffs_.empty());
        assert(wdiffMem_.empty());
    }
    /**
     * Prepare wdiff header and variables.
     */
    void prepare() {
        if (!isHeaderPrepared_) {
            if (wdiffs_.empty()) {
                throw RT_ERR("Wdiff's is not set.");
            }
            const uint8_t *uuid = wdiffs_.back()->header().getUuid();
            if (shouldValidateUuid_) { checkUuid(uuid); }

            ::memset(&wdiffRawH_, 0, sizeof(wdiffRawH_));
            wdiffH_.setUuid(uuid);
            wdiffH_.setMaxIoBlocksIfNecessary(
                maxIoBlocks_ == 0 ? getMaxIoBlocks() : maxIoBlocks_);

            doneAddr_ = 0;
            removeEndedWdiffs();

            isHeaderPrepared_ = true;
        }
    }
    /**
     * Get header.
     */
    const WalbDiffFileHeader &header() const {
        return wdiffH_;
    }
    /**
     * Pop a diffIo.
     * RETURN:
     *   false if there is no diffIo anymore.
     */
    bool pop(DiffRecIo &recIo) {
        prepare();
        while (mergedQ_.empty()) {
            if (wdiffs_.empty()) {
                if (wdiffMem_.empty()) return false;
                moveToQueueUpto(uint64_t(-1));
                break;
            }
            for (size_t i = 0; i < wdiffs_.size(); i++) {
                assert(!wdiffs_[i]->isEnd());
                DiffRecord rec(wdiffs_[i]->front());
                assert(rec.isValid());
                if (canMergeIo(i, rec)) {
                    DiffIo io;
                    wdiffs_[i]->pop(io);
                    assert(io.isValid());
                    mergeIo(rec, std::move(io));
                }
            }
            removeEndedWdiffs();
            doneAddr_ = getMinCurrentAddress();
            moveToQueueUpto(doneAddr_);
        }
        assert(!mergedQ_.empty());
        recIo = std::move(mergedQ_.front());
        mergedQ_.pop();
        return true;
    }
private:
    /**
     * Move all IOs which ioAddress + ioBlocks <= maxAddr
     * to a specified queue.
     */
    void moveToQueueUpto(uint64_t maxAddr) {
        auto it = wdiffMem_.iterator();
        it.begin();
        while (it.isValid() && it.record().endIoAddress() <= maxAddr) {
            walb::diff::DiffRecIo r = std::move(it.recIo());
            assert(r.isValid());
            mergedQ_.push(std::move(r));
            it.erase();
        }
    }
    uint64_t getMinCurrentAddress() const {
        uint64_t minAddr = uint64_t(-1);
        for (const std::shared_ptr<Wdiff> &wdiffP : wdiffs_) {
            uint64_t addr = wdiffP->currentAddress();
            if (addr < minAddr) { minAddr = addr; }
        }
        return minAddr;
    }
    void removeEndedWdiffs() {
        std::deque<std::shared_ptr<Wdiff> >::iterator it = wdiffs_.begin();
        while (it != wdiffs_.end()) {
            std::shared_ptr<Wdiff> p = *it;
            if (p->isEnd()) {
                doneWdiffs_.push_back(p);
                it = wdiffs_.erase(it);
            } else {
                ++it;
            }
        }
    }
    void mergeIo(const DiffRecord &rec, DiffIo &&io) {
        assert(!rec.isCompressed());
        wdiffMem_.add(rec, std::move(io), maxIoBlocks_);
    }
    bool canMergeIo(size_t i, const DiffRecord &rec) {
        if (i == 0) return true;
        for (size_t j = 0; j < i; j++) {
            if (!(rec.endIoAddress() <= wdiffs_[j]->currentAddress())) {
                return false;
            }
        }
        return true;
    }
    void checkUuid(const uint8_t *uuid) const {
        for (const std::shared_ptr<Wdiff> &wdiffP : wdiffs_) {
            if (::memcmp(wdiffP->header().getUuid(), uuid, UUID_SIZE) != 0) {
                throw RT_ERR("Uuids differ\n");
            }
        }
    }
    uint16_t getMaxIoBlocks() const {
        uint16_t ret = 0;
        for (const std::shared_ptr<Wdiff> &wdiffP : wdiffs_) {
            uint16_t m = wdiffP->header().getMaxIoBlocks();
            if (ret < m) { ret = m; }
        }
        return ret;
    }
};

}} //namespace walb::diff

#endif /* WALB_DIFF_MERGE_HPP */
