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
         *   otherwise, -1.
         */
        uint64_t currentAddress() const {
            if (isEnd()) return uint64_t(-1);
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
    /**
     * Diff recIos will be read from wdiffs_,
     * then added to diffMem_ (and merged inside it),
     * then pushed to mergedQ_ finally.
     *
     * The point of the algorithm is which wdiff will be chosen to get recIos.
     * See moveToDiffMemory() for detail.
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
            const uint64_t doneAddr = moveToDiffMemory();
            if (!moveToMergedQueue(doneAddr)) {
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
private:
    /**
     * Try to get Ios from wdiffs and add to wdiffMem_.
     *
     * RETURN:
     *   minimum current address among wdiffs.
     *   uint64_t(-1) if there is no wdiffs.
     */
    uint64_t moveToDiffMemory() {
        uint64_t minAddr = -1; // max value.
        WdiffPtrList::iterator it = wdiffs_.begin();
        while (it != wdiffs_.end()) {
            bool goNext = true;
            Wdiff &wdiff = **it;
            const DiffRecord rec = wdiff.getFrontRec();
            if (rec.endIoAddress() <= minAddr) {
                DiffIo io;
                wdiff.getAndRemoveIo(io);
                mergeIo(rec, std::move(io));
                if (wdiff.isEnd()) {
                    statIn_.update(wdiff.getStat());
                    it = wdiffs_.erase(it);
                    goNext = false;
                }
            }
            minAddr = std::min(minAddr, wdiff.currentAddress());
            if (goNext) ++it;
        }
        return minAddr;
    }
    /**
     * Move all IOs which ioAddress + ioBlocks <= doneAddr
     * from diffMem_ to the mergedQ_.
     *
     * RETURN:
     *   false if there is no Io to move.
     */
    bool moveToMergedQueue(uint64_t doneAddr) {
        if (diffMem_.empty()) return false;
        DiffMemory::Map& map = diffMem_.getMap();
        DiffMemory::Map::iterator i = map.begin();
        while (i != map.end()) {
            DiffRecIo& recIo = i->second;
            if (recIo.record().endIoAddress() > doneAddr) break;
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
