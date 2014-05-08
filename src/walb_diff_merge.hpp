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
#include <deque>
#include <cassert>
#include <cstring>

#include "walb_diff_base.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_mem.hpp"
#include "fileio.hpp"

namespace walb {
namespace diff {

/**
 * To merge walb diff files.
 */
class Merger /* final */
{
private:
    class Wdiff {
    private:
        const std::string wdiffPath_;
        cybozu::util::File file_;
        mutable Reader reader_;
        DiffFileHeader header_;
        mutable DiffRecord rec_;
        mutable DiffIo io_;
        mutable bool isFilled_;
        mutable bool isEnd_;
    public:
        explicit Wdiff(const std::string &wdiffPath)
            : wdiffPath_(wdiffPath)
            , file_(wdiffPath, O_RDONLY)
            , reader_(file_.fd())
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
            , file_(std::move(file))
            , reader_(file_.fd())
            , rec_()
            , io_()
            , isFilled_(false)
            , isEnd_(false) {
            reader_.readHeader(header_);
        }
        const std::string &path() const { return wdiffPath_; }
        const DiffFileHeader &header() { return header_; }
        const DiffRecord &front() {
            verifyNotEnd(__func__);
            fill();
            return rec_;
        }
        void getAndRemove(DiffIo &io) {
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
         *   address of the current head diff record
         *   if the iterator has not reached the end,
         *   or maximum value.
         */
        uint64_t currentAddress() const {
            if (isEnd()) return uint64_t(-1);
            verifyFilled(__func__);
            return rec_.io_address;
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
        void verifyNotEnd(const char *msg) const {
            if (isEnd()) throw cybozu::Exception(msg) << "reached to end";
        }
        void verifyFilled(const char *msg) const {
            if (!isFilled_) throw cybozu::Exception(msg) << "not filled";
        }
    };

    using WdiffPtr = std::unique_ptr<Wdiff>;
    using WdiffPtrDeq = std::deque<WdiffPtr>;
    WdiffPtrDeq wdiffs_;
    WdiffPtrDeq doneWdiffs_;
    /* Wdiffs' lifetime must be the same as the Merger instance. */

    DiffMemory diffMem_;
    DiffFileHeader wdiffH_;
    bool isHeaderPrepared_;
    std::queue<RecIo> mergedQ_;
    bool shouldValidateUuid_;
    uint16_t maxIoBlocks_;
    uint64_t doneAddr_;

public:
    Merger()
        : wdiffs_()
        , doneWdiffs_()
        , diffMem_()
        , wdiffH_()
        , isHeaderPrepared_(false)
        , mergedQ_()
        , shouldValidateUuid_(false)
        , maxIoBlocks_(0)
        , doneAddr_(0) {
    }
    ~Merger() noexcept {
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
    void addWdiffs(const std::vector<std::string> &wdiffPaths) {
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
        Writer writer(outFd);
        writer.writeHeader(wdiffH_);

        RecIo d;
        while (getAndRemove(d)) {
            assert(d.isValid());
            writer.compressAndWriteDiff(d.record(), d.io().get());
        }

        writer.flush();
        assert(wdiffs_.empty());
        assert(diffMem_.empty());
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
            if (shouldValidateUuid_) { verifyUuid(uuid); }

            wdiffH_.init();
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
    const DiffFileHeader &header() const {
        return wdiffH_;
    }
    /**
     * Get a diffIo and remove.
     * RETURN:
     *   false if there is no diffIo anymore.
     */
    bool getAndRemove(RecIo &recIo) {
        prepare();
        while (mergedQ_.empty()) {
            if (wdiffs_.empty()) {
                if (diffMem_.empty()) return false;
                moveToQueueUpto(uint64_t(-1));
                break;
            }
            for (size_t i = 0; i < wdiffs_.size(); i++) {
                assert(!wdiffs_[i]->isEnd());
				// copy rec because reference is invalid after calling wdiffs_[i]->getAndRemove().
                const DiffRecord rec = wdiffs_[i]->front();
                assert(rec.isValid());
                if (canMergeIo(i, rec)) {
                    DiffIo io;
                    wdiffs_[i]->getAndRemove(io);
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
        DiffMemory::Map& map = diffMem_.getMap();
        auto i = map.begin();
        while (i != map.end()) {
            RecIo& recIo = i->second;
            if (recIo.record().endIoAddress() > maxAddr) break;
            mergedQ_.push(std::move(recIo));
            diffMem_.eraseMap(i);
        }
    }
    uint64_t getMinCurrentAddress() const {
        uint64_t minAddr = uint64_t(-1);
        for (const WdiffPtr &wdiffP : wdiffs_) {
            const uint64_t addr = wdiffP->currentAddress();
            if (addr < minAddr) { minAddr = addr; }
        }
        return minAddr;
    }
    void removeEndedWdiffs() {
        WdiffPtrDeq::iterator it = wdiffs_.begin();
        while (it != wdiffs_.end()) {
            WdiffPtr &p = *it;
            if (p->isEnd()) {
                doneWdiffs_.push_back(std::move(p));
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
    bool canMergeIo(size_t i, const DiffRecord &rec) {
        if (i == 0) return true;
        for (size_t j = 0; j < i; j++) {
            if (!(rec.endIoAddress() <= wdiffs_[j]->currentAddress())) {
                return false;
            }
        }
        return true;
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
            if (ret < m) { ret = m; }
        }
        return ret;
    }
};

}} //namespace walb::diff
