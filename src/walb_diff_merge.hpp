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
    using DiffIo = walb::diff::IoData;
    using RecIo = walb::diff::RecIo;

    class Wdiff {
    private:
        std::string wdiffPath_;
        cybozu::util::FileOpener fop_;
        mutable walb::diff::Reader reader_;
        std::shared_ptr<walb::diff::FileHeaderWrap> headerP_;
        mutable walb_diff_record rec_;
        mutable DiffIo io_;
        mutable bool isFilled_;
        mutable bool isEnd_;
    public:
        explicit Wdiff(const std::string &wdiffPath)
            : wdiffPath_(wdiffPath)
            , fop_(wdiffPath, O_RDONLY)
            , reader_(fop_.fd())
            , headerP_(reader_.readHeader())
            , io_()
            , isFilled_(false)
            , isEnd_(false) {
			initRec(rec_);
        }
        /**
         * You must open the file before calling this constructor.
         */
        explicit Wdiff(cybozu::util::FileOpener &&fop)
            : wdiffPath_()
            , fop_(std::move(fop))
            , reader_(fop_.fd())
            , headerP_(reader_.readHeader())
            , io_()
            , isFilled_(false)
            , isEnd_(false) {
            initRec(rec_);
        }
        const std::string &path() const { return wdiffPath_; }
        walb::diff::Reader &reader() { return reader_; }
        walb::diff::FileHeaderWrap &header() { return *headerP_; }
        const walb_diff_record &front() {
            fill();
            assert(isFilled_);
            return rec_;
        }
        void pop(DiffIo &io) {
            if (isEnd()) return;

            /* for check */
            const uint64_t endIoAddr0 = endIoAddressRec(rec_);

            assert(isFilled_);
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
            assert(isFilled_);
            return rec_.io_address;
        }
    private:
        void fill() const {
            if (isEnd_ || isFilled_) return;
            if (reader_.readAndUncompressDiff(rec_, io_)) {
                isFilled_ = true;
            } else {
                clearExistsRec(rec_);
                io_ = DiffIo();
                isFilled_ = false;
                isEnd_ = true;
            }
        }
    };

    std::deque<std::shared_ptr<Wdiff> > wdiffs_;
    std::deque<std::shared_ptr<Wdiff> > doneWdiffs_;
    /* Wdiffs' lifetime must be the same as the Merger instance. */

    walb::diff::MemoryData wdiffMem_;
    struct walb_diff_file_header wdiffRawH_;
    FileHeaderWrap wdiffH_;
    bool isHeaderPrepared_;
    std::queue<RecIo> mergedQ_;
    bool shouldValidateUuid_;
    uint16_t maxIoBlocks_;
    uint64_t doneAddr_;

public:
    Merger()
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
    void addWdiffs(std::vector<cybozu::util::FileOpener> &&ops) {
        for (cybozu::util::FileOpener &op : ops) {
            wdiffs_.emplace_back(new Wdiff(std::move(op)));
        }
        ops.clear();
    }
    /**
     * Merge input wdiff files and put them into output fd.
     * The last wdiff's uuid will be used for output wdiff.
     *
     * @outFd file descriptor for output wdiff.
     */
    void mergeToFd(int outFd) {
        prepare();
        walb::diff::Writer writer(outFd);
        writer.writeHeader(wdiffH_);

        RecIo d;
        while (pop(d)) {
            assert(d.isValid());
            writer.compressAndWriteDiff(d.record2(), d.io().rawData());
        }

        writer.flush();
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
    const FileHeaderWrap &header() const {
        return wdiffH_;
    }
    /**
     * Pop a diffIo.
     * RETURN:
     *   false if there is no diffIo anymore.
     */
    bool pop(RecIo &recIo) {
        prepare();
        while (mergedQ_.empty()) {
            if (wdiffs_.empty()) {
                if (wdiffMem_.empty()) return false;
                moveToQueueUpto(uint64_t(-1));
                break;
            }
            for (size_t i = 0; i < wdiffs_.size(); i++) {
                assert(!wdiffs_[i]->isEnd());
				// copy rec because reference is invalid after calling wdiffs_[i]->pop
                const walb_diff_record rec = wdiffs_[i]->front();
                assert(isValidRec(rec));
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
        MemoryData::Map& map = wdiffMem_.getMap();
        auto i = map.begin();
        while (i != map.end()) {
            RecIo& recIo = i->second;
            if (endIoAddressRec(recIo.record2()) > maxAddr) break;
            mergedQ_.push(std::move(recIo));
            wdiffMem_.eraseMap(i);
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
    void mergeIo(const walb_diff_record &rec, DiffIo &&io) {
        assert(!isCompressedRec(rec));
        wdiffMem_.add(rec, std::move(io), maxIoBlocks_);
    }
    bool canMergeIo(size_t i, const walb_diff_record &rec) {
        if (i == 0) return true;
        for (size_t j = 0; j < i; j++) {
            if (!(endIoAddressRec(rec) <= wdiffs_[j]->currentAddress())) {
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
