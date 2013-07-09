/**
 * @file
 * @brief Merge several walb diff files to a wdiff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <deque>
#include <queue>
#include <map>
#include <string>
#include <memory>
#include <random>
#include <stdexcept>
#include <cstdint>
#include <cinttypes>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <type_traits>

#include "cybozu/option.hpp"

#include "util.hpp"
#include "walb_log.hpp"
#include "walb_diff.hpp"

#include <sys/types.h>

/**
 * To merge walb diff files.
 */
class WalbDiffMerger /* final */
{
private:
    using Block = std::shared_ptr<uint8_t>;
    using DiffRecord = walb::diff::WalbDiffRecord;
    using DiffRecordPtr = std::shared_ptr<DiffRecord>;
    using DiffIo = walb::diff::BlockDiffIo;
    using DiffIoPtr = std::shared_ptr<DiffIo>;
    using DiffData  = std::pair<DiffRecordPtr, DiffIoPtr>;

    class Wdiff {
    private:
        std::string wdiffPath_;
        mutable walb::diff::WalbDiffReader reader_;
        std::shared_ptr<walb::diff::WalbDiffFileHeader> headerP_;
        mutable DiffData buf_;
        mutable bool isEnd_;

    public:
        Wdiff(const std::string &wdiffPath)
            : wdiffPath_(wdiffPath), reader_(wdiffPath, O_RDONLY)
            , headerP_(reader_.readHeader())
            , buf_()
            , isEnd_(false) {}

        const std::string &path() const { return wdiffPath_; }

        walb::diff::WalbDiffReader &reader() { return reader_; }
        walb::diff::WalbDiffFileHeader &header() { return *headerP_; }

        DiffData &front() {
            if (!isEnd_) { fill(); }
            return buf_;
        }

        void pop() {
            if (isEnd_) { return; }

            /* for check */
            uint64_t endIoAddr = 0;
            if (isDiffData(buf_)) {
                DiffRecordPtr recp = buf_.first;
                endIoAddr = recp->endIoAddress();
            }

            fill();
            buf_ = reader_.readDiffAndUncompress();

            /* for check */
            if (isDiffData(buf_)) {
                DiffRecordPtr recp = buf_.first;
                if (!(endIoAddr <= recp->ioAddress())) {
                    throw RT_ERR("Invalid wdiff: IOs must be sorted and not overlapped each other.");
                }
            }
        }

        bool isEnd() const {
            if (isEnd_) { return true; }
            fill();
            isEnd_ = !isDiffData(buf_);
            return isEnd_;
        }

        /**
         * RETURN:
         *   address of the current head diff record
         *   if the iterator has not reached the end,
         *   or maximum value.
         */
        uint64_t currentAddress() const {
            if (isEnd()) { return uint64_t(-1); }
            DiffRecordPtr recp = buf_.first;
            return recp->ioAddress();
        }

    private:
        bool isDiffData(const DiffData &diffData) const {
            const DiffRecordPtr p = diffData.first;
            return p.get() != nullptr;
        }

        void fill() const {
            if (!isDiffData(buf_)) {
                buf_ = reader_.readDiffAndUncompress();
            }
        }
    };

    std::deque<std::shared_ptr<Wdiff> > wdiffs_;
    std::deque<std::shared_ptr<Wdiff> > doneWdiffs_;
    /* Wdiffs' lifetime must be the same as the WalbDiffMerger instance. */

    walb::diff::WalbDiffMemory wdiffMem_;

public:
    WalbDiffMerger() {}
    ~WalbDiffMerger() noexcept {}

    /**
     * Add diff files.
     *
     * Later wdiff file must be added later.
     */
    void addWdiff(const std::string& wdiffPath) {
        std::shared_ptr<Wdiff> p(new Wdiff(wdiffPath));
        wdiffs_.push_back(p);
    }

    /**
     * Merge input wdiff files.
     * The last wdiff's uuid will be used for output wdiff.
     *
     * @outFd file descriptor for output wdiff.
     * @shouldValidateUuid validate that all wdiff's uuid are the same if true,
     * @maxIoBlocks Max io blocks in the output wdiff [logical block].
     *     0 means no limitation.
     */
    void merge(int outFd, bool shouldValidateUuid, uint16_t maxIoBlocks = 0) {
        if (wdiffs_.empty()) {
            throw RT_ERR("Wdiff's is not set.");
        }
        walb::diff::WalbDiffWriter wdiffWriter(outFd);

        const uint8_t *uuid = wdiffs_.back()->header().getUuid();
        if (shouldValidateUuid) { checkUuid(uuid); }

        struct walb_diff_file_header headerT;
        ::memset(&headerT, 0, sizeof(headerT));
        walb::diff::WalbDiffFileHeader wdiffH(headerT);
        wdiffH.setUuid(uuid);
        wdiffH.setMaxIoBlocksIfNecessary(
            maxIoBlocks == 0 ? getMaxIoBlocks() : maxIoBlocks);
        wdiffWriter.writeHeader(wdiffH);

        uint64_t doneAddr = 0;
        removeEndedWdiffs();
        while (!wdiffs_.empty()) {
            for (size_t i = 0; i < wdiffs_.size(); i++) {
                assert(!wdiffs_[i]->isEnd());
                DiffRecordPtr recp;
                DiffIoPtr iop;
                std::tie(recp, iop) = wdiffs_[i]->front();
                if (canMergeIo(i, recp)) {
                    mergeIo(recp, iop, maxIoBlocks);
                    wdiffs_[i]->pop();
                }
            }
            removeEndedWdiffs();
            doneAddr = getMinCurrentAddress();
            writeDiffUpto(wdiffWriter, doneAddr);
        }
        writeDiffUpto(wdiffWriter, uint64_t(-1));
        wdiffWriter.flush();
        assert(wdiffMem_.empty());
    }
private:
    /**
     * Move all IOs which ioAddress + ioBlocks <= maxAddr
     * to a specified queue.
     */
    void moveToQueueUpto(std::queue<DiffData> &q, uint64_t maxAddr) {
        auto it = wdiffMem_.iterator();
        it.begin();
        while (it.isValid() && it.record().endIoAddress() <= maxAddr) {
            auto recp = std::make_shared<DiffRecord>(it.record());
            auto iop = std::make_shared<DiffIo>(recp->ioBlocks());
            iop->moveFrom(it.recData().forMove());
            q.push(std::make_pair(recp, iop));
            it.erase();
            it.next();
        }
    }

    /**
     * Write all wdiff IOs which ioAddress + ioBlocks <= maxAddr.
     */
    void writeDiffUpto(walb::diff::WalbDiffWriter &wdiffWriter, uint64_t maxAddr) {
        std::queue<DiffData> q;
        moveToQueueUpto(q, maxAddr);
        while (!q.empty()) {
            DiffRecordPtr recp;
            DiffIoPtr iop0, iop1;
            std::tie(recp, iop0) = q.front();
            q.pop();
            if (recp->isNormal()) {
                iop1 = std::make_shared<DiffIo>(recp->ioBlocks(), ::WALB_DIFF_CMPR_SNAPPY);
                *iop1 = iop0->compress(::WALB_DIFF_CMPR_SNAPPY);
                recp->setCompressionType(::WALB_DIFF_CMPR_SNAPPY);
                recp->setDataSize(iop1->rawSize());
                recp->setChecksum(iop1->calcChecksum());
            } else {
                iop1 = iop0;
            }
            wdiffWriter.writeDiff(*recp, iop1);
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

    void mergeIo(DiffRecordPtr recp, DiffIoPtr iop, uint16_t maxIoBlocks) {
        assert(!recp->isCompressed());
        wdiffMem_.add(*recp, iop, maxIoBlocks);
    }

    bool canMergeIo(size_t i, DiffRecordPtr recp) {
        if (i == 0) { return true; }
        assert(recp);
        uint64_t endIoAddr = recp->endIoAddress();
        for (size_t j = 0; j < i; j++) {
            if (!(endIoAddr <= wdiffs_[j]->currentAddress())) {
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

struct Option : public cybozu::Option
{
    uint32_t maxIoSize;
    std::vector<std::string> inputWdiffs;
    std::string outputWdiff;
    Option() {
        setUsage("Usage: wdiff-merge (options) -i [input wdiffs] -o [merged wdiff]");
        appendOpt(&maxIoSize, 0, "x", "max IO size [byte]. 0 means no limitation.");
        appendVec(&inputWdiffs, "i", "Input wdiff paths.");
        appendMust(&outputWdiff, "o", "Output wdiff path.");
        appendHelp("h");
    }

    uint16_t maxIoBlocks() const {
        if (uint16_t(-1) < maxIoSize) {
            throw RT_ERR("Max IO size must be less than 32M.");
        }
        return maxIoSize / LOGICAL_BLOCK_SIZE;
    }

    bool parse(int argc, char *argv[]) {
        if (!cybozu::Option::parse(argc, argv)) {
            goto error;
        }
        if (inputWdiffs.empty()) {
            ::printf("You must specify one or more input wdiff files.\n");
            goto error;
        }
        return true;
      error:
        usage();
        return false;
    }
};

int main(int argc, char *argv[])
{
    try {
        Option opt;
        if (!opt.parse(argc, argv)) {
            return 1;
        }
        WalbDiffMerger merger;
        for (std::string &path : opt.inputWdiffs) {
            merger.addWdiff(path);
        }
        cybozu::util::FileOpener fo(opt.outputWdiff, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        merger.merge(fo.fd(), false, opt.maxIoBlocks());
        fo.close();
        return 0;
    } catch (std::runtime_error &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
        return 1;
    }
}

/* end of file. */
