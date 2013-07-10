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
            , isEnd_(false) {}

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
                DiffRecord rec(wdiffs_[i]->front());
                assert(rec.isValid());
                if (canMergeIo(i, rec)) {
                    DiffIo io;
                    wdiffs_[i]->pop(io);
                    assert(io.isValid());
                    mergeIo(rec, std::move(io), maxIoBlocks);
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
    void moveToQueueUpto(std::queue<DiffRecIo> &q, uint64_t maxAddr) {
        auto it = wdiffMem_.iterator();
        it.begin();
        while (it.isValid() && it.record().endIoAddress() <= maxAddr) {
            walb::diff::DiffRecIo r = std::move(it.recIo());
            assert(r.isValid());
            q.push(std::move(r));
            it.erase();
        }
    }

    /**
     * Write all wdiff IOs which ioAddress + ioBlocks <= maxAddr.
     */
    void writeDiffUpto(walb::diff::WalbDiffWriter &wdiffWriter, uint64_t maxAddr) {
        std::queue<DiffRecIo> q;
        moveToQueueUpto(q, maxAddr);
        while (!q.empty()) {
            DiffRecIo r = std::move(q.front());
            q.pop();
            assert(r.isValid());
            wdiffWriter.compressAndWriteDiff(r.record(), r.io());
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

    void mergeIo(const DiffRecord &rec, DiffIo &&io, uint16_t maxIoBlocks) {
        assert(!rec.isCompressed());
        wdiffMem_.add(rec, std::move(io), maxIoBlocks);
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
