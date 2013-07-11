/**
 * @file
 * @brief walb diff in main memory.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <cassert>
#include <map>
#include "walb_diff_base.hpp"
#include "walb_diff_file.hpp"

#ifndef WALB_DIFF_MEM_HPP
#define WALB_DIFF_MEM_HPP

namespace walb {
namespace diff {

/**
 * Diff record and its IO data.
 * Data compression is not supported.
 */
class DiffRecIo /* final */
{
private:
    WalbDiffRecord rec_;
    BlockDiffIo io_;
public:
    struct walb_diff_record &rawRecord() { return *rec_.rawRecord(); }
    const struct walb_diff_record &rawRecord() const { return *rec_.rawRecord(); }
    WalbDiffRecord &record() { return rec_; }
    const WalbDiffRecord &record() const { return rec_; }

    BlockDiffIo &io() { return io_; }
    const BlockDiffIo &io() const { return io_; }

    void copyFrom(const WalbDiffRecord &rec, const BlockDiffIo &io) {
        rec_ = rec;
        if (rec.isNormal()) {
            io_.setIoBlocks(io.ioBlocks());
            io_.setCompressionType(io.compressionType());
            io_.data().resize(io.data().size());
            ::memcpy(io_.rawData(), io.rawData(), io.rawSize());
        } else {
            io_ = BlockDiffIo();
        }
    }
    void moveFrom(const WalbDiffRecord &rec, BlockDiffIo &&io) {
        rec_ = rec;
        if (rec.isNormal()) {
            io_ = std::move(io);
        } else {
            io_ = BlockDiffIo();
        }
    }
    void moveFrom(const WalbDiffRecord &rec, std::vector<char> &&data) {
        rec_ = rec;
        if (rec.isNormal()) {
            io_.setIoBlocks(rec.ioBlocks());
            io_.setCompressionType(rec.compressionType());
            io_.data() = std::move(data);
        } else {
            io_ = BlockDiffIo();
        }
    }

    void updateChecksum() {
        rec_.setChecksum(io_.calcChecksum());
    }

    bool isValid(bool isChecksum = false) const {
        if (!rec_.isValid()) {
            LOGd("rec is not valid.\n");
            return false;
        }
        if (!io_.isValid()) {
            LOGd("io is not valid.\n");
            return false;
        }
        if (!rec_.isNormal()) {
            if (io_.ioBlocks() != 0) {
                LOGd("Fro non-normal record, io.ioBlocks must be 0.\n");
                return false;
            }
            return true;
        }
        if (rec_.ioBlocks() != io_.ioBlocks()) {
            LOGd("ioSize invalid %u %u\n", rec_.ioBlocks(), io_.ioBlocks());
            return false;
        }
        if (rec_.dataSize() != io_.rawSize()) {
            LOGd("dataSize invalid %" PRIu32 " %zu\n", rec_.dataSize(), io_.rawSize());
            return false;
        }
        if (rec_.isCompressed()) {
            LOGd("DiffRecIo does not support compressed data.\n");
            return false;
        }
        if (isChecksum && rec_.checksum() != io_.calcChecksum()) {
            LOGd("checksum invalid %0x %0x\n", rec_.checksum(), io_.calcChecksum());
            return false;
        }
        return true;
    }

    void print(::FILE *fp) const {
        rec_.printOneline(fp);
        io_.printOneline(fp);
    }

    void print() const { print(::stdout); }

    /**
     * Split the DiffRecIo into pieces
     * where each ioBlocks is <= a specified one.
     */
    std::vector<DiffRecIo> splitAll(uint16_t ioBlocks) const {
        assert(isValid());
        std::vector<DiffRecIo> v;

        std::vector<WalbDiffRecord> recV = rec_.splitAll(ioBlocks);
        std::vector<BlockDiffIo> ioV;
        if (rec_.isNormal()) {
            ioV = io_.splitAll(ioBlocks);
        } else {
            ioV.resize(recV.size());
        }
        assert(recV.size() == ioV.size());
        auto it0 = recV.begin();
        auto it1 = ioV.begin();
        while (it0 != recV.end() && it1 != ioV.end()) {
            DiffRecIo r;
            r.moveFrom(*it0, std::move(*it1));
            r.updateChecksum();
            assert(r.isValid());
            v.push_back(std::move(r));
            ++it0;
            ++it1;
        }
        return std::move(v);
    }

    /**
     * Create (IO portions of rhs) - (that of *this).
     * If non-overlapped, throw runtime error.
     * The overlapped data of rhs will be used.
     * *this will not be changed.
     */
    std::vector<DiffRecIo> minus(const DiffRecIo &rhs) const {
        assert(isValid(true));
        assert(rhs.isValid(true));
        if (!rec_.isOverlapped(rhs.rec_)) {
            throw RT_ERR("Non-overlapped.");
        }
        std::vector<DiffRecIo> v;
        /*
         * Pattern 1:
         * __oo__ + xxxxxx = xxxxxx
         */
        if (rec_.isOverwrittenBy(rhs.rec_)) {
            /* Empty */
            return std::move(v);
        }
        /*
         * Pattern 2:
         * oooooo + __xx__ = ooxxoo
         */
        if (rhs.rec_.isOverwrittenBy(rec_)) {
            uint16_t blks0 = rhs.rec_.ioAddress() - rec_.ioAddress();
            uint16_t blks1 = rec_.endIoAddress() - rhs.rec_.endIoAddress();
            uint64_t addr0 = rec_.ioAddress();
            uint64_t addr1 = rec_.endIoAddress() - blks1;

            WalbDiffRecord rec0(rec_), rec1(rec_);
            rec0.setIoAddress(addr0);
            rec0.setIoBlocks(blks0);
            rec1.setIoAddress(addr1);
            rec1.setIoBlocks(blks1);

            size_t size0 = 0;
            size_t size1 = 0;
            if (rec_.isNormal()) {
                size0 = blks0 * LOGICAL_BLOCK_SIZE;
                size1 = blks1 * LOGICAL_BLOCK_SIZE;
            }
            rec0.setDataSize(size0);
            rec1.setDataSize(size1);

            std::vector<char> data0(size0), data1(size1);
            if (rec_.isNormal()) {
                size_t off1 = (addr1 - rec_.ioAddress()) * LOGICAL_BLOCK_SIZE;
                assert(size0 + rhs.rec_.ioBlocks() * LOGICAL_BLOCK_SIZE + size1 == rec_.dataSize());
                ::memcpy(&data0[0], io_.rawData(), size0);
                ::memcpy(&data1[0], io_.rawData() + off1, size1);
            }

            if (0 < blks0) {
                DiffRecIo r;
                r.moveFrom(rec0, std::move(data0));
                r.updateChecksum();
                assert(r.isValid());
                v.push_back(std::move(r));
            }
            if (0 < blks1) {
                DiffRecIo r;
                r.moveFrom(rec1, std::move(data1));
                r.updateChecksum();
                assert(r.isValid());
                v.push_back(std::move(r));
            }
            return std::move(v);
        }
        /*
         * Pattern 3:
         * oooo__ + __xxxx = ooxxxx
         */
        if (rec_.ioAddress() < rhs.rec_.ioAddress()) {
            assert(rhs.rec_.ioAddress() < rec_.endIoAddress());
            uint16_t rblks = rec_.endIoAddress() - rhs.rec_.ioAddress();
            assert(rhs.rec_.ioAddress() + rblks == rec_.endIoAddress());

            WalbDiffRecord rec(rec_);
            /* rec.ioAddress() does not change. */
            rec.setIoBlocks(rec_.ioBlocks() - rblks);
            assert(rec.endIoAddress() == rhs.rec_.ioAddress());

            size_t size = 0;
            if (rec_.isNormal()) {
                size = io_.rawSize() - rblks * LOGICAL_BLOCK_SIZE;
            }
            std::vector<char> data(size);
            if (rec_.isNormal()) {
                assert(rec_.dataSize() == io_.rawSize());
                rec.setDataSize(size);
                ::memcpy(&data[0], io_.rawData(), size);
            }

            DiffRecIo r;
            r.moveFrom(rec, std::move(data));
            r.updateChecksum();
            assert(r.isValid());
            v.push_back(std::move(r));
            return std::move(v);
        }
        /*
         * Pattern 4:
         * __oooo + xxxx__ = xxxxoo
         */
        assert(rec_.ioAddress() < rhs.rec_.endIoAddress());
        uint16_t rblks = rhs.rec_.endIoAddress() - rec_.ioAddress();
        assert(rec_.ioAddress() + rblks == rhs.rec_.endIoAddress());
        size_t off = rblks * LOGICAL_BLOCK_SIZE;

        WalbDiffRecord rec(rec_);
        rec.setIoAddress(rec_.ioAddress() + rblks);
        rec.setIoBlocks(rec_.ioBlocks() - rblks);

        size_t size = 0;
        if (rec_.isNormal()) {
            size = io_.rawSize() - off;
        }
        std::vector<char> data(size);
        if (rec_.isNormal()) {
            assert(rec_.dataSize() == io_.rawSize());
            rec.setDataSize(size);
            ::memcpy(&data[0], io_.rawData() + off, size);
        }
        assert(rhs.rec_.endIoAddress() == rec.ioAddress());
        DiffRecIo r;
        r.moveFrom(rec, std::move(data));
        r.updateChecksum();
        assert(r.isValid());
        v.push_back(std::move(r));
        return std::move(v);
    }
};

/**
 * Simpler implementation of in-memory walb diff data.
 * IO data compression is not supported.
 */
class WalbDiffMemory
{
private:
    const uint16_t maxIoBlocks_; /* All IOs must not exceed the size. */
    std::map<uint64_t, DiffRecIo> map_;
    struct walb_diff_file_header h_;
    WalbDiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    explicit WalbDiffMemory(uint16_t maxIoBlocks = uint16_t(-1))
        : maxIoBlocks_(maxIoBlocks), map_(), h_(), fileH_(h_), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~WalbDiffMemory() noexcept = default;
    bool init() {
        /* Initialize always. */
        return false;
    }
    bool empty() const { return map_.empty(); }

    void add(const WalbDiffRecord &rec, const BlockDiffIo &io, uint16_t maxIoBlocks = 0) {
        add(rec, BlockDiffIo(io), maxIoBlocks);
    }
    void add(const WalbDiffRecord &rec, BlockDiffIo &&io, uint16_t maxIoBlocks = 0) {
        /* Decide key range to search. */
        uint64_t addr0 = rec.ioAddress();
        if (addr0 <= fileH_.getMaxIoBlocks()) {
            addr0 = 0;
        } else {
            addr0 -= fileH_.getMaxIoBlocks();
        }
        /* Search overlapped items. */
        uint64_t addr1 = rec.endIoAddress();
        std::queue<DiffRecIo> q;
        auto it = map_.lower_bound(addr0);
        while (it != map_.end() && it->first < addr1) {
            DiffRecIo &r = it->second;
            if (r.record().isOverlapped(rec)) {
                nIos_--;
                nBlocks_ -= r.record().ioBlocks();
                q.push(std::move(r));
                it = map_.erase(it);
            } else {
                ++it;
            }
        }
        /* Eliminate overlaps. */
        DiffRecIo r0;
        r0.moveFrom(rec, std::move(io));
        assert(r0.isValid());
        while (!q.empty()) {
            std::vector<DiffRecIo> v = q.front().minus(r0);
            for (DiffRecIo &r : v) {
                nIos_++;
                nBlocks_ += r.record().ioBlocks();
                uint64_t addr = r.record().ioAddress();
                map_.insert(std::make_pair(addr, std::move(r)));
            }
            q.pop();
        }
        /* Insert the item. */
        nIos_++;
        nBlocks_ += r0.record().ioBlocks();
        std::vector<DiffRecIo> rv;
        if (0 < maxIoBlocks && maxIoBlocks < rec.ioBlocks()) {
            rv = r0.splitAll(maxIoBlocks);
        } else if (maxIoBlocks_ < rec.ioBlocks()) {
            rv = r0.splitAll(maxIoBlocks_);
        } else {
            rv.push_back(std::move(r0));
        }
        for (DiffRecIo &r : rv) {
            uint64_t addr = r.record().ioAddress();
            uint16_t blks = r.record().ioBlocks();
            map_.insert(std::make_pair(addr, std::move(r)));
            fileH_.setMaxIoBlocksIfNecessary(blks);
        }
    }
    bool sync() {
        /* do nothing. */
        return true;
    }
    void print(::FILE *fp) const {
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const WalbDiffRecord &rec = it->second.record();
            rec.printOneline(fp);
            ++it;
        }
    }
    void print() const { print(::stdout); }
    uint64_t getNBlocks() const { return nBlocks_; }
    uint64_t getNIos() const { return nIos_; }
    void checkStatistics() const {
        uint64_t nBlocks = 0;
        uint64_t nIos = 0;
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const WalbDiffRecord &rec = it->second.record();
            nBlocks += rec.ioBlocks();
            nIos++;
            ++it;
        }
        if (nBlocks_ != nBlocks) {
            throw RT_ERR("nBlocks_ %" PRIu64 " nBlocks %" PRIu64 "\n",
                         nBlocks_, nBlocks);
        }
        if (nIos_ != nIos) {
            throw RT_ERR("nIos_ %" PRIu64 " nIos %" PRIu64 "\n",
                         nIos_, nIos);
        }
    }
    WalbDiffFileHeader& header() { return fileH_; }
    bool writeTo(int outFd, bool isCompressed = true) {
        WalbDiffWriter writer(outFd);
        writer.writeHeader(fileH_);
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const DiffRecIo &r = it->second;
            assert(r.isValid());
            if (isCompressed) {
                writer.compressAndWriteDiff(r.record(), r.io());
            } else {
                auto iop = std::make_shared<BlockDiffIo>();
                if (r.record().isNormal()) {
                    *iop = r.io();
                }
                writer.writeDiff(r.record(), iop);
            }
            ++it;
        }
        writer.flush();
        return true;
    }
    void checkNoOverlappedAndSorted() const {
        auto it = map_.cbegin();
        const WalbDiffRecord *prev = nullptr;
        while (it != map_.cend()) {
            const WalbDiffRecord *curr = &it->second.record();
            if (prev) {
                if (!(prev->ioAddress() < curr->ioAddress())) {
                    throw RT_ERR("Not sorted.");
                }
                if (!(prev->endIoAddress() <= curr->ioAddress())) {
                    throw RT_ERR("Overlapped records exist.");
                }
            }
            prev = curr;
            ++it;
        }
    }

    using Map = std::map<uint64_t, DiffRecIo>;
    template <typename DiffMemory, typename MapIterator>
    class IteratorBase {
    protected:
        DiffMemory *mem_;
        MapIterator it_;
    public:
        explicit IteratorBase(DiffMemory *mem)
            : mem_(mem)
            , it_() {
            assert(mem);
        }
        IteratorBase(const IteratorBase &rhs)
            : mem_(rhs.mem_)
            , it_(rhs.it_) {
        }
        virtual ~IteratorBase() noexcept = default;
        IteratorBase &operator=(const IteratorBase &rhs) {
            mem_ = rhs.mem_;
            it_ = rhs.it_;
            return *this;
        }
        bool isValid() const { return it_ != mem_->map_.end(); }
        void begin() { it_ = mem_->map_.begin(); }
        void end() { it_ = mem_->map_.end(); }
        void next() { ++it_; }
        void prev() { --it_; }
        void lowerBound(uint64_t addr) {
            it_ = mem_->map_.lower_bound(addr);
        }
        void upperBound(uint64_t addr) {
            it_ = mem_->map_.upper_bound(addr);
        }
        WalbDiffRecord &record() {
            checkValid();
            return it_->second.record();
        }
        const WalbDiffRecord &record() const {
            checkValid();
            return it_->second.record();
        }
        char *rawData() {
            checkValid();
            return it_->second.rawData();
        }
        const char *rawData() const {
            checkValid();
            return it_->second.rawData();
        }
        uint32_t rawSize() const {
            checkValid();
            return it_->second.rawSize();
        }
    protected:
        void checkValid() const {
            if (!isValid()) {
                throw RT_ERR("Invalid iterator position.");
            }
        }
    };
    class ConstIterator
        : public IteratorBase<const WalbDiffMemory, Map::iterator>
    {
    public:
        explicit ConstIterator(const WalbDiffMemory *mem)
            : IteratorBase<const WalbDiffMemory, Map::iterator>(mem) {
        }
        ConstIterator(const ConstIterator &rhs)
            : IteratorBase<const WalbDiffMemory, Map::iterator>(rhs) {
        }
        ~ConstIterator() noexcept override = default;

    };
    class Iterator
        : public IteratorBase<WalbDiffMemory, Map::iterator>
    {
    public:
        explicit Iterator(WalbDiffMemory *mem)
            : IteratorBase<WalbDiffMemory, Map::iterator>(mem) {
        }
        Iterator(const Iterator &rhs)
            : IteratorBase<WalbDiffMemory, Map::iterator>(rhs) {
        }
        ~Iterator() noexcept override = default;
        /**
         * Erase the item on the iterator.
         * The iterator will indicate the next of the removed item.
         */
        void erase() {
            checkValid();
            mem_->nIos_--;
            mem_->nBlocks_ -= it_->second.record().ioBlocks();
            it_ = mem_->map_.erase(it_);
            if (mem_->map_.empty()) {
                mem_->fileH_.resetMaxIoBlocks();
            }
        }
        DiffRecIo &recIo() {
            return it_->second;
        }
    };
    Iterator iterator() {
        return Iterator(this);
    }
    ConstIterator constIterator() const {
        return ConstIterator(this);
    }
};

}} //namespace walb::diff

#endif /* WALB_DIFF_MEM_HPP */
