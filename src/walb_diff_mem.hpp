#pragma once
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

namespace walb {

/**
 * Diff record and its IO data.
 * Data compression is not supported.
 * Checksum is not calculated.
 */
class DiffRecIo /* final */
{
private:
    DiffRecord rec_;
    DiffIo io_;
public:
    const DiffRecord &record() const { return rec_; }
    const DiffIo &io() const { return io_; }

    DiffRecIo() {}
    DiffRecIo(const DiffRecord &rec, DiffIo &&io)
        : rec_(rec) {
        if (rec.isNormal()) {
            io_ = std::move(io);
        } else {
            io_.clear();
        }
        assert(isValid());
    }
    DiffRecIo(const DiffRecord &rec, Buffer2 &&data)
        : rec_(rec) {
        if (rec.isNormal()) {
            io_.ioBlocks = rec.io_blocks;
            io_.compressionType = rec.compression_type;
            io_.data = std::move(data);
        } else {
            io_.clear();
        }
        assert(isValid());
    }
    bool isValid(bool isChecksum = false) const {
        if (!rec_.isNormal()) {
            if (io_.ioBlocks != 0) {
                LOGd("Fro non-normal record, io.ioBlocks must be 0.\n");
                return false;
            }
            return true;
        }
        if (rec_.io_blocks != io_.ioBlocks) {
            LOGd("ioSize invalid %u %u\n", rec_.io_blocks, io_.ioBlocks);
            return false;
        }
        if (rec_.data_size != io_.getSize()) {
            LOGd("dataSize invalid %" PRIu32 " %zu\n", rec_.data_size, io_.getSize());
            return false;
        }
        if (rec_.isCompressed()) {
            LOGd("DiffRecIo does not support compressed data.\n");
            return false;
        }
        if (isChecksum && rec_.checksum != io_.calcChecksum()) {
            LOGd("checksum invalid %0x %0x\n", rec_.checksum, io_.calcChecksum());
            return false;
        }
        return true;
    }

    void print(::FILE *fp = ::stdout) const {
        rec_.printOneline(fp);
        io_.printOneline(fp);
    }

    /**
     * Split the DiffRecIo into pieces
     * where each ioBlocks is <= a specified one.
     */
    std::vector<DiffRecIo> splitAll(uint16_t ioBlocks) const {
        assert(isValid());
        std::vector<DiffRecIo> v;

        std::vector<DiffRecord> recV = rec_.splitAll(ioBlocks);
        std::vector<DiffIo> ioV;
        if (rec_.isNormal()) {
            ioV = io_.splitIoDataAll(ioBlocks);
        } else {
            ioV.resize(recV.size());
        }
        assert(recV.size() == ioV.size());
        auto it0 = recV.begin();
        auto it1 = ioV.begin();
        while (it0 != recV.end() && it1 != ioV.end()) {
            v.emplace_back(*it0, std::move(*it1));
            ++it0;
            ++it1;
        }
        return v;
    }

    /**
     * Create (IO portions of rhs) - (that of *this).
     * If non-overlapped, throw runtime error.
     * The overlapped data of rhs will be used.
     * *this will not be changed.
     */
    std::vector<DiffRecIo> minus(const DiffRecIo &rhs) const {
        assert(isValid());
        assert(rhs.isValid());
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
            return v;
        }
        /*
         * Pattern 2:
         * oooooo + __xx__ = ooxxoo
         */
        if (rhs.rec_.isOverwrittenBy(rec_)) {
            uint16_t blks0 = rhs.rec_.io_address - rec_.io_address;
            uint16_t blks1 = rec_.endIoAddress() - rhs.rec_.endIoAddress();
            uint64_t addr0 = rec_.io_address;
            uint64_t addr1 = rec_.endIoAddress() - blks1;

            DiffRecord rec0 = rec_;
            DiffRecord rec1 = rec_;
            rec0.io_address = addr0;
            rec0.io_blocks = blks0;
            rec1.io_address = addr1;
            rec1.io_blocks = blks1;

            size_t size0 = 0;
            size_t size1 = 0;
			const bool recIsNormal = rec_.isNormal();
            if (recIsNormal) {
                size0 = blks0 * LOGICAL_BLOCK_SIZE;
                size1 = blks1 * LOGICAL_BLOCK_SIZE;
            }
            rec0.data_size = size0;
            rec1.data_size = size1;

            Buffer2 data0, data1;
            if (recIsNormal) {
                size_t off1 = (addr1 - rec_.io_address) * LOGICAL_BLOCK_SIZE;
                assert(size0 + rhs.rec_.io_blocks * LOGICAL_BLOCK_SIZE + size1 == rec_.data_size);
                const char *p = io_.get();
                util::assignAlignedArray(data0, p, size0);
                p += off1;
                util::assignAlignedArray(data1, p, size1);
            }

            if (0 < blks0) {
                v.emplace_back(rec0, std::move(data0));
            }
            if (0 < blks1) {
                v.emplace_back(rec1, std::move(data1));
            }
            return v;
        }
        /*
         * Pattern 3:
         * oooo__ + __xxxx = ooxxxx
         */
        if (rec_.io_address < rhs.rec_.io_address) {
            const uint64_t endIoAddr = rec_.endIoAddress();
            assert(rhs.rec_.io_address < endIoAddr);
            uint16_t rblks = endIoAddr - rhs.rec_.io_address;
            assert(rhs.rec_.io_address + rblks == endIoAddr);

            DiffRecord rec = rec_;
            /* rec.io_address does not change. */
            rec.io_blocks = rec_.io_blocks - rblks;
            assert(rec.endIoAddress() == rhs.rec_.io_address);

            size_t size = 0;
            if (rec_.isNormal()) {
                size = io_.getSize() - rblks * LOGICAL_BLOCK_SIZE;
            }
            Buffer2 data;
            if (rec_.isNormal()) {
                assert(rec_.data_size == io_.getSize());
                rec.data_size = size;
                const char *p = io_.get();
                util::assignAlignedArray(data, p, size);
            }

            v.emplace_back(rec, std::move(data));
            return v;
        }
        /*
         * Pattern 4:
         * __oooo + xxxx__ = xxxxoo
         */
        const uint64_t rhsEndIoAddr = rhs.rec_.endIoAddress();
        assert(rec_.io_address < rhsEndIoAddr);
        uint16_t rblks = rhsEndIoAddr - rec_.io_address;
        assert(rec_.io_address + rblks == rhsEndIoAddr);
        size_t off = rblks * LOGICAL_BLOCK_SIZE;

        DiffRecord rec = rec_;
        rec.io_address = rec_.io_address + rblks;
        rec.io_blocks = rec_.io_blocks - rblks;

        size_t size = 0;
        const bool isNormal = rec_.isNormal();
        if (isNormal) {
            size = io_.getSize() - off;
        }
        Buffer2 data;
        if (isNormal) {
            assert(rec_.data_size == io_.getSize());
            rec.data_size = size;
            const char *p = io_.get() + off;
            util::assignAlignedArray(data, p, size);
        }
        assert(rhsEndIoAddr == rec.io_address);
        v.emplace_back(rec, std::move(data));
        return v;
    }
};

/**
 * Simpler implementation of in-memory walb diff data.
 * IO data compression is not supported.
 * IO checksum is not calculated.
 */
class DiffMemory
{
public:
    using Map = std::map<uint64_t, DiffRecIo>;
private:
    const uint16_t maxIoBlocks_; /* All IOs must not exceed the size. */
    Map map_;
    DiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    explicit DiffMemory(uint16_t maxIoBlocks = uint16_t(-1))
        : maxIoBlocks_(maxIoBlocks), map_(), fileH_(), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~DiffMemory() noexcept = default;
    bool empty() const { return map_.empty(); }

    void add(const DiffRecord& rec, DiffIo &&io, uint16_t maxIoBlocks = 0) {
        /* Decide key range to search. */
        uint64_t addr0 = rec.io_address;
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
                nBlocks_ -= r.record().io_blocks;
                q.push(std::move(r));
                it = map_.erase(it);
            } else {
                ++it;
            }
        }
        /* Eliminate overlaps. */
        DiffRecIo r0(rec, std::move(io));
        while (!q.empty()) {
            std::vector<DiffRecIo> v = q.front().minus(r0);
            for (DiffRecIo &r : v) {
				const DiffRecord& dr = r.record();
                nIos_++;
                nBlocks_ += dr.io_blocks;
                map_.emplace(dr.io_address, std::move(r));
            }
            q.pop();
        }
        /* Insert the item. */
        nIos_++;
        nBlocks_ += r0.record().io_blocks;
        std::vector<DiffRecIo> rv;
        if (0 < maxIoBlocks && maxIoBlocks < rec.io_blocks) {
            rv = r0.splitAll(maxIoBlocks);
        } else if (maxIoBlocks_ < rec.io_blocks) {
            rv = r0.splitAll(maxIoBlocks_);
        } else {
            rv.push_back(std::move(r0));
        }
        for (DiffRecIo &r : rv) {
            uint64_t addr = r.record().io_address;
            uint16_t blks = r.record().io_blocks;
            map_.emplace(addr, std::move(r));
            fileH_.setMaxIoBlocksIfNecessary(blks);
        }
    }
    void print(::FILE *fp = ::stdout) const {
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const DiffRecord &rec = it->second.record();
            rec.printOneline(fp);
            ++it;
        }
    }
    uint64_t getNBlocks() const { return nBlocks_; }
    uint64_t getNIos() const { return nIos_; }
    void checkStatistics() const {
        uint64_t nBlocks = 0;
        uint64_t nIos = 0;
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const DiffRecord &rec = it->second.record();
            nBlocks += rec.io_blocks;
            nIos++;
            ++it;
        }
        if (nBlocks_ != nBlocks) {
            throw cybozu::Exception("DiffMemory:getNIos:bad blocks") << nBlocks_ << nBlocks;
        }
        if (nIos_ != nIos) {
            throw cybozu::Exception("DiffMemory:getNIos:bad ios") << nIos_ << nIos;
        }
    }
    DiffFileHeader& header() { return fileH_; }
    void writeTo(int outFd, bool isCompressed = true) {
        DiffWriter writer(outFd);
        writer.writeHeader(fileH_);
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const DiffRecIo &r = it->second;
            assert(r.isValid());
            if (isCompressed) {
                writer.compressAndWriteDiff(r.record(), r.io().get());
            } else {
                DiffRecord rec = r.record();
                rec.checksum = r.io().calcChecksum();
                writer.writeDiff(rec, r.io().get());
            }
            ++it;
        }
        writer.close();
    }
    void readFrom(int inFd) {
        DiffReader reader(inFd);
        reader.readHeader(fileH_);
        DiffRecord rec;
        DiffIo io;
        while (reader.readAndUncompressDiff(rec, io, false)) {
            add(rec, std::move(io));
        }
    }
    /**
     * Clear all data.
     */
    void clear() {
        map_.clear();
        nIos_ = 0;
        nBlocks_ = 0;
        fileH_.init();
    }
    void checkNoOverlappedAndSorted() const {
        auto it = map_.cbegin();
        const DiffRecord *prev = nullptr;
        while (it != map_.cend()) {
            const DiffRecord *curr = &it->second.record();
            if (prev) {
                if (!(prev->io_address < curr->io_address)) {
                    throw RT_ERR("Not sorted.");
                }
                if (!(prev->endIoAddress() <= curr->io_address)) {
                    throw RT_ERR("Overlapped records exist.");
                }
            }
            prev = curr;
            ++it;
        }
    }
    const Map& getMap() const { return map_; }
    Map& getMap() { return map_; }
	void eraseFromMap(Map::iterator& i)
	{
        nIos_--;
        nBlocks_ -= i->second.record().io_blocks;
        i = map_.erase(i);
        if (map_.empty()) {
            fileH_.resetMaxIoBlocks();
        }
	}
};

} //namespace walb
