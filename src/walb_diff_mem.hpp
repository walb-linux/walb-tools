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
namespace diff {

/**
 * Diff record and its IO data.
 * Data compression is not supported.
 */
class RecIo /* final */
{
private:
    RecordRaw rec_;
    IoData io_;
public:
    RecordRaw &record() { return rec_; }
    const walb_diff_record &record2() const { return rec_.record(); }

    IoData &io() { return io_; }
    const IoData &io() const { return io_; }

    void copyFrom(const walb_diff_record &rec, const IoData &io) {
        rec_ = rec;
        if (isNormalRec(rec)) {
            io_ = io;
        } else {
            io_.clear();
        }
    }
    void moveFrom(const walb_diff_record &rec, IoData &&io) {
        rec_ = rec;
        if (isNormalRec(rec)) {
            io_ = std::move(io);
        } else {
            io_.clear();
        }
    }
    void moveFrom(const walb_diff_record &rec, std::vector<char> &&data) {
        rec_ = rec;
        if (isNormalRec(rec)) {
            io_.ioBlocks = rec.io_blocks;
            io_.compressionType = rec.compression_type;
            io_.moveFrom(std::move(data));
        } else {
            io_.clear();
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
            if (io_.ioBlocks != 0) {
                LOGd("Fro non-normal record, io.ioBlocks must be 0.\n");
                return false;
            }
            return true;
        }
        if (rec_.ioBlocks() != io_.ioBlocks) {
            LOGd("ioSize invalid %u %u\n", rec_.ioBlocks(), io_.ioBlocks);
            return false;
        }
        if (rec_.dataSize() != io_.size) {
            LOGd("dataSize invalid %" PRIu32 " %zu\n", rec_.dataSize(), io_.size);
            return false;
        }
        if (rec_.isCompressed()) {
            LOGd("RecIo does not support compressed data.\n");
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
     * Split the RecIo into pieces
     * where each ioBlocks is <= a specified one.
     */
    std::vector<RecIo> splitAll(uint16_t ioBlocks) const {
        assert(isValid());
        std::vector<RecIo> v;

        std::vector<walb_diff_record> recV = diff::splitAll(rec_.record(), ioBlocks);
        std::vector<IoData> ioV;
        if (rec_.isNormal()) {
            ioV = splitIoDataAll(io_, ioBlocks);
        } else {
            ioV.resize(recV.size());
        }
        assert(recV.size() == ioV.size());
        auto it0 = recV.begin();
        auto it1 = ioV.begin();
        while (it0 != recV.end() && it1 != ioV.end()) {
            RecIo r;
            r.moveFrom(*it0, std::move(*it1));
            r.updateChecksum();
            assert(r.isValid());
            v.push_back(std::move(r));
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
    std::vector<RecIo> minus(const RecIo &rhs) const {
        assert(isValid(true));
        assert(rhs.isValid(true));
        if (!rec_.isOverlapped(rhs.rec_)) {
            throw RT_ERR("Non-overlapped.");
        }
        std::vector<RecIo> v;
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
            uint16_t blks0 = rhs.rec_.ioAddress() - rec_.ioAddress();
            uint16_t blks1 = rec_.endIoAddress() - rhs.rec_.endIoAddress();
            uint64_t addr0 = rec_.ioAddress();
            uint64_t addr1 = rec_.endIoAddress() - blks1;

            walb_diff_record rec0 = rec_.record();
            walb_diff_record rec1 = rec_.record();
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

            std::vector<char> data0(size0), data1(size1);
            if (recIsNormal) {
                size_t off1 = (addr1 - rec_.ioAddress()) * LOGICAL_BLOCK_SIZE;
                assert(size0 + rhs.rec_.ioBlocks() * LOGICAL_BLOCK_SIZE + size1 == rec_.dataSize());
                ::memcpy(&data0[0], io_.rawData(), size0);
                ::memcpy(&data1[0], io_.rawData() + off1, size1);
            }

            if (0 < blks0) {
                RecIo r;
                r.moveFrom(rec0, std::move(data0));
                r.updateChecksum();
                assert(r.isValid());
                v.push_back(std::move(r));
            }
            if (0 < blks1) {
                RecIo r;
                r.moveFrom(rec1, std::move(data1));
                r.updateChecksum();
                assert(r.isValid());
                v.push_back(std::move(r));
            }
            return v;
        }
        /*
         * Pattern 3:
         * oooo__ + __xxxx = ooxxxx
         */
        if (rec_.ioAddress() < rhs.rec_.ioAddress()) {
            assert(rhs.rec_.ioAddress() < rec_.endIoAddress());
            uint16_t rblks = rec_.endIoAddress() - rhs.rec_.ioAddress();
            assert(rhs.rec_.ioAddress() + rblks == rec_.endIoAddress());

            walb_diff_record rec = rec_.record();
            /* rec.ioAddress() does not change. */
            rec.io_blocks = rec_.ioBlocks() - rblks;
            assert(endIoAddressRec(rec) == rhs.rec_.ioAddress());

            size_t size = 0;
            if (rec_.isNormal()) {
                size = io_.size - rblks * LOGICAL_BLOCK_SIZE;
            }
            std::vector<char> data(size);
            if (rec_.isNormal()) {
                assert(rec_.dataSize() == io_.size);
                rec.data_size = size;
                ::memcpy(&data[0], io_.rawData(), size);
            }

            RecIo r;
            r.moveFrom(rec, std::move(data));
            r.updateChecksum();
            assert(r.isValid());
            v.push_back(std::move(r));
            return v;
        }
        /*
         * Pattern 4:
         * __oooo + xxxx__ = xxxxoo
         */
        assert(rec_.ioAddress() < rhs.rec_.endIoAddress());
        uint16_t rblks = rhs.rec_.endIoAddress() - rec_.ioAddress();
        assert(rec_.ioAddress() + rblks == rhs.rec_.endIoAddress());
        size_t off = rblks * LOGICAL_BLOCK_SIZE;

        walb_diff_record rec = rec_.record();
        rec.io_address = rec_.ioAddress() + rblks;
        rec.io_blocks = rec_.ioBlocks() - rblks;

        size_t size = 0;
        if (rec_.isNormal()) {
            size = io_.size - off;
        }
        std::vector<char> data(size);
        if (rec_.isNormal()) {
            assert(rec_.dataSize() == io_.size);
            rec.data_size = size;
            ::memcpy(&data[0], io_.rawData() + off, size);
        }
        assert(rhs.rec_.endIoAddress() == rec.io_address);
        RecIo r;
        r.moveFrom(rec, std::move(data));
        r.updateChecksum();
        assert(r.isValid());
        v.push_back(std::move(r));
        return v;
    }
};

/**
 * Simpler implementation of in-memory walb diff data.
 * IO data compression is not supported.
 */
class MemoryData
{
public:
    using Map = std::map<uint64_t, RecIo>;
private:
    const uint16_t maxIoBlocks_; /* All IOs must not exceed the size. */
    Map map_;
    struct walb_diff_file_header h_;
    FileHeaderWrap fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    explicit MemoryData(uint16_t maxIoBlocks = uint16_t(-1))
        : maxIoBlocks_(maxIoBlocks), map_(), h_(), fileH_(h_), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~MemoryData() noexcept = default;
    bool empty() const { return map_.empty(); }

    void add(const Record &rec, const IoData &io, uint16_t maxIoBlocks = 0) {
        add(rec, IoData(io), maxIoBlocks);
    }
    void add(const Record& rec, IoData &&io, uint16_t maxIoBlocks = 0) {
        add(rec.record(), std::move(io), maxIoBlocks);
    }
    void add(const walb_diff_record& rec, IoData &&io, uint16_t maxIoBlocks = 0) {
        const DiffRecord& _rec = static_cast<const DiffRecord&>(rec);
        /* Decide key range to search. */
        uint64_t addr0 = rec.io_address;
        if (addr0 <= fileH_.getMaxIoBlocks()) {
            addr0 = 0;
        } else {
            addr0 -= fileH_.getMaxIoBlocks();
        }
        /* Search overlapped items. */
        uint64_t addr1 = endIoAddressRec(rec);
        std::queue<RecIo> q;
        auto it = map_.lower_bound(addr0);
        while (it != map_.end() && it->first < addr1) {
            RecIo &r = it->second;
            if (r.record().isOverlapped(_rec)) {
                nIos_--;
                nBlocks_ -= r.record().ioBlocks();
                q.push(std::move(r));
                it = map_.erase(it);
            } else {
                ++it;
            }
        }
        /* Eliminate overlaps. */
        RecIo r0;
        r0.moveFrom(rec, std::move(io));
        assert(r0.isValid());
        while (!q.empty()) {
            std::vector<RecIo> v = q.front().minus(r0);
            for (RecIo &r : v) {
                nIos_++;
                nBlocks_ += r.record().ioBlocks();
                uint64_t addr = r.record().ioAddress();
                map_.emplace(addr, std::move(r));
            }
            q.pop();
        }
        /* Insert the item. */
        nIos_++;
        nBlocks_ += r0.record().ioBlocks();
        std::vector<RecIo> rv;
        if (0 < maxIoBlocks && maxIoBlocks < rec.io_blocks) {
            rv = r0.splitAll(maxIoBlocks);
        } else if (maxIoBlocks_ < rec.io_blocks) {
            rv = r0.splitAll(maxIoBlocks_);
        } else {
            rv.push_back(std::move(r0));
        }
        for (RecIo &r : rv) {
            uint64_t addr = r.record().ioAddress();
            uint16_t blks = r.record().ioBlocks();
            map_.emplace(addr, std::move(r));
            fileH_.setMaxIoBlocksIfNecessary(blks);
        }
    }
    void print(::FILE *fp = ::stdout) const {
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const walb_diff_record &rec = it->second.record2();
            printOnelineRec(rec, fp);
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
            const walb_diff_record &rec = it->second.record2();
            nBlocks += rec.io_blocks;
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
    FileHeaderWrap& header() { return fileH_; }
    void writeTo(int outFd, bool isCompressed = true) {
        Writer writer(outFd);
        writer.writeHeader(fileH_);
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const RecIo &r = it->second;
            assert(r.isValid());
            if (isCompressed) {
                writer.compressAndWriteDiff(r.record2(), r.io().rawData());
            } else {
                writer.writeDiff(r.record2(), r.io().rawData());
            }
            ++it;
        }
        writer.close();
    }
    void readFrom(int inFd) {
        Reader reader(inFd);
        reader.readHeader(fileH_);
        walb_diff_record rec;
        IoData io;
        while (reader.readAndUncompressDiff(rec, io)) {
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
        const walb_diff_record *prev = nullptr;
        while (it != map_.cend()) {
            const walb_diff_record *curr = &it->second.record2();
            if (prev) {
                if (!(prev->io_address < curr->io_address)) {
                    throw RT_ERR("Not sorted.");
                }
                if (!(endIoAddressRec(*prev) <= curr->io_address)) {
                    throw RT_ERR("Overlapped records exist.");
                }
            }
            prev = curr;
            ++it;
        }
    }
    const Map& getMap() const { return map_; }
    Map& getMap() { return map_; }
	void eraseMap(Map::iterator& i)
	{
        nIos_--;
        nBlocks_ -= i->second.record2().io_blocks;
        i = map_.erase(i);
        if (map_.empty()) {
            fileH_.resetMaxIoBlocks();
        }
	}
};

}} //namespace walb::diff
