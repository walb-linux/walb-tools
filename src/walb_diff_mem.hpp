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
    DiffRecIo(const DiffRecord &rec, AlignedArray &&data)
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
    bool isValid(bool isChecksum = false) const;

    void print(::FILE *fp = ::stdout) const {
        rec_.printOneline(fp);
        io_.printOneline(fp);
    }

    /**
     * Split the DiffRecIo into pieces
     * where each ioBlocks is <= a specified one.
     */
    std::vector<DiffRecIo> splitAll(uint32_t ioBlocks) const;

    /**
     * Create (IO portions of rhs) - (that of *this).
     * If non-overlapped, throw runtime error.
     * The overlapped data of rhs will be used.
     * *this will not be changed.
     */
    std::vector<DiffRecIo> minus(const DiffRecIo &rhs) const;
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
    /* All IOs must not exceed the size inside the map. */
    const uint32_t maxIoBlocks_;
    Map map_;
    DiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    explicit DiffMemory(uint32_t maxIoBlocks = DEFAULT_MAX_WDIFF_IO_BLOCKS)
        : maxIoBlocks_(maxIoBlocks), map_(), fileH_(), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~DiffMemory() noexcept = default;
    bool empty() const { return map_.empty(); }
    void add(const DiffRecord& rec, DiffIo &&io, uint32_t maxIoBlocks = 0);
    void print(::FILE *fp = ::stdout) const;
    uint64_t getNBlocks() const { return nBlocks_; }
    uint64_t getNIos() const { return nIos_; }
    void checkStatistics() const;
    DiffFileHeader& header() { return fileH_; }
    void writeTo(int outFd, bool isCompressed = true);
    void readFrom(int inFd);
    /**
     * Clear all data.
     */
    void clear() {
        map_.clear();
        nIos_ = 0;
        nBlocks_ = 0;
        fileH_.init();
    }
    void checkNoOverlappedAndSorted() const;
    const Map& getMap() const { return map_; }
    Map& getMap() { return map_; }
    void eraseFromMap(Map::iterator& i);
};

} //namespace walb
