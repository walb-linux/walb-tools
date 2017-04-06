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
    AlignedArray io_;
public:
    const DiffRecord &record() const { return rec_; }
    const AlignedArray &io() const { return io_; }

    DiffRecIo() {}
    DiffRecIo(const DiffRecord &rec, AlignedArray &&buf)
        : rec_(rec) {
        if (rec.isNormal()) {
            io_ = std::move(buf);
        } else {
            io_.clear();
        }
        assert(isValid());
    }
    bool isValid(bool isChecksum = false) const;

    void print(::FILE *fp = ::stdout) const {
        rec_.printOneline(fp);
        printOnelineDiffIo(io_, fp);
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
    /*
     * This parameter is in order not to exist too large IOs.
     * 0 means no limitation.
     */
    uint32_t maxIoBlocks_;

    Map map_;
    DiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    DiffMemory()
        : maxIoBlocks_(DEFAULT_MAX_IO_LB)
        , map_(), fileH_(), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~DiffMemory() noexcept = default;
    void setMaxIoBlocks(uint32_t maxIoBlocks) { maxIoBlocks_ = maxIoBlocks; }
    bool empty() const { return map_.empty(); }
    void add(const DiffRecord& rec, AlignedArray &&buf);
    void print(::FILE *fp = ::stdout) const;
    uint64_t getNBlocks() const { return nBlocks_; }
    uint64_t getNIos() const { return nIos_; }
    void checkStatistics() const;
    DiffFileHeader& header() { return fileH_; }
    void writeTo(int outFd, int cmprType = ::WALB_DIFF_CMPR_SNAPPY);
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
