#pragma once
/**
 * @file
 * @brief walb diff base utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <map>
#include <queue>

#include <snappy.h>

#include "util.hpp"
#include "memory_buffer.hpp"
#include "fileio.hpp"
#include "checksum.hpp"
#include "block_diff.hpp"
#include "walb_diff.h"
#include "walb_logger.hpp"
#include "walb/block_size.h"
#include "backtrace.hpp"
#include "compressor.hpp"

static_assert(::WALB_DIFF_FLAGS_SHIFT_MAX <= 8, "Too many walb diff flags.");
static_assert(::WALB_DIFF_CMPR_MAX <= 256, "Too many walb diff cmpr types.");

namespace walb {

/*
    you can freely cast this class to DiffRecord safely and vice versa.
*/
struct DiffRecord : public walb_diff_record {
    DiffRecord()
    {
        init();
    }
    void init() {
        ::memset(this, 0, sizeof(struct walb_diff_record));
        flags = WALB_DIFF_FLAG(EXIST);
    }
    uint64_t endIoAddress() const { return io_address + io_blocks; }
    bool isCompressed() const { return compression_type != ::WALB_DIFF_CMPR_NONE; }

    bool exists() const {
        return (flags & WALB_DIFF_FLAG(EXIST)) != 0;
    }
    bool isAllZero() const { return (flags & WALB_DIFF_FLAG(ALLZERO)) != 0; }
    bool isDiscard() const { return (flags & WALB_DIFF_FLAG(DISCARD)) != 0; }
    bool isNormal() const {
        return !isAllZero() && !isDiscard();
    }
    bool isValid() const {
        if (!exists()) {
            LOGd("Does not exist.\n");
            return false;
        }
        if (!isNormal()) {
            if (isAllZero() && isDiscard()) {
                LOGd("allzero and discard flag is exclusive.\n");
                return false;
            }
            return true;
        }
        if (::WALB_DIFF_CMPR_MAX <= compression_type) {
            LOGd("compression type is invalid.\n");
            return false;
        }
        if (io_blocks == 0) {
            LOGd("ioBlocks() must not be 0 for normal IO.\n");
            return false;
        }
        return true;
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "----------\n"
           "ioAddress: %" PRIu64 "\n"
           "ioBlocks: %u\n"
           "compressionType: %u\n"
           "dataOffset: %u\n"
           "dataSize: %u\n"
           "checksum: %08x\n"
           "exists: %d\n"
           "isAllZero: %d\n"
           "isDiscard: %d\n",
           io_address, io_blocks,
           compression_type, data_offset, data_size,
           checksum, exists(), isAllZero(), isDiscard());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "wdiff_rec:\t%" PRIu64 "\t%u\t%u\t%u\t%u\t%08x\t%d%d%d\n",
            io_address, io_blocks,
            compression_type, data_offset, data_size,
            checksum, exists(), isAllZero(), isDiscard());
    }
    void setExists() { flags |= WALB_DIFF_FLAG(EXIST); }
    void clearExists() { flags &= ~WALB_DIFF_FLAG(EXIST); }
    void setNormal() {
        flags &= ~WALB_DIFF_FLAG(ALLZERO);
        flags &= ~WALB_DIFF_FLAG(DISCARD);
    }
    void setAllZero() {
        flags |= WALB_DIFF_FLAG(ALLZERO);
        flags &= ~WALB_DIFF_FLAG(DISCARD);
    }
    void setDiscard() {
        flags &= ~WALB_DIFF_FLAG(ALLZERO);
        flags |= WALB_DIFF_FLAG(DISCARD);
    }
	bool isOverwrittenBy(const DiffRecord &rhs) const {
        return rhs.io_address <= io_address &&
            io_address + io_blocks <= rhs.io_address + rhs.io_blocks;
    }
    bool isOverlapped(const DiffRecord &rhs) const {
        return io_address < rhs.io_address + rhs.io_blocks &&
            rhs.io_address < io_address + io_blocks;
    }
};

namespace diff {
/**
 * Split a record into several records
 * where all splitted records' ioBlocks will be <= a specified one.
 *
 * CAUSION:
 *   The checksum of splitted records will be invalid state.
 *   Only non-compressed records can be splitted.
 */
std::vector<DiffRecord> splitAll(const DiffRecord& rec, uint16_t ioBlocks0) {
    if (ioBlocks0 == 0) {
        throw cybozu::Exception("splitAll: ioBlocks0 must not be 0.");
    }
    if (rec.isCompressed()) {
        throw cybozu::Exception("splitAll: compressed data can not be splitted.");
    }
    std::vector<DiffRecord> v;
    uint64_t addr = rec.io_address;
    uint16_t remaining = rec.io_blocks;
    const bool isNormal = rec.isNormal();
    while (remaining > 0) {
        uint16_t blks = std::min(ioBlocks0, remaining);
        v.push_back(DiffRecord());
        DiffRecord& r = v.back();
        r = rec;
        r.io_address = addr;
        r.io_blocks = blks;
        if (isNormal) {
            r.data_size = blks * LOGICAL_BLOCK_SIZE;
        }
        addr += blks;
        remaining -= blks;
    }
    assert(!v.empty());
    return v;
}

/**
 * Block diff for an IO.
 */
class IoData
{
public:
    uint16_t ioBlocks; /* [logical block]. */
    int compressionType;
    std::vector<char> data;

    explicit IoData(uint16_t ioBlocks = 0, int compressionType = ::WALB_DIFF_CMPR_NONE, const char *data = nullptr, size_t size = 0) : ioBlocks(ioBlocks), compressionType(compressionType), data(data, data + size) {
    }
    IoData(const IoData &rhs) : ioBlocks(rhs.ioBlocks), compressionType(rhs.compressionType), data(rhs.data) {
    }
    IoData(IoData &&rhs) : ioBlocks(rhs.ioBlocks), compressionType(rhs.compressionType), data(std::move(rhs.data)) {
    }
    void clear() {
        ioBlocks = 0;
        compressionType = ::WALB_DIFF_CMPR_NONE;
        data.clear();
    }
    bool isCompressed() const { return compressionType != ::WALB_DIFF_CMPR_NONE; }

    bool empty() const { return ioBlocks == 0; }

    bool isValid() const {
        if (empty()) {
            if (!data.empty()) {
                LOGd("Data is not empty.\n");
                return false;
            }
            return true;
        } else {
            if (isCompressed()) {
                return true;
            } else {
                if (data.size() != ioBlocks * LOGICAL_BLOCK_SIZE) {
                    LOGd("dataSize is not the same: %zu %u\n"
                         , data.size(), ioBlocks * LOGICAL_BLOCK_SIZE);
                    return false;
                }
                return true;
            }
        }
    }

    /**
     * Calculate checksum.
     */
    uint32_t calcChecksum() const {
        if (empty()) { return 0; }
        return cybozu::util::calcChecksum(data.data(), data.size(), 0);
    }

    /**
     * Calculate whether all-zero or not.
     */
    bool calcIsAllZero() const {
        const size_t size = data.size();
        if (isCompressed() || size == 0) { return false; }
        assert(size % LOGICAL_BLOCK_SIZE == 0);
        return cybozu::util::calcIsAllZero(data.data(), size);
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "ioBlocks %u\n"
                  "type %d\n"
                  "size %zu\n"
                  "checksum %0x\n"
                  , ioBlocks
                  , compressionType
                  , data.size()
                  , calcChecksum());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "ioBlocks %u type %d size %zu checksum %0x\n"
                  , ioBlocks
                  , compressionType
                  , data.size()
                  , calcChecksum());
    }

    void swap(IoData& rhs) noexcept {
        std::swap(ioBlocks, rhs.ioBlocks);
        std::swap(compressionType, rhs.compressionType);
        data.swap(rhs.data);
    }
    IoData &operator=(const IoData &rhs) {
        ioBlocks = rhs.ioBlocks;
        compressionType = rhs.compressionType;
        data = rhs.data;
        return *this;
    }
    IoData &operator=(IoData &&rhs) {
        swap(rhs);
        return *this;
    }

    void set(const DiffRecord &rec) {
        if (rec.isNormal()) {
            ioBlocks = rec.io_blocks;
            compressionType = rec.compression_type;
        } else {
            ioBlocks = 0;
            compressionType = ::WALB_DIFF_CMPR_NONE;
        }
        data.resize(rec.data_size);
    }
};

/**
 * Compress an IO data.
 * Supported algorithms: snappy.
 */

inline IoData compressIoData(const DiffRecord& rec, const char *data, int type)
{
    if (type != ::WALB_DIFF_CMPR_SNAPPY) {
        throw cybozu::Exception("compressIoData:Currently only snappy is supported.");
    }
    if (rec.io_blocks == 0) {
        return IoData();
    }
    IoData io1(rec.io_blocks, type);
    io1.data.resize(snappy::MaxCompressedLength(rec.data_size));
    size_t compressedSize;
    snappy::RawCompress(data, rec.data_size, io1.data.data(), &compressedSize);
    io1.data.resize(compressedSize);
    return io1;
}

/**
 * Uncompress an IO data.
 * Supported algorithms: snappy.
 */
inline IoData uncompressIoData(const IoData &io0)
{
    if (!io0.isCompressed()) {
        throw RT_ERR("Need not uncompress already uncompressed diff IO.");
    }
    walb::Uncompressor dec(io0.compressionType);
    const size_t decSize = io0.ioBlocks * LOGICAL_BLOCK_SIZE;
    IoData io1(io0.ioBlocks, WALB_DIFF_CMPR_NONE);
    io1.data.resize(decSize);
    size_t size = dec.run(io1.data.data(), decSize, io0.data.data(), io0.data.size());
    if (size != decSize) {
        throw cybozu::Exception("uncompressIoData:size is invalid") << size << decSize;
    }
    return io1;
}

/**
 * Split an IO into multiple IOs
 * each of which io size is not more than a specified one.
 *
 * CAUSION:
 *   Compressed IO can not be splitted.
 */
inline std::vector<IoData> splitIoDataAll(const IoData &io0, uint16_t ioBlocks0)
{
    if (ioBlocks0 == 0) {
        throw RT_ERR("splitAll: ioBlocks0 must not be 0.");
    }
    if (io0.isCompressed()) {
        throw RT_ERR("splitAll: compressed IO can not be splitted.");
    }
    assert(io0.isValid());

    std::vector<IoData> v;
    uint16_t remaining = io0.ioBlocks;
    size_t off = 0;
    while (0 < remaining) {
        uint16_t blks = std::min(remaining, ioBlocks0);
        size_t size = blks * LOGICAL_BLOCK_SIZE;
        v.emplace_back(blks, WALB_DIFF_CMPR_NONE, &io0.data[off], size);
        remaining -= blks;
        off += size;
    }
    assert(off == io0.data.size());
    assert(!v.empty());

    return v;
}

}} //namesapce walb::diff
