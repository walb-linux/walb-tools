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
namespace diff {

inline uint64_t endIoAddressRec(const walb_diff_record& rec) {
    return rec.io_address + rec.io_blocks;
}
inline bool existsRec(const walb_diff_record& rec) {
    return (rec.flags & WALB_DIFF_FLAG(EXIST)) != 0;
}
inline bool isAllZeroRec(const walb_diff_record& rec) {
    return (rec.flags & WALB_DIFF_FLAG(ALLZERO)) != 0;
}
inline bool isDiscardRec(const walb_diff_record& rec) {
    return (rec.flags & WALB_DIFF_FLAG(DISCARD)) != 0;
}
inline bool isCompressedRec(const walb_diff_record& rec) {
    return rec.compression_type != ::WALB_DIFF_CMPR_NONE;
}
inline bool isNormalRec(const walb_diff_record& rec) {
    return !isAllZeroRec(rec) && !isDiscardRec(rec);
}
inline bool isValidRec(const walb_diff_record& rec) {
    if (!existsRec(rec)) {
        LOGd("Does not exist.\n");
        return false;
    }
    if (!isNormalRec(rec)) {
        if (isAllZeroRec(rec) && isDiscardRec(rec)) {
            LOGd("allzero and discard flag is exclusive.\n");
            return false;
        }
        return true;
    }
    if (::WALB_DIFF_CMPR_MAX <= rec.compression_type) {
        LOGd("compression type is invalid.\n");
        return false;
    }
    if (rec.io_blocks == 0) {
        LOGd("ioBlocks() must not be 0 for normal IO.\n");
        return false;
    }
    return true;
}
inline void printRec(const walb_diff_record& rec, ::FILE *fp = ::stdout) {
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
       rec.io_address, rec.io_blocks,
       rec.compression_type, rec.data_offset, rec.data_size,
       rec.checksum, existsRec(rec), isAllZeroRec(rec), isDiscardRec(rec));
}
inline void printOnelineRec(const walb_diff_record& rec, FILE *fp = stdout) {
    ::fprintf(fp, "wdiff_rec:\t%" PRIu64 "\t%u\t%u\t%u\t%u\t%08x\t%d%d%d\n",
        rec.io_address, rec.io_blocks,
        rec.compression_type, rec.data_offset, rec.data_size,
        rec.checksum, existsRec(rec), isAllZeroRec(rec), isDiscardRec(rec));
}
inline void initRec(walb_diff_record& rec) {
    ::memset(&rec, 0, sizeof(struct walb_diff_record));
    rec.flags = WALB_DIFF_FLAG(EXIST);
}
inline void setExistsRec(walb_diff_record& rec) {
    rec.flags |= WALB_DIFF_FLAG(EXIST);
}
inline void clearExistsRec(walb_diff_record& rec) {
    rec.flags &= ~WALB_DIFF_FLAG(EXIST);
}
inline void setNormalRec(walb_diff_record& rec) {
    rec.flags &= ~WALB_DIFF_FLAG(ALLZERO);
    rec.flags &= ~WALB_DIFF_FLAG(DISCARD);
}
inline void setAllZeroRec(walb_diff_record& rec) {
    rec.flags |= WALB_DIFF_FLAG(ALLZERO);
    rec.flags &= ~WALB_DIFF_FLAG(DISCARD);
}
inline void setDiscardRec(walb_diff_record& rec) {
    rec.flags &= ~WALB_DIFF_FLAG(ALLZERO);
    rec.flags |= WALB_DIFF_FLAG(DISCARD);
}
inline bool isOverwrittenBy(const walb_diff_record& lhs, const walb_diff_record &rhs) {
    return rhs.io_address <= lhs.io_address &&
        lhs.io_address + lhs.io_blocks <= rhs.io_address + rhs.io_blocks;
}

inline bool isOverlapped(const walb_diff_record& lhs, const walb_diff_record &rhs) {
    return lhs.io_address < rhs.io_address + rhs.io_blocks &&
        rhs.io_address < lhs.io_address + lhs.io_blocks;
}

/*
    you can change this class with walb_diff_record safely
*/
struct DiffRecord : public walb_diff_record {
    void init() {
        initRec(*this);
    }
    uint64_t endIoAddress() const { return io_address + io_blocks; }
    bool isCompressed() const { return compression_type != ::WALB_DIFF_CMPR_NONE; }

    bool exists() const { return existsRec(*this); }
    bool isAllZero() const { return isAllZeroRec(*this); }
    bool isDiscard() const { return isDiscardRec(*this); }
    bool isNormal() const { return isNormalRec(*this); }
    bool isValid() const { return isValidRec(*this); }

    void print(::FILE *fp = ::stdout) const { printRec(*this, fp); }
    void printOneline(::FILE *fp = ::stdout) const { printOnelineRec(*this, fp); }

    void setExists() { setExistsRec(*this); }
    void clearExists() { clearExistsRec(*this); }
    void setNormal() { setNormalRec(*this); }
    void setAllZero() { setAllZeroRec(*this); }
    void setDiscard() { setDiscardRec(*this); }
	bool isOverwrittenBy(const walb_diff_record &rhs) const { return diff::isOverwrittenBy(*this, rhs); }
    bool isOverlapped(const walb_diff_record &rhs) const { return diff::isOverlapped(*this, rhs); }
};

/**
 * Split a record into several records
 * where all splitted records' ioBlocks will be <= a specified one.
 *
 * CAUSION:
 *   The checksum of splitted records will be invalid state.
 *   Only non-compressed records can be splitted.
 */
std::vector<walb_diff_record> splitAll(const walb_diff_record& rec, uint16_t ioBlocks0) {
    if (ioBlocks0 == 0) {
        throw cybozu::Exception("splitAll: ioBlocks0 must not be 0.");
    }
    if (isCompressedRec(rec)) {
        throw cybozu::Exception("splitAll: compressed data can not be splitted.");
    }
    std::vector<walb_diff_record> v;
    uint64_t addr = rec.io_address;
    uint16_t remaining = rec.io_blocks;
    const bool isNormal = isNormalRec(rec);
    while (remaining > 0) {
        uint16_t blks = std::min(ioBlocks0, remaining);
        v.push_back(walb_diff_record());
        walb_diff_record& r = v.back();
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
        return cybozu::util::calcChecksum(&data[0], data.size(), 0);
    }

    /**
     * Calculate whether all-zero or not.
     */
    bool calcIsAllZero() const {
        const size_t size = data.size();
        if (isCompressed() || size == 0) { return false; }
        assert(size % LOGICAL_BLOCK_SIZE == 0);
        return cybozu::util::calcIsAllZero(&data[0], size);
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
    /*
        set data
        return written size
        don't write larger than reserveSize
        size_t setter(char *data);
    */
    template<class Writter>
    void setByWritter(size_t reserveSize, Writter writter) {
        data.resize(reserveSize);
        if (reserveSize == 0) return;
        size_t writtenSize = writter(&data[0]);
        data.resize(writtenSize);
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

    void set(const struct walb_diff_record &rec) {
        if (isNormalRec(rec)) {
            ioBlocks = rec.io_blocks;
            compressionType = rec.compression_type;
        } else {
            ioBlocks = 0;
            compressionType = ::WALB_DIFF_CMPR_NONE;
        }
        data.resize(rec.data_size);
    }

    void moveFrom(std::vector<char> &&data) {
        this->data = std::move(data);
    }
    const char *rawData() const { return &data[0]; }
    char *rawData() { return &data[0]; }
};

/**
 * Compress an IO data.
 * Supported algorithms: snappy.
 */

inline IoData compressIoData(const walb_diff_record& rec, const char *data, int type)
{
    if (type != ::WALB_DIFF_CMPR_SNAPPY) {
        throw cybozu::Exception("compressIoData:Currently only snappy is supported.");
    }
    if (rec.io_blocks == 0) {
        return IoData();
    }
    IoData io1(rec.io_blocks, type);
    io1.setByWritter(snappy::MaxCompressedLength(rec.data_size), [&](char *p) {
        size_t size;
        snappy::RawCompress(data, rec.data_size, p, &size);
        return size;
    });
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
    io1.setByWritter(decSize, [&](char *p) {
        size_t size = dec.run(p, decSize, &io0.data[0], io0.data.size());
        if (size != decSize) {
            throw cybozu::Exception("uncompressIoData:size is invalid") << size << decSize;
        }
        return decSize;
    });
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
