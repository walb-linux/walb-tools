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

    bool exists() const { return (flags & WALB_DIFF_FLAG(EXIST)) != 0; }
    bool isAllZero() const { return (flags & WALB_DIFF_FLAG(ALLZERO)) != 0; }
    bool isDiscard() const { return (flags & WALB_DIFF_FLAG(DISCARD)) != 0; }
    bool isNormal() const { return !isAllZero() && !isDiscard(); }
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
    /**
     * Split a record into several records
     * where all splitted records' ioBlocks will be <= a specified one.
     *
     * CAUSION:
     *   The checksum of splitted records will be invalid state.
     *   Only non-compressed records can be splitted.
     */
    std::vector<DiffRecord> splitAll(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0) {
            throw cybozu::Exception("splitAll: ioBlocks0 must not be 0.");
        }
        if (isCompressed()) {
            throw cybozu::Exception("splitAll: compressed data can not be splitted.");
        }
        std::vector<DiffRecord> v;
        uint64_t addr = io_address;
        uint16_t remaining = io_blocks;
        const bool isNormal = this->isNormal();
        while (remaining > 0) {
            uint16_t blks = std::min(ioBlocks0, remaining);
            v.push_back(*this);
            DiffRecord& r = v.back();
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
    std::vector<char> compress(DiffRecord& compRec, const char *data) const {
        std::vector<char> compData;
        const size_t dataSize = io_blocks * LOGICAL_BLOCK_SIZE;
        compData.resize(snappy::MaxCompressedLength(dataSize));
        size_t compressedSize;
        snappy::RawCompress(data, dataSize, compData.data(), &compressedSize);
        compData.resize(compressedSize);
        compRec = *this;
        compRec.compression_type = ::WALB_DIFF_CMPR_SNAPPY;
        compRec.data_size = compressedSize;
        compRec.checksum = cybozu::util::calcChecksum(compData.data(), compData.size(), 0);
        return compData;
    }
};

/**
 * Block diff for an IO.
 */
struct DiffIo
{
    uint16_t ioBlocks; /* [logical block]. */
    int compressionType;
    std::vector<char> data;
    const char *get() const { return data.data(); }
    char *get() { return data.data(); }
    size_t getSize() const { return data.size(); }

    explicit DiffIo(uint16_t ioBlocks = 0, int compressionType = ::WALB_DIFF_CMPR_NONE, const char *data = nullptr, size_t size = 0) : ioBlocks(ioBlocks), compressionType(compressionType), data(data, data + size) {
    }
    DiffIo(const DiffIo &) = default;
    DiffIo(DiffIo &&) = default;
    DiffIo& operator=(DiffIo&&) = default;

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
                if (getSize() != ioBlocks * LOGICAL_BLOCK_SIZE) {
                    LOGd("dataSize is not the same: %zu %u\n"
                         , getSize(), ioBlocks * LOGICAL_BLOCK_SIZE);
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
        return cybozu::util::calcChecksum(get(), getSize(), 0);
    }

    /**
     * Calculate whether all-zero or not.
     */
    bool calcIsAllZero() const {
        const size_t size = getSize();
        if (isCompressed() || size == 0) { return false; }
        assert(size % LOGICAL_BLOCK_SIZE == 0);
        return cybozu::util::calcIsAllZero(get(), size);
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "ioBlocks %u\n"
                  "type %d\n"
                  "size %zu\n"
                  "checksum %0x\n"
                  , ioBlocks
                  , compressionType
                  , getSize()
                  , calcChecksum());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "ioBlocks %u type %d size %zu checksum %0x\n"
                  , ioBlocks
                  , compressionType
                  , getSize()
                  , calcChecksum());
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
    /**
     * Split an IO into multiple IOs
     * each of which io size is not more than a specified one.
     *
     * CAUSION:
     *   Compressed IO can not be splitted.
     */
    std::vector<DiffIo> splitIoDataAll(uint16_t ioBlocks0) const
    {
        if (ioBlocks0 == 0) {
            throw cybozu::Exception("splitIoDataAll: ioBlocks0 must not be 0.");
        }
        if (isCompressed()) {
            throw cybozu::Exception("splitIoDataAll: compressed IO can not be splitted.");
        }
        assert(isValid());
        std::vector<DiffIo> v;
        uint16_t remaining = ioBlocks;
        const char *p = data.data();
        while (remaining > 0) {
            uint16_t blks = std::min(remaining, ioBlocks0);
            size_t size = blks * LOGICAL_BLOCK_SIZE;
            v.emplace_back(blks, WALB_DIFF_CMPR_NONE, p, size);
            remaining -= blks;
            p += size;
        }
        return v;
    }
    void uncompress()
    {
        assert(isCompressed());
        walb::Uncompressor dec(compressionType);
        const size_t decSize = ioBlocks * LOGICAL_BLOCK_SIZE;
        std::vector<char> decData(decSize);
        size_t size = dec.run(decData.data(), decSize, data.data(), data.size());
        if (size != decSize) {
            throw cybozu::Exception("uncompressIoData:size is invalid") << size << decSize;
        }
        compressionType = WALB_DIFF_CMPR_NONE;
        data = std::move(decData);
    }
};

} //namesapce walb
