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
#include "fileio.hpp"
#include "checksum.hpp"
#include "walb_diff.h"
#include "walb_logger.hpp"
#include "walb/block_size.h"
#include "backtrace.hpp"
#include "compressor.hpp"
#include "compression_type.hpp"

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
    }
    uint64_t endIoAddress() const { return io_address + io_blocks; }
    bool isCompressed() const { return compression_type != ::WALB_DIFF_CMPR_NONE; }

    bool isAllZero() const { return (flags & WALB_DIFF_FLAG(ALLZERO)) != 0; }
    bool isDiscard() const { return (flags & WALB_DIFF_FLAG(DISCARD)) != 0; }
    bool isNormal() const { return !isAllZero() && !isDiscard(); }
    bool isValid() const {
        try {
            verify();
            return true;
        } catch (...) {
            return false;
        }
    }
    void verify() const {
        const char *const NAME = "DiffRecord";
        if (!isNormal()) {
            if (isAllZero() && isDiscard()) {
                throw cybozu::Exception(NAME) << "allzero and discard flag is exclusive";
            }
            return;
        }
        if (::WALB_DIFF_CMPR_MAX <= compression_type) {
            throw cybozu::Exception(NAME) << "compression type is invalid";
        }
        if (io_blocks == 0) {
            throw cybozu::Exception(NAME) << "ioBlocks() must not be 0 for normal IO";
        }
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "----------\n"
                  "ioAddress: %" PRIu64 "\n"
                  "ioBlocks: %u\n"
                  "compressionType: %u (%s)\n"
                  "dataOffset: %u\n"
                  "dataSize: %u\n"
                  "checksum: %08x\n"
                  "isAllZero: %d\n"
                  "isDiscard: %d\n"
                  , io_address, io_blocks
                  , compression_type, compressionTypeToStr(compression_type).c_str()
                  , data_offset, data_size
                  , checksum, isAllZero(), isDiscard());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s\n", toStr("wdiff_rec:\t").c_str());
    }
    std::string toStr(const char *prefix = "") const {
        return cybozu::util::formatString(
            "%s""%" PRIu64 "\t%u\t%s\t%u\t%u\t%08x\t%c%c"
            , prefix, io_address, io_blocks
            , compressionTypeToStr(compression_type).c_str()
            , data_offset, data_size, checksum
            , isAllZero() ? 'Z' : '-', isDiscard() ? 'D' : '-');
    }
    friend inline std::ostream &operator<<(std::ostream &os, const DiffRecord &rec) {
        os << rec.toStr();
        return os;
    }
    static void printHeader(::FILE *fp = ::stdout) {
        ::fprintf(fp, "%s\n", getHeader());
    }
    static const char* getHeader() {
        return "#wdiff_rec: addr blks cmpr offset size csum allzero discard";
    }
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
    std::vector<char> tryCompress(DiffRecord& compRec, const char *data) const {
        std::vector<char> compData;
        const size_t dataSize = io_blocks * LOGICAL_BLOCK_SIZE;
        compData.resize(snappy::MaxCompressedLength(dataSize));
        size_t compressedSize;
        snappy::RawCompress(data, dataSize, compData.data(), &compressedSize);
        compRec = *this; // copy
        if (compressedSize < dataSize) {
            compData.resize(compressedSize);
            compRec.compression_type = ::WALB_DIFF_CMPR_SNAPPY;
            compRec.data_size = compressedSize;
        } else {
            compData.resize(dataSize);
            compRec.compression_type = ::WALB_DIFF_CMPR_NONE;
            compRec.data_size = dataSize;
            ::memcpy(compData.data(), data, dataSize);
        }
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
    DiffIo& operator=(const DiffIo&) = default;
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
            data.resize(rec.data_size);
        } else {
            ioBlocks = 0;
            compressionType = ::WALB_DIFF_CMPR_NONE;
            data.clear();
        }
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
    template <typename Writer>
    void writeTo(Writer &writer) const {
        if (getSize() > 0) {
            writer.write(get(), getSize());
        }
    }
    template <typename Reader>
    void readFrom(Reader &reader) {
        if (getSize() > 0) {
            reader.read(get(), getSize());
        }
    }
    template <typename Reader>
    void setAndReadFrom(const DiffRecord &rec, Reader &reader) {
        set(rec);
        readFrom(reader);
        const uint32_t csum = calcChecksum();
        if (rec.checksum != csum) {
            throw cybozu::Exception(__func__) << "checksum differ" << rec.checksum << csum;
        }
    }
};

} //namesapce walb
