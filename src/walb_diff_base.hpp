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
#include "range_util.hpp"
#include "fileio.hpp"
#include "checksum.hpp"
#include "walb_types.hpp"
#include "walb_util.hpp"
#include "walb_diff.h"
#include "walb_logger.hpp"
#include "linux/walb/block_size.h"
#include "backtrace.hpp"
#include "compressor.hpp"
#include "compression_type.hpp"
#include "address_util.hpp"

static_assert(::WALB_DIFF_FLAGS_SHIFT_MAX <= 8, "Too many walb diff flags.");
static_assert(::WALB_DIFF_CMPR_MAX <= 256, "Too many walb diff cmpr types.");

namespace walb {

/*
    you can freely cast this class to DiffRecord safely and vice versa.
*/
struct DiffRecord : public walb_diff_record
{
    constexpr static const char *NAME = "DiffRecord";
    DiffRecord() {
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
    bool isValid() const;
    void verify() const;

    void print(::FILE *fp = ::stdout) const;
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s\n", toStr("wdiff_rec:\t").c_str());
    }
    std::string toStr(const char *prefix = "") const;
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
        return cybozu::isOverwritten(io_address, io_blocks, rhs.io_address, rhs.io_blocks);
    }
    bool isOverlapped(const DiffRecord &rhs) const {
        return cybozu::isOverlapped(io_address, io_blocks, rhs.io_address, rhs.io_blocks);
    }
    /**
     * Split a record into several records
     * where all splitted records' ioBlocks will be <= a specified one.
     *
     * CAUSION:
     *   The checksum of splitted records will be invalid state.
     *   Only non-compressed records can be splitted.
     */
    std::vector<DiffRecord> splitAll(uint32_t ioBlocks0) const;
};

/**
 * Block diff for an IO.
 */
struct DiffIo
{
    uint32_t ioBlocks; /* [logical block]. */
    int compressionType;
    AlignedArray data;

    const char *get() const { return data.data(); }
    char *get() { return data.data(); }
    size_t getSize() const { return data.size(); }

    explicit DiffIo(uint32_t ioBlocks = 0, int compressionType = ::WALB_DIFF_CMPR_NONE, const char *data = nullptr, size_t size = 0)
        : ioBlocks(ioBlocks), compressionType(compressionType), data() {
        if (data && size > 0) util::assignAlignedArray(this->data, data, size);
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

    bool isValid() const;

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
    bool isAllZero() const {
        const size_t size = getSize();
        if (isCompressed() || size == 0) { return false; }
        assert(size % LOGICAL_BLOCK_SIZE == 0);
        return cybozu::util::isAllZero(get(), size);
    }

    void print(::FILE *fp = ::stdout) const;
    void printOneline(::FILE *fp = ::stdout) const;

    void set(const DiffRecord &rec, const char *data0 = nullptr);

    /**
     * Split an IO into multiple IOs
     * each of which io size is not more than a specified one.
     *
     * CAUSION:
     *   Compressed IO can not be splitted.
     */
    std::vector<DiffIo> splitIoDataAll(uint32_t ioBlocks0) const;

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
    void setAndReadFrom(const DiffRecord &rec, Reader &reader, bool verifyChecksum = true) {
        set(rec);
        readFrom(reader);
        if (!verifyChecksum) return;
        const uint32_t csum = calcChecksum();
        if (rec.checksum != csum) {
            throw cybozu::Exception(__func__) << "checksum differ" << rec.checksum << csum;
        }
    }
};

inline uint32_t calcDiffIoChecksum(const AlignedArray &io)
{
    if (io.empty()) return 0;
    return cybozu::util::calcChecksum(io.data(), io.size(), 0);
}

inline bool calcDiffIoIsAllZero(const AlignedArray &io)
{
    if (io.size() == 0) return false;
    return cybozu::util::isAllZero(io.data(), io.size());
}


int compressData(
    const char *inData, size_t inSize, AlignedArray &outData, size_t &outSize,
    int type = ::WALB_DIFF_CMPR_SNAPPY, int level = 0);
void uncompressData(
    const char *inData, size_t inSize, AlignedArray &outData, int type);

void compressDiffIo(
    const DiffRecord &inRec, const char *inData,
    DiffRecord &outRec, AlignedArray &outData, int type = ::WALB_DIFF_CMPR_SNAPPY, int level = 0);
void uncompressDiffIo(
    const DiffRecord &inRec, const char *inData,
    DiffRecord &outRec, AlignedArray &outData, bool calcChecksum = true);


/**
 * sizeof(DiffIndexRecord) == sizeof(walb_indexed_diff_record)
 */
struct DiffIndexRecord : public walb_indexed_diff_record
{
    void init() {
        ::memset(this, 0, sizeof(*this));
        // Now isNormal() will be true.
    }

    uint64_t endIoAddress() const { return io_address + io_blocks; }
    bool isCompressed() const { return compression_type != ::WALB_DIFF_CMPR_NONE; }

    bool isAllZero() const { return (flags & WALB_DIFF_FLAG(ALLZERO)) != 0; }
    bool isDiscard() const { return (flags & WALB_DIFF_FLAG(DISCARD)) != 0; }
    bool isNormal() const { return !isAllZero() && !isDiscard(); }

    bool isValid(bool doChecksum = true) const { return verifyDetail(false, doChecksum); }
    void verify(bool doChecksum = true) const { verifyDetail(true, doChecksum); }

    static constexpr const char *NAME = "DiffIndexRecord";

    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s\n", toStr("wdiff_idx_rec:\t").c_str());
    }
    std::string toStr(const char *prefix = "") const;
    friend inline std::ostream &operator<<(std::ostream &os, const DiffIndexRecord &rec) {
        os << rec.toStr();
        return os;
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

    bool isOverwrittenBy(const DiffIndexRecord &rhs) const {
        return cybozu::isOverwritten(io_address, io_blocks, rhs.io_address, rhs.io_blocks);
    }
    bool isOverlapped(const DiffIndexRecord &rhs) const {
        return cybozu::isOverlapped(io_address, io_blocks, rhs.io_address, rhs.io_blocks);
    }

    void verifyAligned() const {
        if (!isAlignedSize(io_blocks)) {
            throw cybozu::Exception(NAME) << "IO is not alined" << io_blocks;
        }
    }

    std::vector<DiffIndexRecord> split() const;
    std::vector<DiffIndexRecord> minus(const DiffIndexRecord& rhs) const;

    void updateRecChecksum();
private:
    bool verifyDetail(bool throwError, bool doChecksum) const;
};


/**
 * sizeof(DiffIndexedSuper) == sizeof(walb_diff_index_super)
 */
struct DiffIndexSuper : walb_diff_index_super
{
    constexpr static const char *NAME = "DiffIndexSuper";
    void init() {
        ::memset(this, 0, sizeof(*this));
    }
    void updateChecksum() {
        checksum = 0;
        checksum = cybozu::util::calcChecksum(this, sizeof(*this), 0);
    }
    void verify() {
        if (cybozu::util::calcChecksum(this, sizeof(*this), 0) != 0) {
            throw cybozu::Exception(NAME) << "invalid checksum";
        }
    }
};


} //namesapce walb
