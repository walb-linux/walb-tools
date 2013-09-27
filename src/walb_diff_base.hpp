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
#include "stdout_logger.hpp"

#include "walb/block_size.h"

static_assert(::WALB_DIFF_FLAGS_SHIFT_MAX <= 8, "Too many walb diff flags.");
static_assert(::WALB_DIFF_CMPR_MAX <= 256, "Too many walb diff cmpr types.");

namespace walb {
namespace diff {

template <class RecT>
class RecordWrapT : public block_diff::BlockDiffKey
{
private:
    RecT *recP_; /* must not be nullptr. */

public:
    explicit RecordWrapT(RecT *recP) : recP_(recP) {
        assert(recP);
    }
    RecordWrapT(const RecordWrapT &rhs) : recP_(rhs.recP_) {}
    RecordWrapT(RecordWrapT &&rhs) = delete;
    RecordWrapT &operator=(const RecordWrapT &rhs) {
        *recP_ = *rhs.recP_;
        return *this;
    }
    RecordWrapT &operator=(RecordWrapT &&rhs) = delete;

    void init() {
        ::memset(recP_, 0, sizeof(RecT));
        setExists();
    }

    template <typename T>
    const T *ptr() const { return reinterpret_cast<const T *>(recP_); }
    template <typename T>
    T *ptr() { return reinterpret_cast<T *>(recP_); }

    uint64_t ioAddress() const override { return recP_->io_address; }
    uint16_t ioBlocks() const override { return recP_->io_blocks; }
    uint64_t endIoAddress() const { return ioAddress() + ioBlocks(); }
    size_t rawSize() const override { return sizeof(*recP_); }
    const char *rawData() const override { return ptr<char>(); }
    char *rawData() { return ptr<char>(); }
    struct walb_diff_record *rawRecord() { return recP_; }
    const struct walb_diff_record *rawRecord() const { return recP_; }

    uint8_t compressionType() const { return recP_->compression_type; }
    bool isCompressed() const { return compressionType() != ::WALB_DIFF_CMPR_NONE; }
    uint32_t dataOffset() const { return recP_->data_offset; }
    uint32_t dataSize() const { return recP_->data_size; }
    uint32_t checksum() const { return recP_->checksum; }

    bool exists() const {
        return (recP_->flags & WALB_DIFF_FLAG(EXIST)) != 0;
    }
    bool isAllZero() const {
        return (recP_->flags & WALB_DIFF_FLAG(ALLZERO)) != 0;
    }
    bool isDiscard() const {
        return (recP_->flags & WALB_DIFF_FLAG(DISCARD)) != 0;
    }
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
            if (dataSize() != 0) {
                LOGd("dataSize must be 0.\n");
            }
            return true;
        }
        if (::WALB_DIFF_CMPR_MAX <= compressionType()) {
            LOGd("compression type is invalid.\n");
            return false;
        }
        if (ioBlocks() == 0) {
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
                  ioAddress(), ioBlocks(),
                  compressionType(), dataOffset(), dataSize(),
                  checksum(), exists(), isAllZero(), isDiscard());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "wdiff_rec:\t%" PRIu64 "\t%u\t%u\t%u\t%u\t%08x\t%d%d%d\n",
                  ioAddress(), ioBlocks(),
                  compressionType(), dataOffset(), dataSize(),
                  checksum(), exists(), isAllZero(), isDiscard());
    }

    void setIoAddress(uint64_t ioAddress) { recP_->io_address = ioAddress; }
    void setIoBlocks(uint16_t ioBlocks) { recP_->io_blocks = ioBlocks; }
    void setCompressionType(uint8_t type) { recP_->compression_type = type; }
    void setDataOffset(uint32_t offset) { recP_->data_offset = offset; }
    void setDataSize(uint32_t size) { recP_->data_size = size; }
    void setChecksum(uint32_t csum) { recP_->checksum = csum; }

    void setExists() {
        recP_->flags |= WALB_DIFF_FLAG(EXIST);
    }
    void clearExists() {
        recP_->flags &= ~WALB_DIFF_FLAG(EXIST);
    }
    void setNormal() {
        recP_->flags &= ~WALB_DIFF_FLAG(ALLZERO);
        recP_->flags &= ~WALB_DIFF_FLAG(DISCARD);
    }
    void setAllZero() {
        recP_->flags |= WALB_DIFF_FLAG(ALLZERO);
        recP_->flags &= ~WALB_DIFF_FLAG(DISCARD);
    }
    void setDiscard() {
        recP_->flags &= ~WALB_DIFF_FLAG(ALLZERO);
        recP_->flags |= WALB_DIFF_FLAG(DISCARD);
    }
};

using RecordWrap = RecordWrapT<struct walb_diff_record>;
using RecordWrapConst = RecordWrapT<const struct walb_diff_record>;

/**
 * Class for struct walb_diff_record.
 */
class RecordRaw : public RecordWrap
{
private:
    struct walb_diff_record rec_;

public:
    /**
     * Default.
     */
    RecordRaw() : RecordWrap(&rec_), rec_() {
        init();
    }

    /**
     * Clone.
     */
    RecordRaw(const RecordRaw &rec, bool isCheck = true)
        : RecordWrap(&rec_), rec_(rec.rec_) {
        if (isCheck && !isValid()) { throw RT_ERR("invalid record."); }
    }

    /**
     * Convert.
     */
    RecordRaw(const struct walb_diff_record &rawRec, bool isCheck = true)
        : RecordWrap(&rec_), rec_(rawRec) {
        if (isCheck && !isValid()) { throw RT_ERR("invalid record."); }
    }

    /**
     * For raw data.
     */
    RecordRaw(const char *data, size_t size)
        : RecordWrap(&rec_), rec_(*reinterpret_cast<const struct walb_diff_record *>(data)) {
        if (size != sizeof(rec_)) {
            throw RT_ERR("size is invalid.");
        }
    }

    /**
     * Split a record into two records
     * where the first record's ioBlocks will be a specified one.
     *
     * CAUSION:
     *   The checksum of splitted records will be invalid state.
     *   Only non-compressed records can be splitted.
     */
    std::pair<RecordRaw, RecordRaw> split(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0 || ioBlocks() <= ioBlocks0) {
            throw RT_ERR("split: ioBlocks0 is out or range.");
        }
        if (isCompressed()) {
            throw RT_ERR("split: compressed data can not be splitted.");
        }
        RecordRaw r0(*this), r1(*this);
        uint16_t ioBlocks1 = ioBlocks() - ioBlocks0;
        r0.setIoBlocks(ioBlocks0);
        r1.setIoBlocks(ioBlocks1);
        r1.setIoAddress(ioAddress() + ioBlocks0);
        if (isNormal()) {
            r0.setDataSize(ioBlocks0 * LOGICAL_BLOCK_SIZE);
            r1.setDataSize(ioBlocks1 * LOGICAL_BLOCK_SIZE);
        }
        return std::make_pair(r0, r1);
    }
    /**
     * Split a record into several records
     * where all splitted records' ioBlocks will be <= a specified one.
     *
     * CAUSION:
     *   The checksum of splitted records will be invalid state.
     *   Only non-compressed records can be splitted.
     */
    std::vector<RecordRaw> splitAll(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0) {
            throw RT_ERR("splitAll: ioBlocks0 must not be 0.");
        }
        if (isCompressed()) {
            throw RT_ERR("splitAll: compressed data can not be splitted.");
        }
        std::vector<RecordRaw> v;
        uint64_t addr = ioAddress();
        uint16_t remaining = ioBlocks();
        while (0 < remaining) {
            uint16_t blks = std::min(ioBlocks0, remaining);
            v.emplace_back(*this);
            v.back().setIoAddress(addr);
            v.back().setIoBlocks(blks);
            if (isNormal()) {
                v.back().setDataSize(blks * LOGICAL_BLOCK_SIZE);
            }
            addr += blks;
            remaining -= blks;
        }
        assert(!v.empty());
        return v;
    }
};

/**
 * Io data.
 * This does not manage data array.
 */
class IoWrap
{
protected:
    uint16_t ioBlocks_; /* [logical block]. */
    int compressionType_;
    const char *dataP_;
    size_t dataSize_;

public:
    IoWrap()
        : ioBlocks_(0), compressionType_(::WALB_DIFF_CMPR_NONE)
        , dataP_(nullptr), dataSize_(0) {}
    IoWrap(uint16_t ioBlocks, int compressionType, const char *data, size_t size)
        : ioBlocks_(ioBlocks), compressionType_(compressionType)
        , dataP_(data), dataSize_(size) {}
    IoWrap(const IoWrap &rhs)
        : IoWrap(rhs.ioBlocks_, rhs.compressionType_, rhs.dataP_, rhs.dataSize_) {}
    IoWrap(IoWrap &&) = delete;
    virtual ~IoWrap() noexcept = default;

    IoWrap &operator=(const IoWrap &rhs) {
        ioBlocks_ = rhs.ioBlocks_;
        compressionType_ = rhs.compressionType_;
        dataP_ = rhs.dataP_;
        dataSize_ = rhs.dataSize_;
        return *this;
    }
    IoWrap &operator=(IoWrap &&rhs) = delete;

    uint16_t ioBlocks() const { return ioBlocks_; }
    void setIoBlocks(uint16_t ioBlocks) { ioBlocks_ = ioBlocks; }
    int compressionType() const { return compressionType_; }
    void setCompressionType(int type) { compressionType_ = type; }
    bool isCompressed() const { return compressionType_ != ::WALB_DIFF_CMPR_NONE; }

    bool isValid() const {
        if (ioBlocks_ == 0) {
            if (dataP_ != nullptr || dataSize_ != 0) {
                LOGd("Data is not empty.\n");
                return false;
            }
            return true;
        } else {
            if (isCompressed()) {
                if (dataP_ == nullptr) {
                    LOGd("data pointer is null\n");
                    return false;
                }
                if (dataSize_ == 0) {
                    LOGd("data size is not 0: %zu\n", dataSize_);
                    return false;
                }
                return true;
            } else {
                if (dataSize_ != ioBlocks_ * LOGICAL_BLOCK_SIZE) {
                    LOGd("dataSize is not the same: %zu %u\n"
                         , dataSize_, ioBlocks_ * LOGICAL_BLOCK_SIZE);
                    return false;
                }
                return true;
            }
        }
    }

    const char *rawData(size_t offset = 0) const { return dataP_ + offset; }
    size_t rawSize() const { return dataSize_; }

    /**
     * Calculate checksum.
     */
    uint32_t calcChecksum() const {
        if (!rawData()) { return 0; }
        return cybozu::util::calcChecksum(rawData(), rawSize(), 0);
    }

    /**
     * Calculate whether all-zero or not.
     */
    bool calcIsAllZero() const {
        if (isCompressed() || rawSize() == 0) { return false; }
        assert(rawSize() % LOGICAL_BLOCK_SIZE == 0);
        const uint64_t *p = reinterpret_cast<const uint64_t *>(rawData());
        for (size_t i = 0; i < rawSize() / sizeof(*p); i++) {
            if (p[i] != 0) { return false; }
        }
        return true;
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "ioBlocks %u\n"
                  "type %d\n"
                  "size %zu\n"
                  "checksum %0x\n"
                  , ioBlocks()
                  , compressionType()
                  , rawSize()
                  , calcChecksum());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "ioBlocks %u type %d size %zu checksum %0x\n"
                  , ioBlocks()
                  , compressionType()
                  , rawSize()
                  , calcChecksum());
    }
};

/**
 * Block diff for an IO.
 */
class IoData : public IoWrap
{
private:
    /* You must call resetData() for consistency of
       dataP_ and dataSize_ after changing data_. */
    std::vector<char> data_;

public:
    IoData()
        : IoWrap()
        , data_() {}

    IoData(const IoData &rhs) : IoWrap(rhs), data_(rhs.data_) {
        resetData();
    }
    IoData(IoData &&rhs) : IoWrap(rhs), data_(std::move(rhs.data_)) {
        resetData();
    }
    ~IoData() noexcept = default;

    IoData &operator=(const IoData &rhs) {
        ioBlocks_ = rhs.ioBlocks_;
        compressionType_ = rhs.compressionType_;
        data_ = rhs.data_;
        resetData();
        return *this;
    }
    IoData &operator=(IoData &&rhs) {
        ioBlocks_ = rhs.ioBlocks_;
        compressionType_ = rhs.compressionType_;
        data_ = std::move(rhs.data_);
        resetData();
        return *this;
    }

    bool isValid() const {
        if (!IoWrap::isValid()) return false;
        if (dataP_ != &data_[0] || dataSize_ != data_.size()) {
            LOGd("resetData() must be called.\n");
            return false;
        }
        return true;
    }

    void copyFrom(const void *data, size_t size) {
        data_.resize(size);
        ::memcpy(&data_[0], data, size);
        resetData();
    }
    void moveFrom(std::vector<char> &&data) {
        data_ = std::move(data);
        resetData();
    }
    const char *rawData(size_t offset = 0) const { return &data_[offset]; }
    char *rawData(size_t offset = 0) { return &data_[offset]; }
    std::vector<char> forMove() {
        std::vector<char> v = std::move(data_);
        resetData();
        return v;
    }
    void resizeData(size_t size) {
        data_.resize(size);
        resetData();
    }
private:
    /**
     * You must call this after changing data_.
     */
    void resetData() {
        dataP_ = &data_[0];
        dataSize_ = data_.size();
    }
};

/**
 * Compress an IO data.
 * Supported algorithms: snappy.
 */
static inline IoData compressIoData(const IoWrap &io0, int type)
{
    if (io0.isCompressed()) {
        throw RT_ERR("Could not compress already compressed diff IO.");
    }
    if (type != ::WALB_DIFF_CMPR_SNAPPY) {
        throw RT_ERR("Currently only snappy is supported.");
    }
    if (io0.ioBlocks() == 0) {
        return IoData();
    }
    assert(io0.isValid());
    IoData io1;
    io1.setIoBlocks(io0.ioBlocks());
    io1.setCompressionType(type);
    io1.resizeData(snappy::MaxCompressedLength(io0.rawSize()));
    size_t size;
    snappy::RawCompress(io0.rawData(), io0.rawSize(), io1.rawData(), &size);
    io1.resizeData(size);
    return io1;
}

/**
 * Uncompress an IO data.
 * Supported algorithms: snappy.
 */
static inline IoData uncompressIoData(const IoWrap &io0)
{
    if (!io0.isCompressed()) {
        throw RT_ERR("Need not uncompress already uncompressed diff IO.");
    }
    if (io0.compressionType() != ::WALB_DIFF_CMPR_SNAPPY) {
        throw RT_ERR("Currently only snappy is supported.");
    }
    if (io0.ioBlocks() == 0) {
        return IoData();
    }
    IoData io1;
    io1.setIoBlocks(io0.ioBlocks());
    io1.resizeData(io0.ioBlocks() * LOGICAL_BLOCK_SIZE);
    size_t size;
    if (!snappy::GetUncompressedLength(io0.rawData(), io0.rawSize(), &size)) {
        throw RT_ERR("snappy::GetUncompressedLength() failed.");
    }
    if (size != io1.rawSize()) {
        throw RT_ERR("Uncompressed data size is invalid %zu %zu.", size, io1.rawSize());
    }
    if (!snappy::RawUncompress(io0.rawData(), io0.rawSize(), io1.rawData())) {
        throw RT_ERR("snappy::RawUncompress() failed.");
    }
    return io1;
}

/**
 * Split an IO into two IOs
 * where the first IO's ioBlocks will be a specified one.
 *
 * CAUSION:
 *   Compressed IO can not be splitted.
 */
static inline std::pair<IoData, IoData> splitIoData(const IoWrap &io0, uint16_t ioBlocks0)
{
    if (ioBlocks0 == 0 || io0.ioBlocks() <= ioBlocks0) {
        throw RT_ERR("split: ioBlocks0 is out or range.");
    }
    if (io0.isCompressed()) {
        throw RT_ERR("split: compressed IO can not be splitted.");
    }
    assert(io0.isValid());

    IoData r0, r1;
    uint16_t ioBlocks1 = io0.ioBlocks() - ioBlocks0;
    r0.setIoBlocks(ioBlocks0);
    r1.setIoBlocks(ioBlocks1);
    size_t size0 = ioBlocks0 * LOGICAL_BLOCK_SIZE;
    size_t size1 = ioBlocks1 * LOGICAL_BLOCK_SIZE;
    r0.resizeData(size0);
    r1.resizeData(size1);
    ::memcpy(r0.rawData(), io0.rawData(), size0);
    ::memcpy(r1.rawData(), io0.rawData(size0), size1);

    return std::make_pair(std::move(r0), std::move(r1));
}

/**
 * Split an IO into multiple IOs
 * each of which io size is not more than a specified one.
 *
 * CAUSION:
 *   Compressed IO can not be splitted.
 */
static inline std::vector<IoData> splitIoDataAll(const IoWrap &io0, uint16_t ioBlocks0)
{
    if (ioBlocks0 == 0) {
        throw RT_ERR("splitAll: ioBlocks0 must not be 0.");
    }
    if (io0.isCompressed()) {
        throw RT_ERR("splitAll: compressed IO can not be splitted.");
    }
    assert(io0.isValid());

    std::vector<IoData> v;
    uint16_t remaining = io0.ioBlocks();
    size_t off = 0;
    while (0 < remaining) {
        IoData io;
        uint16_t blks = std::min(remaining, ioBlocks0);
        size_t size = blks * LOGICAL_BLOCK_SIZE;
        io.setIoBlocks(blks);
        io.resizeData(size);
        ::memcpy(io.rawData(), io0.rawData(off), size);
        v.push_back(std::move(io));
        remaining -= blks;
        off += size;
    }
    assert(off == io0.rawSize());
    assert(!v.empty());

    return v;
}

}} //namesapce walb::diff
