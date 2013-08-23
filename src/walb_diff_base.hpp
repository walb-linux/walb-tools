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

#ifndef WALB_DIFF_BASE_HPP
#define WALB_DIFF_BASE_HPP

static_assert(::WALB_DIFF_FLAGS_MAX <= 8, "Too many walb diff flags.");
static_assert(::WALB_DIFF_CMPR_MAX <= 256, "Too many walb diff cmpr types.");

namespace walb {
namespace diff {

/**
 * Class for struct walb_diff_record.
 */
class RecordRaw
    : public block_diff::BlockDiffKey
{
private:
    struct walb_diff_record rec_;

public:
    /**
     * Default.
     */
    RecordRaw()
        : rec_() {
        init();
    }

    /**
     * Clone.
     */
    RecordRaw(const RecordRaw &rec, bool isCheck = true)
        : rec_(rec.rec_) {
        if (isCheck && !isValid()) { throw RT_ERR("invalid record."); }
    }

    /**
     * Convert.
     */
    RecordRaw(const struct walb_diff_record &rawRec, bool isCheck = true)
        : rec_(rawRec) {
        if (isCheck && !isValid()) { throw RT_ERR("invalid record."); }
    }

    /**
     * For raw data.
     */
    RecordRaw(const char *data, size_t size)
        : rec_(*reinterpret_cast<const struct walb_diff_record *>(data)) {
        if (size != sizeof(rec_)) {
            throw RT_ERR("size is invalid.");
        }
    }

    virtual ~RecordRaw() noexcept = default;

    RecordRaw &operator=(const RecordRaw &rhs) {
        rec_ = rhs.rec_;
        return *this;
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

    template <typename T>
    const T *ptr() const { return reinterpret_cast<const T *>(&rec_); }
    template <typename T>
    T *ptr() { return reinterpret_cast<T *>(&rec_); }

    uint64_t ioAddress() const override { return rec_.io_address; }
    uint16_t ioBlocks() const override { return rec_.io_blocks; }
    uint64_t endIoAddress() const { return ioAddress() + ioBlocks(); }
    size_t rawSize() const override { return sizeof(rec_); }
    const char *rawData() const override { return ptr<char>(); }
    char *rawData() { return ptr<char>(); }
    struct walb_diff_record *rawRecord() { return &rec_; }
    const struct walb_diff_record *rawRecord() const { return &rec_; }

    uint8_t compressionType() const { return rec_.compression_type; }
    bool isCompressed() const { return compressionType() != ::WALB_DIFF_CMPR_NONE; }
    uint32_t dataOffset() const { return rec_.data_offset; }
    uint32_t dataSize() const { return rec_.data_size; }
    uint32_t checksum() const { return rec_.checksum; }

    bool exists() const {
        return (rec_.flags & (1U << ::WALB_DIFF_FLAG_EXIST)) != 0;
    }
    bool isAllZero() const {
        return (rec_.flags & (1U << ::WALB_DIFF_FLAG_ALLZERO)) != 0;
    }
    bool isDiscard() const {
        return (rec_.flags & (1U << ::WALB_DIFF_FLAG_DISCARD)) != 0;
    }
    bool isNormal() const {
        return !isAllZero() && !isDiscard();
    }

    void print(::FILE *fp) const {
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

    void print() const { print(::stdout); }

    void printOneline(::FILE *fp) const {
        ::fprintf(fp, "wdiff_rec:\t%" PRIu64 "\t%u\t%u\t%u\t%u\t%08x\t%d%d%d\n",
                  ioAddress(), ioBlocks(),
                  compressionType(), dataOffset(), dataSize(),
                  checksum(), exists(), isAllZero(), isDiscard());
    }

    void printOneline() const { printOneline(::stdout); }

    void setIoAddress(uint64_t ioAddress) { rec_.io_address = ioAddress; }
    void setIoBlocks(uint16_t ioBlocks) { rec_.io_blocks = ioBlocks; }
    void setCompressionType(uint8_t type) { rec_.compression_type = type; }
    void setDataOffset(uint32_t offset) { rec_.data_offset = offset; }
    void setDataSize(uint32_t size) { rec_.data_size = size; }
    void setChecksum(uint32_t csum) { rec_.checksum = csum; }

    void setExists() {
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_EXIST);
    }
    void clearExists() {
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_EXIST);
    }

    void setNormal() {
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_ALLZERO);
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_DISCARD);
    }
    void setAllZero() {
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_ALLZERO);
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_DISCARD);
    }
    void setDiscard() {
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_ALLZERO);
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_DISCARD);
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
        return std::move(v);
    }
private:
    void init() {
        ::memset(&rec_, 0, sizeof(rec_));
        setExists();
    }
};

/**
 * Block diff for an IO.
 */
class IoData
{
private:
    uint16_t ioBlocks_; /* [logical block]. */
    int compressionType_;
    std::vector<char> data_;

public:
    IoData()
        : ioBlocks_(0)
        , compressionType_(::WALB_DIFF_CMPR_NONE)
        , data_() {
    }

    IoData(const IoData &) = default;
    IoData(IoData &&) = default;
    virtual ~IoData() noexcept = default;

    IoData &operator=(const IoData &rhs) {
        ioBlocks_ = rhs.ioBlocks_;
        compressionType_ = rhs.compressionType_;
        data_ = rhs.data_;
        return *this;
    }
    IoData &operator=(IoData &&rhs) {
        ioBlocks_ = rhs.ioBlocks_;
        compressionType_ = rhs.compressionType_;
        data_ = std::move(rhs.data_);
        return *this;
    }

    uint16_t ioBlocks() const { return ioBlocks_; }
    void setIoBlocks(uint16_t ioBlocks) { ioBlocks_ = ioBlocks; }
    int compressionType() const { return compressionType_; }
    void setCompressionType(int type) { compressionType_ = type; }
    bool isCompressed() const { return compressionType_ != ::WALB_DIFF_CMPR_NONE; }

    bool isValid() const {
        if (ioBlocks_ == 0) {
            if (!data_.empty()) {
                LOGd("Data is not empty.\n");
                return false;
            }
            return true;
        } else {
            if (isCompressed()) {
                if (data_.size() == 0) {
                    LOGd("data size is not 0: %zu\n", data_.size());
                    return false;
                }
                return true;
            } else {
                if (data_.size() != ioBlocks_ * LOGICAL_BLOCK_SIZE) {
                    LOGd("dataSize is not the same: %zu %u\n"
                         , data_.size(), ioBlocks_ * LOGICAL_BLOCK_SIZE);
                    return false;
                }
                return true;
            }
        }
    }

    void copyFrom(const void *data, size_t size) {
        data_.resize(size);
        ::memcpy(&data_[0], data, size);
    }
    void moveFrom(std::vector<char> &&data) {
        data_ = std::move(data);
    }
    const std::vector<char> &data() const { return data_; }
    std::vector<char> &data() { return data_; }
    std::vector<char> &&forMove() { return std::move(data_); }

    const char *rawData() const { return &data_[0]; }
    char *rawData() { return &data_[0]; }
    size_t rawSize() const { return data_.size(); }

    /**
     * Calculate checksum.
     */
    uint32_t calcChecksum() const {
        if (!rawData()) { return 0; }
        return cybozu::util::calcChecksum(rawData(), rawSize(), 0);
    }

    /**
     * Split the IO into two IOs
     * where the first IO's ioBlocks will be a specified one.
     *
     * CAUSION:
     *   Compressed IO can not be splitted.
     */
    std::pair<IoData, IoData> split(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0 || ioBlocks() <= ioBlocks0) {
            throw RT_ERR("split: ioBlocks0 is out or range.");
        }
        if (isCompressed()) {
            throw RT_ERR("split: compressed IO can not be splitted.");
        }
        assert(isValid());

        IoData r0, r1;
        uint16_t ioBlocks1 = ioBlocks() - ioBlocks0;
        r0.setIoBlocks(ioBlocks0);
        r1.setIoBlocks(ioBlocks1);
        size_t size0 = ioBlocks0 * LOGICAL_BLOCK_SIZE;
        size_t size1 = ioBlocks1 * LOGICAL_BLOCK_SIZE;
        r0.data_.resize(size0);
        r1.data_.resize(size1);
        ::memcpy(&r0.data_[0], &data_[0], size0);
        ::memcpy(&r1.data_[0], &data_[size0], size1);

        return std::make_pair(std::move(r0), std::move(r1));
    }

    std::vector<IoData> splitAll(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0) {
            throw RT_ERR("splitAll: ioBlocks0 must not be 0.");
        }
        if (isCompressed()) {
            throw RT_ERR("splitAll: compressed IO can not be splitted.");
        }
        assert(isValid());

        std::vector<IoData> v;
        uint16_t remaining = ioBlocks();
        size_t off = 0;
        while (0 < remaining) {
            IoData io;
            uint16_t blks = std::min(remaining, ioBlocks0);
            size_t size = blks * LOGICAL_BLOCK_SIZE;
            io.setIoBlocks(blks);
            io.data_.resize(size);
            ::memcpy(&io.data_[0], &data_[off], size);
            v.push_back(std::move(io));
            remaining -= blks;
            off += size;
        }
        assert(off == data_.size());
        assert(!v.empty());

        return std::move(v);
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

    /**
     * Compress an IO data.
     * Supported algorithms: snappy.
     */
    IoData compress(int type) const {
        if (isCompressed()) {
            throw RT_ERR("Could not compress already compressed diff IO.");
        }
        if (type != ::WALB_DIFF_CMPR_SNAPPY) {
            throw RT_ERR("Currently only snappy is supported.");
        }
        if (ioBlocks() == 0) {
            return IoData();
        }
        assert(isValid());
        IoData io;
        io.setIoBlocks(ioBlocks());
        io.setCompressionType(type);
        io.data_.resize(snappy::MaxCompressedLength(rawSize()));
        size_t size;
        snappy::RawCompress(rawData(), rawSize(), io.rawData(), &size);
        io.data_.resize(size);
        return std::move(io);
    }

    /**
     * Uncompress an IO data.
     * Supported algorithms: snappy.
     */
    IoData uncompress() const {
        if (!isCompressed()) {
            throw RT_ERR("Need not uncompress already uncompressed diff IO.");
        }
        if (compressionType() != ::WALB_DIFF_CMPR_SNAPPY) {
            throw RT_ERR("Currently only snappy is supported.");
        }
        if (ioBlocks() == 0) {
            return IoData();
        }
        IoData io;
        io.setIoBlocks(ioBlocks());
        io.data_.resize(ioBlocks() * LOGICAL_BLOCK_SIZE);
        size_t size;
        if (!snappy::GetUncompressedLength(rawData(), rawSize(), &size)) {
            throw RT_ERR("snappy::GetUncompressedLength() failed.");
        }
        if (size != io.rawSize()) {
            throw RT_ERR("Uncompressed data size is not invaid.");
        }
        if (!snappy::RawUncompress(rawData(), rawSize(), io.rawData())) {
            throw RT_ERR("snappy::RawUncompress() failed.");
        }
        return std::move(io);
    }

    void print(::FILE *fp) const {
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
    void print() const { print(::stdout); }
    void printOneline(::FILE *fp) const {
        ::fprintf(fp, "ioBlocks %u type %d size %zu checksum %0x\n"
                  , ioBlocks()
                  , compressionType()
                  , rawSize()
                  , calcChecksum());
    }
    void printOneline() const { printOneline(::stdout); }
};

}} //namesapce walb::diff

#endif /* WALB_DIFF_BASE_HPP */
