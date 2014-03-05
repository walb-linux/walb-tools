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

/**
 * Interface.
 */
class Record : public block_diff::BlockDiffKey
{
public:

    Record() = default;
    Record(const Record &rhs) = delete;
    DISABLE_MOVE(Record);
    Record &operator=(const Record &rhs) {
        record() = rhs.record();
        return *this;
    }
    virtual ~Record() noexcept = default;

    /*
     * You must implement these member functions.
     */
    virtual const struct walb_diff_record &record() const = 0;
    virtual struct walb_diff_record &record() = 0;

    /*
     * Utilities.
     */
    void init() {
        ::memset(&record(), 0, sizeof(struct walb_diff_record));
        setExists();
    }
    template <typename T>
    const T *ptr() const { return reinterpret_cast<const T *>(&record()); }
    template <typename T>
    T *ptr() { return reinterpret_cast<T *>(&record()); }

    uint64_t ioAddress() const override { return record().io_address; }
    uint16_t ioBlocks() const override { return record().io_blocks; }
    uint64_t endIoAddress() const { return ioAddress() + ioBlocks(); }
    size_t rawSize() const override { return sizeof(struct walb_diff_record); }
    const char *rawData() const override { return ptr<char>(); }
    char *rawData() { return ptr<char>(); }

    uint8_t compressionType() const { return record().compression_type; }
    bool isCompressed() const { return compressionType() != ::WALB_DIFF_CMPR_NONE; }
    uint32_t dataOffset() const { return record().data_offset; }
    uint32_t dataSize() const { return record().data_size; }
    uint32_t checksum() const { return record().checksum; }

    bool exists() const {
        return (record().flags & WALB_DIFF_FLAG(EXIST)) != 0;
    }
    bool isAllZero() const {
        return (record().flags & WALB_DIFF_FLAG(ALLZERO)) != 0;
    }
    bool isDiscard() const {
        return (record().flags & WALB_DIFF_FLAG(DISCARD)) != 0;
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

    void setIoAddress(uint64_t ioAddress) { record().io_address = ioAddress; }
    void setIoBlocks(uint16_t ioBlocks) { record().io_blocks = ioBlocks; }
    void setCompressionType(uint8_t type) { record().compression_type = type; }
    void setDataOffset(uint32_t offset) { record().data_offset = offset; }
    void setDataSize(uint32_t size) { record().data_size = size; }
    void setChecksum(uint32_t csum) { record().checksum = csum; }

    void setExists() {
        record().flags |= WALB_DIFF_FLAG(EXIST);
    }
    void clearExists() {
        record().flags &= ~WALB_DIFF_FLAG(EXIST);
    }
    void setNormal() {
        record().flags &= ~WALB_DIFF_FLAG(ALLZERO);
        record().flags &= ~WALB_DIFF_FLAG(DISCARD);
    }
    void setAllZero() {
        record().flags |= WALB_DIFF_FLAG(ALLZERO);
        record().flags &= ~WALB_DIFF_FLAG(DISCARD);
    }
    void setDiscard() {
        record().flags &= ~WALB_DIFF_FLAG(ALLZERO);
        record().flags |= WALB_DIFF_FLAG(DISCARD);
    }
};

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

struct DiffRecord : public block_diff::BlockDiffKey2<walb_diff_record> {
    void init() {
        initRec(*this);
    }
//    uint64_t ioAddress() const override { return io_address; }
//    uint16_t ioBlocks() const override { return io_blocks; }
    uint64_t endIoAddress() const { return io_address + io_blocks; }
//    size_t rawSize() const override { return sizeof(struct walb_diff_record); }

//    uint8_t compressionType() const { return record().compression_type; }
    bool isCompressed() const { return compression_type != ::WALB_DIFF_CMPR_NONE; }
//    uint32_t dataOffset() const { return data_offset; }
//    uint32_t dataSize() const { return data_size; }
//    uint32_t checksum() const { return checksum; }

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
};

/**
 * Class for struct walb_diff_record.
 */
class RecordRaw : public Record
{
private:
    struct walb_diff_record rec_;

public:
    struct walb_diff_record &record() override { return rec_; }
    const struct walb_diff_record &record() const override { return rec_; }

    /**
     * Default.
     */
    RecordRaw() : Record(), rec_() { init(); }
    /**
     * Clone.
     */
    RecordRaw(const RecordRaw &rec, bool isCheck = true)
        : Record(), rec_(rec.rec_) {
        if (isCheck) check();
    }
    /**
     * Convert.
     */
    RecordRaw(const struct walb_diff_record &rawRec, bool isCheck = true)
        : Record(), rec_(rawRec) {
        if (isCheck) check();
    }
    /**
     * Convert.
     */
    RecordRaw(const Record &rec, bool isCheck = true)
        : Record(), rec_(rec.record()) {
        if (isCheck) check();
    }
    /**
     * For raw data.
     */
    RecordRaw(const char *data, size_t size)
        : Record(), rec_(*reinterpret_cast<const struct walb_diff_record *>(data)) {
        if (size != sizeof(rec_)) {
            throw RT_ERR("size is invalid.");
        }
    }
private:
    void check() const {
        if (!isValid()) throw RT_ERR("invalid record.");
    }
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
 * Io data.
 * This does not manage data array.
 */
struct IoWrap
{
    uint16_t ioBlocks; /* [logical block]. */
    int compressionType;
    const char *data;
    size_t size;

    IoWrap()
        : ioBlocks(0), compressionType(::WALB_DIFF_CMPR_NONE)
        , data(nullptr), size(0) {}

    bool isCompressed() const { return compressionType != ::WALB_DIFF_CMPR_NONE; }

    bool empty() const { return ioBlocks == 0; }

    void set0(const walb_diff_record &rec) {
        if (isNormalRec(rec)) {
            ioBlocks = rec.io_blocks;
            compressionType = rec.compression_type;
        } else {
            ioBlocks = 0;
            compressionType = ::WALB_DIFF_CMPR_NONE;
        }
    }
    void set(const walb_diff_record &rec0, const char *data, size_t size) {
        set0(rec0);
        this->data = data;
        this->size = size;
    }

    bool isValid() const {
        if (empty()) {
            if (data != nullptr || size != 0) {
                LOGd("Data is not empty.\n");
                return false;
            }
            return true;
        } else {
            if (isCompressed()) {
                if (data == nullptr) {
                    LOGd("data pointer is null\n");
                    return false;
                }
                if (size == 0) {
                    LOGd("data size is not 0: %zu\n", size);
                    return false;
                }
                return true;
            } else {
                if (size != ioBlocks * LOGICAL_BLOCK_SIZE) {
                    LOGd("dataSize is not the same: %zu %u\n"
                         , size, ioBlocks * LOGICAL_BLOCK_SIZE);
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
        assert(data);
        assert(size > 0);
        return cybozu::util::calcChecksum(data, size, 0);
    }

    /**
     * Calculate whether all-zero or not.
     */
    bool calcIsAllZero() const {
        if (isCompressed() || size == 0) { return false; }
        assert(size % LOGICAL_BLOCK_SIZE == 0);
        return cybozu::util::calcIsAllZero(data, size);
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "ioBlocks %u\n"
                  "type %d\n"
                  "size %zu\n"
                  "checksum %0x\n"
                  , ioBlocks
                  , compressionType
                  , size
                  , calcChecksum());
    }
    void printOneline(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "ioBlocks %u type %d size %zu checksum %0x\n"
                  , ioBlocks
                  , compressionType
                  , size
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
       data and size after changing data_. */
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
    IoData(uint16_t ioBlocks, int compressionType, const char *data = 0, size_t size = 0) {
        setBlocksAndType(ioBlocks, compressionType);
        if (data) data_.assign(data, data + size);
        resetData();
    }
    void clear() {
        ioBlocks = 0;
        compressionType = ::WALB_DIFF_CMPR_NONE;
        data_.clear();
        resetData();
    }
    void setBlocksAndType(uint16_t ioBlocks, int compressionType) {
        this->ioBlocks = ioBlocks;
        this->compressionType = compressionType;
    }
    /*
        set data
        return written size
        don't write larger than reserveSize
        size_t setter(char *data);
    */
    template<class Writter>
    void setByWritter(size_t reserveSize, Writter writter) {
        data_.resize(reserveSize);
        size_t writtenSize = writter(&data_[0]);
        data_.resize(writtenSize);
        resetData();
    }

    IoData &operator=(const IoData &rhs) {
        setBlocksAndType(rhs.ioBlocks, rhs.compressionType);
        data_ = rhs.data_;
        resetData();
        return *this;
    }
    IoData &operator=(IoData &&rhs) {
        setBlocksAndType(rhs.ioBlocks, rhs.compressionType);
        data_ = std::move(rhs.data_);
        resetData();
        return *this;
    }

    void set(const struct walb_diff_record &rec0) {
        IoWrap::set0(rec0);
        data_.resize(rec0.data_size);
        resetData();
    }

    bool isValid() const {
        if (!IoWrap::isValid()) return false;
        if (data != &data_[0] || size != data_.size()) {
            LOGd("resetData() must be called.\n");
            return false;
        }
        return true;
    }

    void moveFrom(std::vector<char> &&data) {
        data_ = std::move(data);
        resetData();
    }
    const char *rawData() const { return &data_[0]; }
    char *rawData() { return &data_[0]; }
    std::vector<char> forMove() {
        std::vector<char> v = std::move(data_);
        data_.clear();
        resetData();
        return v;
    }
private:
    /**
     * You must call this after changing data_.
     */
    void resetData() {
        data = &data_[0];
        size = data_.size();
    }
};

/**
 * Compress an IO data.
 * Supported algorithms: snappy.
 */
inline IoData compressIoData(const IoWrap &io0, int type)
{
    if (io0.isCompressed()) {
        throw RT_ERR("Could not compress already compressed diff IO.");
    }
    if (type != ::WALB_DIFF_CMPR_SNAPPY) {
        throw RT_ERR("Currently only snappy is supported.");
    }
    if (io0.ioBlocks == 0) {
        return IoData();
    }
    assert(io0.isValid());
    IoData io1(io0.ioBlocks, type);
    io1.setByWritter(snappy::MaxCompressedLength(io0.size), [&](char *p) {
        size_t size;
        snappy::RawCompress(io0.data, io0.size, p, &size);
        return size;
    });
    return io1;
}

/**
 * Uncompress an IO data.
 * Supported algorithms: snappy.
 */
inline IoData uncompressIoData(const IoWrap &io0)
{
    if (!io0.isCompressed()) {
        throw RT_ERR("Need not uncompress already uncompressed diff IO.");
    }
    walb::Uncompressor dec(io0.compressionType);
    const size_t decSize = io0.ioBlocks * LOGICAL_BLOCK_SIZE;
    IoData io1(io0.ioBlocks, WALB_DIFF_CMPR_NONE);
    io1.setByWritter(decSize, [&](char *p) {
        size_t size = dec.run(p, decSize, io0.data, io0.size);
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
inline std::vector<IoData> splitIoDataAll(const IoWrap &io0, uint16_t ioBlocks0)
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
        v.emplace_back(blks, WALB_DIFF_CMPR_NONE, io0.data + off, size);
        remaining -= blks;
        off += size;
    }
    assert(off == io0.size);
    assert(!v.empty());

    return v;
}

}} //namesapce walb::diff
