/**
 * @file
 * @brief walb diff utilities.
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

#include "walb/block_size.h"

#ifndef WALB_DIFF_UTIL_HPP
#define WALB_DIFF_UTIL_HPP

static_assert(::WALB_DIFF_FLAGS_MAX <= 8, "Too many walb diff flags.");
static_assert(::WALB_DIFF_CMPR_MAX <= 256, "Too many walb diff cmpr types.");

namespace walb {
namespace diff {

/**
 * Class for struct walb_diff_record.
 */
class WalbDiffRecord
    : public block_diff::BlockDiffKey
{
private:
    struct walb_diff_record rec_;

public:
    /**
     * Default.
     */
    WalbDiffRecord()
        : rec_() {
        init();
    }

    /**
     * Clone.
     */
    WalbDiffRecord(const WalbDiffRecord &rec, bool isCheck = true)
        : rec_(rec.rec_) {
        if (isCheck && !isValid()) { throw RT_ERR("invalid record."); }
    }

    /**
     * Convert.
     */
    WalbDiffRecord(const struct walb_diff_record &rawRec, bool isCheck = true)
        : rec_(rawRec) {
        if (isCheck && !isValid()) { throw RT_ERR("invalid record."); }
    }

    /**
     * For raw data.
     */
    WalbDiffRecord(const char *data, size_t size)
        : rec_(*reinterpret_cast<const struct walb_diff_record *>(data)) {
        if (size != sizeof(rec_)) {
            throw RT_ERR("size is invalid.");
        }
    }

    virtual ~WalbDiffRecord() noexcept = default;

    WalbDiffRecord &operator=(const WalbDiffRecord &rhs) {
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
    std::pair<WalbDiffRecord, WalbDiffRecord> split(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0 || ioBlocks() <= ioBlocks0) {
            throw RT_ERR("split: ioBlocks0 is out or range.");
        }
        if (isCompressed()) {
            throw RT_ERR("split: compressed data can not be splitted.");
        }
        WalbDiffRecord r0(*this), r1(*this);
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
    std::vector<WalbDiffRecord> splitAll(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0) {
            throw RT_ERR("splitAll: ioBlocks0 must not be 0.");
        }
        if (isCompressed()) {
            throw RT_ERR("splitAll: compressed data can not be splitted.");
        }
        std::vector<WalbDiffRecord> v;
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
class BlockDiffIo
{
private:
    uint16_t ioBlocks_; /* [logical block]. */
    int compressionType_;
    std::vector<char> data_;

public:
    BlockDiffIo()
        : ioBlocks_(0)
        , compressionType_(::WALB_DIFF_CMPR_NONE)
        , data_() {
    }

    BlockDiffIo(const BlockDiffIo &) = default;
    BlockDiffIo(BlockDiffIo &&) = default;
    virtual ~BlockDiffIo() noexcept = default;

    BlockDiffIo &operator=(const BlockDiffIo &rhs) {
        ioBlocks_ = rhs.ioBlocks_;
        compressionType_ = rhs.compressionType_;
        data_ = rhs.data_;
        return *this;
    }
    BlockDiffIo &operator=(BlockDiffIo &&rhs) {
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
    std::pair<BlockDiffIo, BlockDiffIo> split(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0 || ioBlocks() <= ioBlocks0) {
            throw RT_ERR("split: ioBlocks0 is out or range.");
        }
        if (isCompressed()) {
            throw RT_ERR("split: compressed IO can not be splitted.");
        }
        assert(isValid());

        BlockDiffIo r0, r1;
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

    std::vector<BlockDiffIo> splitAll(uint16_t ioBlocks0) const {
        if (ioBlocks0 == 0) {
            throw RT_ERR("splitAll: ioBlocks0 must not be 0.");
        }
        if (isCompressed()) {
            throw RT_ERR("splitAll: compressed IO can not be splitted.");
        }
        assert(isValid());

        std::vector<BlockDiffIo> v;
        uint16_t remaining = ioBlocks();
        size_t off = 0;
        while (0 < remaining) {
            BlockDiffIo io;
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
    BlockDiffIo compress(int type) const {
        if (isCompressed()) {
            throw RT_ERR("Could not compress already compressed diff IO.");
        }
        if (type != ::WALB_DIFF_CMPR_SNAPPY) {
            throw RT_ERR("Currently only snappy is supported.");
        }
        if (ioBlocks() == 0) {
            return BlockDiffIo();
        }
        assert(isValid());
        BlockDiffIo io;
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
    BlockDiffIo uncompress() const {
        if (!isCompressed()) {
            throw RT_ERR("Need not uncompress already uncompressed diff IO.");
        }
        if (compressionType() != ::WALB_DIFF_CMPR_SNAPPY) {
            throw RT_ERR("Currently only snappy is supported.");
        }
        if (ioBlocks() == 0) {
            return BlockDiffIo();
        }
        BlockDiffIo io;
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

/**
 * Walb diff header data.
 */
class WalbDiffFileHeader
{
private:
    struct walb_diff_file_header &h_;

public:
    WalbDiffFileHeader(struct walb_diff_file_header &h) : h_(h) {}
    virtual ~WalbDiffFileHeader() noexcept = default;

    uint32_t getChecksum() const { return h_.checksum; }
    uint16_t getMaxIoBlocks() const { return h_.max_io_blocks; }
    const uint8_t *getUuid() const { return &h_.uuid[0]; }

    void setMaxIoBlocksIfNecessary(uint16_t ioBlocks) {
        if (getMaxIoBlocks() < ioBlocks) {
            h_.max_io_blocks = ioBlocks;
        }
    }

    void resetMaxIoBlocks() { h_.max_io_blocks = 0; }

    void assign(const void *h) {
        h_ = *reinterpret_cast<const struct walb_diff_file_header *>(h);
    }

    const char *rawData() const { return ptr<char>(); }
    char *rawData() { return ptr<char>(); }
    size_t rawSize() const { return sizeof(h_); }

    template <typename T>
    T *ptr() { return reinterpret_cast<T *>(&h_); }

    template <typename T>
    const T *ptr() const { return reinterpret_cast<const T *>(&h_); }

    bool isValid() const {
        return cybozu::util::calcChecksum(ptr<char>(), sizeof(h_), 0) == 0;
    }

    void updateChecksum() {
        h_.checksum = 0;
        h_.checksum = cybozu::util::calcChecksum(ptr<char>(), sizeof(h_), 0);
        assert(isValid());
    }

    void setUuid(const uint8_t *uuid) {
        ::memcpy(&h_.uuid[0], uuid, UUID_SIZE);
    }

    void print(::FILE *fp) const {
        ::fprintf(fp, "-----walb_file_header-----\n"
                  "checksum: %08x\n"
                  "maxIoBlocks: %u\n"
                  "uuid: ",
                  getChecksum(), getMaxIoBlocks());
        for (size_t i = 0; i < UUID_SIZE; i++) {
            ::fprintf(fp, "%02x", getUuid()[i]);
        }
        ::fprintf(fp, "\n");
    }

    void print() const { print(::stdout); }

    void init() {
        ::memset(&h_, 0, sizeof(h_));
    }
};

/**
 * With raw data.
 */
class WalbDiffFileHeaderWithBody
    : public WalbDiffFileHeader
{
private:
    struct walb_diff_file_header header_;

public:
    WalbDiffFileHeaderWithBody()
        : WalbDiffFileHeader(header_), header_() {}
    ~WalbDiffFileHeaderWithBody() noexcept = default;
};

/**
 * Walb diff pack wrapper.
 */
class WalbDiffPack /* final */
{
private:
    std::vector<char> buf_;
public:
    WalbDiffPack() : buf_(::WALB_DIFF_PACK_SIZE, 0) {}
    ~WalbDiffPack() noexcept = default;

    const char *rawData() const { return &buf_[0]; }
    char *rawData() { return &buf_[0]; }
    size_t rawSize() const { return ::WALB_DIFF_PACK_SIZE; }

    void reset() { ::memset(rawData(), 0, rawSize()); }

    struct walb_diff_pack &header() {
        return *reinterpret_cast<struct walb_diff_pack *>(&buf_[0]);
    }
    const struct walb_diff_pack &header() const {
        return *reinterpret_cast<const struct walb_diff_pack *>(&buf_[0]);
    }
    struct walb_diff_record &record(size_t i) {
        checkRange(i);
        return header().record[i];
    }
    const struct walb_diff_record &record(size_t i) const {
        checkRange(i);
        return header().record[i];
    }

    uint16_t nRecords() const { return header().n_records; }
    uint32_t totalSize() const { return header().total_size; }

    bool isEnd() const {
        const uint8_t mask = 1U << WALB_DIFF_PACK_END;
        return (header().flags & mask) != 0;
    }

    void setEnd() {
        const uint8_t mask = 1U << WALB_DIFF_PACK_END;
        header().flags |= mask;
    }

    /**
     * RETURN:
     *   true when added successfully.
     *   false when pack is full.
     */
    bool add(const struct walb_diff_record &inRec) {
#ifdef DEBUG
        WalbDiffRecord r(reinterpret_cast<const char *>(&inRec), sizeof(inRec));
        assert(r.isValid());
#endif
        if (!canAdd(inRec)) { return false; }
        struct walb_diff_record &outRec = record(header().n_records);
        outRec = inRec;
        outRec.data_offset = header().total_size;
        header().n_records++;
        header().total_size += inRec.data_size;
        assert(outRec.data_size == inRec.data_size);
        return true;
    }

    void updateChecksum() {
        header().checksum = 0;
        header().checksum = cybozu::util::calcChecksum(rawData(), rawSize(), 0);
        assert(isValid());
    }

    bool isValid() const {
        return cybozu::util::calcChecksum(rawData(), rawSize(), 0) == 0;
    }

    void print(::FILE *fp) const {
        const struct walb_diff_pack &h = header();
        ::fprintf(fp, "checksum %u\n"
                  "n_records: %u\n"
                  "total_size: %u\n"
                  , h.checksum
                  , h.n_records
                  , h.total_size);
        for (size_t i = 0; i < h.n_records; i++) {
            ::fprintf(fp, "record %zu: ", i);
            WalbDiffRecord rec(reinterpret_cast<const char *>(&record(i)), sizeof(record(i)));
            rec.printOneline(fp);
        }
    }

    void print() const { print(::stdout); }

private:
    void checkRange(size_t i) const {
        if (::MAX_N_RECORDS_IN_WALB_DIFF_PACK <= i) {
            throw RT_ERR("walb_diff_pack boundary error.");
        }
    }

    bool canAdd(const struct walb_diff_record &rec) {
        uint16_t n_rec = header().n_records;
        if (::MAX_N_RECORDS_IN_WALB_DIFF_PACK <= n_rec) {
            return false;
        }
        if (0 < n_rec && ::WALB_DIFF_PACK_MAX_SIZE < header().total_size + rec.data_size) {
            return false;
        }
        return true;
    }
};

/**
 * Walb diff writer.
 */
class WalbDiffWriter /* final */
{
private:
    std::shared_ptr<cybozu::util::FileOpener> opener_;
    int fd_;
    cybozu::util::FdWriter fdw_;
    bool isWrittenHeader_;
    bool isClosed_;

    /* Buffers. */
    WalbDiffPack pack_;
    std::queue<std::shared_ptr<BlockDiffIo> > ioPtrQ_;

public:
    explicit WalbDiffWriter(int fd)
        : opener_(), fd_(fd), fdw_(fd)
        , isWrittenHeader_(false)
        , isClosed_(false)
        , pack_()
        , ioPtrQ_() {}

    explicit WalbDiffWriter(const std::string &diffPath, int flags, mode_t mode)
        : opener_(new cybozu::util::FileOpener(diffPath, flags, mode))
        , fd_(opener_->fd())
        , fdw_(fd_)
        , isWrittenHeader_(false)
        , isClosed_(false)
        , pack_()
        , ioPtrQ_() {
        assert(0 < fd_);
    }

    ~WalbDiffWriter() noexcept {
        try {
            close();
        } catch (...) {}
    }

    void close() {
        if (!isClosed_) {
            flush();
            writeEof();
            if (opener_) { opener_->close(); }
            isClosed_ = true;
        }
    }

    /**
     * Write header data.
     * You must call this at first.
     */
    void writeHeader(WalbDiffFileHeader &header) {
        if (isWrittenHeader_) {
            throw RT_ERR("Do not call writeHeader() more than once.");
        }
        header.updateChecksum();
        assert(header.isValid());
        fdw_.write(header.rawData(), header.rawSize());
        isWrittenHeader_ = true;
    }

    /**
     * Write a diff data.
     *
     * @rec record.
     * @iop may contains nullptr.
     */
    void writeDiff(const WalbDiffRecord &rec, const std::shared_ptr<BlockDiffIo> iop) {
        checkWrittenHeader();
        assert(rec.isValid());
        if (iop) {
            assert(iop->isValid());
        }
        if (rec.isNormal()) {
            assert(rec.compressionType() == iop->compressionType());
            assert(rec.dataSize() == iop->rawSize());
            assert(rec.checksum() == iop->calcChecksum());
        }

        /* Try to add. */
        if (pack_.add(*rec.rawRecord())) {
            ioPtrQ_.push(iop);
            return;
        }

        /* Flush and add. */
        writePack();
        UNUSED bool ret = pack_.add(*rec.rawRecord());
        assert(ret);
        ioPtrQ_.push(iop);
    }

    /**
     * Compress and write a diff data.
     *
     * @rec record.
     * @io IO data.
     */
    void compressAndWriteDiff(const WalbDiffRecord &rec, const BlockDiffIo &io) {
        assert(rec.isValid());
        assert(io.isValid());
        assert(rec.compressionType() == io.compressionType());
        assert(rec.dataSize() == io.rawSize());
        if (rec.isNormal()) {
            assert(rec.ioBlocks() == io.ioBlocks());
            assert(rec.checksum() == io.calcChecksum());
        }

        auto iop = std::make_shared<BlockDiffIo>();
        if (!rec.isNormal()) {
            writeDiff(rec, iop);
            return;
        }

        WalbDiffRecord rec0(rec);
        if (rec.isCompressed()) {
            /* copy */
            *iop = io;
        } else {
            *iop = io.compress(::WALB_DIFF_CMPR_SNAPPY);
            rec0.setCompressionType(::WALB_DIFF_CMPR_SNAPPY);
            rec0.setDataSize(iop->rawSize());
            rec0.setChecksum(iop->calcChecksum());
        }
        writeDiff(rec0, iop);
    }

    /**
     * Write buffered data.
     */
    void flush() {
        writePack();
    }

private:
    /* Write the buffered pack and its related diff ios. */
    void writePack() {
        if (pack_.header().n_records == 0) {
            assert(ioPtrQ_.empty());
            return;
        }

        size_t total = 0;
        pack_.updateChecksum();
        fdw_.write(pack_.rawData(), pack_.rawSize());
        while (!ioPtrQ_.empty()) {
            std::shared_ptr<BlockDiffIo> iop0 = ioPtrQ_.front();
            ioPtrQ_.pop();
            if (!iop0 || iop0->rawSize() == 0) { continue; }
            fdw_.write(iop0->rawData(), iop0->rawSize());
            total += iop0->rawSize();
        }
        assert(total == pack_.header().total_size);
        pack_.reset();
    }

    void writeEof() {
        pack_.reset();
        pack_.setEnd();
        pack_.updateChecksum();
        fdw_.write(pack_.rawData(), pack_.rawSize());
    }

    void checkWrittenHeader() const {
        if (!isWrittenHeader_) {
            throw RT_ERR("Call writeHeader() before calling writeDiff().");
        }
    }
};

/**
 * Read walb diff data from an input stream.
 */
class WalbDiffReader
{
private:
    std::shared_ptr<cybozu::util::FileOpener> opener_;
    int fd_;
    cybozu::util::FdReader fdr_;
    bool isReadHeader_;

    /* Buffers. */
    WalbDiffPack pack_;
    uint16_t recIdx_;
    uint32_t totalSize_;

public:
    explicit WalbDiffReader(int fd)
        : opener_(), fd_(fd), fdr_(fd)
        , isReadHeader_(false)
        , pack_()
        , recIdx_(0)
        , totalSize_(0) {}

    explicit WalbDiffReader(const std::string &diffPath, int flags)
        : opener_(new cybozu::util::FileOpener(diffPath, flags))
        , fd_(opener_->fd())
        , fdr_(fd_)
        , isReadHeader_(false)
        , pack_()
        , recIdx_(0)
        , totalSize_(0) {
        assert(0 < fd_);
    }

    ~WalbDiffReader() noexcept {
        try {
            close();
        } catch (...) {}
    }

    void close() {
        if (opener_) { opener_->close(); }
    }

    /**
     * Read header data.
     * You must call this at first.
     */
    std::shared_ptr<WalbDiffFileHeader> readHeader() {
        auto p = std::make_shared<WalbDiffFileHeaderWithBody>();
        readHeader(*p);
        return p;
    }

    /**
     * Read header data with another interface.
     */
    void readHeader(WalbDiffFileHeader &head) {
        if (isReadHeader_) {
            throw RT_ERR("Do not call readHeader() more than once.");
        }
        fdr_.read(head.rawData(), head.rawSize());
        if (!head.isValid()) {
            throw RT_ERR("diff header invalid.\n");
        }
        isReadHeader_ = true;
        readPackHeader();
    }

    /**
     * Read a diff IO.
     *
     * RETURN:
     *   false if the input stream reached the end.
     */
    bool readDiff(WalbDiffRecord &rec, BlockDiffIo &io) {
        if (!canRead()) return false;
        ::memcpy(rec.rawData(), &pack_.record(recIdx_), sizeof(struct walb_diff_record));
        if (!rec.isValid()) {
            throw RT_ERR("Invalid record.");
        }
        readDiffIo(rec, io);
        return true;
    }

    /**
     * Read a diff IO and uncompress it.
     *
     * RETURN:
     *   false if the input stream reached the end.
     */
    bool readAndUncompressDiff(WalbDiffRecord &rec, BlockDiffIo &io) {
        WalbDiffRecord rec0;
        BlockDiffIo io0;
        if (!readDiff(rec0, io0)) {
            rec0.clearExists();
            rec = rec0;
            io = std::move(io0);
            return false;
        }
        if (!rec0.isCompressed()) {
            rec = rec0;
            io = std::move(io0);
            return true;
        }
        rec = rec0;
        io = io0.uncompress();
        rec.setCompressionType(::WALB_DIFF_CMPR_NONE);
        rec.setDataSize(io.rawSize());
        rec.setChecksum(io.calcChecksum());
        assert(rec.isValid());
        assert(io.isValid());
        return true;
    }

    bool canRead() {
        if (pack_.isEnd() ||
            (recIdx_ == pack_.nRecords() && !readPackHeader())) {
            return false;
        }
        return true;
    }

private:
    /**
     * Read pack header.
     *
     * RETURN:
     *   false if EofError caught.
     */
    bool readPackHeader() {
        try {
            fdr_.read(pack_.rawData(), pack_.rawSize());
        } catch (cybozu::util::EofError &e) {
            return false;
        }
        if (!pack_.isValid()) {
            throw RT_ERR("pack header invalid.");
        }
        if (pack_.isEnd()) { return false; }
        recIdx_ = 0;
        totalSize_ = 0;
        return true;
    }

    /**
     * Read a diff IO.
     * @rec diff record.
     * @io block IO to be filled.
     *
     * If rec.dataSize() == 0, io will not be changed.
     */
    void readDiffIo(const WalbDiffRecord &rec, BlockDiffIo &io) {
        if (rec.dataOffset() != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.dataOffset(), totalSize_);
        }
        if (0 < rec.dataSize()) {
            io.data().resize(rec.dataSize());
            io.setIoBlocks(rec.ioBlocks());
            io.setCompressionType(rec.compressionType());

            fdr_.read(io.rawData(), io.rawSize());
            uint32_t csum = cybozu::util::calcChecksum(io.rawData(), io.rawSize(), 0);
            if (rec.checksum() != csum) {
                throw RT_ERR("checksum invalid rec: %08x data: %08x.\n", rec.checksum(), csum);
            }
        }
        recIdx_++;
        totalSize_ += rec.dataSize();
    }
};

/**
 * Diff record and its IO data.
 * Data compression is not supported.
 */
class DiffRecIo /* final */
{
private:
    WalbDiffRecord rec_;
    BlockDiffIo io_;
public:
    struct walb_diff_record &rawRecord() { return *rec_.rawRecord(); }
    const struct walb_diff_record &rawRecord() const { return *rec_.rawRecord(); }
    WalbDiffRecord &record() { return rec_; }
    const WalbDiffRecord &record() const { return rec_; }

    BlockDiffIo &io() { return io_; }
    const BlockDiffIo &io() const { return io_; }

    void copyFrom(const WalbDiffRecord &rec, const BlockDiffIo &io) {
        rec_ = rec;
        if (rec.isNormal()) {
            io_.setIoBlocks(io.ioBlocks());
            io_.setCompressionType(io.compressionType());
            io_.data().resize(io.data().size());
            ::memcpy(io_.rawData(), io.rawData(), io.rawSize());
        } else {
            io_ = BlockDiffIo();
        }
    }
    void moveFrom(const WalbDiffRecord &rec, BlockDiffIo &&io) {
        rec_ = rec;
        if (rec.isNormal()) {
            io_ = std::move(io);
        } else {
            io_ = BlockDiffIo();
        }
    }
    void moveFrom(const WalbDiffRecord &rec, std::vector<char> &&data) {
        rec_ = rec;
        if (rec.isNormal()) {
            io_.setIoBlocks(rec.ioBlocks());
            io_.setCompressionType(rec.compressionType());
            io_.data() = std::move(data);
        } else {
            io_ = BlockDiffIo();
        }
    }

    void updateChecksum() {
        rec_.setChecksum(io_.calcChecksum());
    }

    bool isValid(bool isChecksum = false) const {
        if (!rec_.isValid()) {
            LOGd("rec is not valid.\n");
            return false;
        }
        if (!io_.isValid()) {
            LOGd("io is not valid.\n");
            return false;
        }
        if (!rec_.isNormal()) {
            if (io_.ioBlocks() != 0) {
                LOGd("Fro non-normal record, io.ioBlocks must be 0.\n");
                return false;
            }
            return true;
        }
        if (rec_.ioBlocks() != io_.ioBlocks()) {
            LOGd("ioSize invalid %u %u\n", rec_.ioBlocks(), io_.ioBlocks());
            return false;
        }
        if (rec_.dataSize() != io_.rawSize()) {
            LOGd("dataSize invalid %" PRIu32 " %zu\n", rec_.dataSize(), io_.rawSize());
            return false;
        }
        if (rec_.isCompressed()) {
            LOGd("DiffRecIo does not support compressed data.\n");
            return false;
        }
        if (isChecksum && rec_.checksum() != io_.calcChecksum()) {
            LOGd("checksum invalid %0x %0x\n", rec_.checksum(), io_.calcChecksum());
            return false;
        }
        return true;
    }

    void print(::FILE *fp) const {
        rec_.printOneline(fp);
        io_.printOneline(fp);
    }

    void print() const { print(::stdout); }

    /**
     * Split the DiffRecIo into pieces
     * where each ioBlocks is <= a specified one.
     */
    std::vector<DiffRecIo> splitAll(uint16_t ioBlocks) const {
        assert(isValid());
        std::vector<DiffRecIo> v;

        std::vector<WalbDiffRecord> recV = rec_.splitAll(ioBlocks);
        std::vector<BlockDiffIo> ioV;
        if (rec_.isNormal()) {
            ioV = io_.splitAll(ioBlocks);
        } else {
            ioV.resize(recV.size());
        }
        assert(recV.size() == ioV.size());
        auto it0 = recV.begin();
        auto it1 = ioV.begin();
        while (it0 != recV.end() && it1 != ioV.end()) {
            DiffRecIo r;
            r.moveFrom(*it0, std::move(*it1));
            r.updateChecksum();
            assert(r.isValid());
            v.push_back(std::move(r));
            ++it0;
            ++it1;
        }
        return std::move(v);
    }

    /**
     * Create (IO portions of rhs) - (that of *this).
     * If non-overlapped, throw runtime error.
     * The overlapped data of rhs will be used.
     * *this will not be changed.
     */
    std::vector<DiffRecIo> minus(const DiffRecIo &rhs) const {
        assert(isValid(true));
        assert(rhs.isValid(true));
        if (!rec_.isOverlapped(rhs.rec_)) {
            throw RT_ERR("Non-overlapped.");
        }
        std::vector<DiffRecIo> v;
        /*
         * Pattern 1:
         * __oo__ + xxxxxx = xxxxxx
         */
        if (rec_.isOverwrittenBy(rhs.rec_)) {
            /* Empty */
            return std::move(v);
        }
        /*
         * Pattern 2:
         * oooooo + __xx__ = ooxxoo
         */
        if (rhs.rec_.isOverwrittenBy(rec_)) {
            uint16_t blks0 = rhs.rec_.ioAddress() - rec_.ioAddress();
            uint16_t blks1 = rec_.endIoAddress() - rhs.rec_.endIoAddress();
            uint64_t addr0 = rec_.ioAddress();
            uint64_t addr1 = rec_.endIoAddress() - blks1;

            WalbDiffRecord rec0(rec_), rec1(rec_);
            rec0.setIoAddress(addr0);
            rec0.setIoBlocks(blks0);
            rec1.setIoAddress(addr1);
            rec1.setIoBlocks(blks1);

            size_t size0 = 0;
            size_t size1 = 0;
            if (rec_.isNormal()) {
                size0 = blks0 * LOGICAL_BLOCK_SIZE;
                size1 = blks1 * LOGICAL_BLOCK_SIZE;
            }
            rec0.setDataSize(size0);
            rec1.setDataSize(size1);

            std::vector<char> data0(size0), data1(size1);
            if (rec_.isNormal()) {
                size_t off1 = (addr1 - rec_.ioAddress()) * LOGICAL_BLOCK_SIZE;
                assert(size0 + rhs.rec_.ioBlocks() * LOGICAL_BLOCK_SIZE + size1 == rec_.dataSize());
                ::memcpy(&data0[0], io_.rawData(), size0);
                ::memcpy(&data1[0], io_.rawData() + off1, size1);
            }

            if (0 < blks0) {
                DiffRecIo r;
                r.moveFrom(rec0, std::move(data0));
                r.updateChecksum();
                assert(r.isValid());
                v.push_back(std::move(r));
            }
            if (0 < blks1) {
                DiffRecIo r;
                r.moveFrom(rec1, std::move(data1));
                r.updateChecksum();
                assert(r.isValid());
                v.push_back(std::move(r));
            }
            return std::move(v);
        }
        /*
         * Pattern 3:
         * oooo__ + __xxxx = ooxxxx
         */
        if (rec_.ioAddress() < rhs.rec_.ioAddress()) {
            assert(rhs.rec_.ioAddress() < rec_.endIoAddress());
            uint16_t rblks = rec_.endIoAddress() - rhs.rec_.ioAddress();
            assert(rhs.rec_.ioAddress() + rblks == rec_.endIoAddress());

            WalbDiffRecord rec(rec_);
            /* rec.ioAddress() does not change. */
            rec.setIoBlocks(rec_.ioBlocks() - rblks);
            assert(rec.endIoAddress() == rhs.rec_.ioAddress());

            size_t size = 0;
            if (rec_.isNormal()) {
                size = io_.rawSize() - rblks * LOGICAL_BLOCK_SIZE;
            }
            std::vector<char> data(size);
            if (rec_.isNormal()) {
                assert(rec_.dataSize() == io_.rawSize());
                rec.setDataSize(size);
                ::memcpy(&data[0], io_.rawData(), size);
            }

            DiffRecIo r;
            r.moveFrom(rec, std::move(data));
            r.updateChecksum();
            assert(r.isValid());
            v.push_back(std::move(r));
            return std::move(v);
        }
        /*
         * Pattern 4:
         * __oooo + xxxx__ = xxxxoo
         */
        assert(rec_.ioAddress() < rhs.rec_.endIoAddress());
        uint16_t rblks = rhs.rec_.endIoAddress() - rec_.ioAddress();
        assert(rec_.ioAddress() + rblks == rhs.rec_.endIoAddress());
        size_t off = rblks * LOGICAL_BLOCK_SIZE;

        WalbDiffRecord rec(rec_);
        rec.setIoAddress(rec_.ioAddress() + rblks);
        rec.setIoBlocks(rec_.ioBlocks() - rblks);

        size_t size = 0;
        if (rec_.isNormal()) {
            size = io_.rawSize() - off;
        }
        std::vector<char> data(size);
        if (rec_.isNormal()) {
            assert(rec_.dataSize() == io_.rawSize());
            rec.setDataSize(size);
            ::memcpy(&data[0], io_.rawData() + off, size);
        }
        assert(rhs.rec_.endIoAddress() == rec.ioAddress());
        DiffRecIo r;
        r.moveFrom(rec, std::move(data));
        r.updateChecksum();
        assert(r.isValid());
        v.push_back(std::move(r));
        return std::move(v);
    }
};

/**
 * Simpler implementation of in-memory walb diff data.
 * IO data compression is not supported.
 */
class WalbDiffMemory
{
private:
    const uint16_t maxIoBlocks_; /* All IOs must not exceed the size. */
    std::map<uint64_t, DiffRecIo> map_;
    struct walb_diff_file_header h_;
    WalbDiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    explicit WalbDiffMemory(uint16_t maxIoBlocks = uint16_t(-1))
        : maxIoBlocks_(maxIoBlocks), map_(), h_(), fileH_(h_), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~WalbDiffMemory() noexcept = default;
    bool init() {
        /* Initialize always. */
        return false;
    }
    bool empty() const { return map_.empty(); }

    void add(const WalbDiffRecord &rec, const BlockDiffIo &io, uint16_t maxIoBlocks = 0) {
        add(rec, BlockDiffIo(io), maxIoBlocks);
    }
    void add(const WalbDiffRecord &rec, BlockDiffIo &&io, uint16_t maxIoBlocks = 0) {
        /* Decide key range to search. */
        uint64_t addr0 = rec.ioAddress();
        if (addr0 <= fileH_.getMaxIoBlocks()) {
            addr0 = 0;
        } else {
            addr0 -= fileH_.getMaxIoBlocks();
        }
        /* Search overlapped items. */
        uint64_t addr1 = rec.endIoAddress();
        std::queue<DiffRecIo> q;
        auto it = map_.lower_bound(addr0);
        while (it != map_.end() && it->first < addr1) {
            DiffRecIo &r = it->second;
            if (r.record().isOverlapped(rec)) {
                nIos_--;
                nBlocks_ -= r.record().ioBlocks();
                q.push(std::move(r));
                it = map_.erase(it);
            } else {
                ++it;
            }
        }
        /* Eliminate overlaps. */
        DiffRecIo r0;
        r0.moveFrom(rec, std::move(io));
        assert(r0.isValid());
        while (!q.empty()) {
            std::vector<DiffRecIo> v = q.front().minus(r0);
            for (DiffRecIo &r : v) {
                nIos_++;
                nBlocks_ += r.record().ioBlocks();
                uint64_t addr = r.record().ioAddress();
                map_.insert(std::make_pair(addr, std::move(r)));
            }
            q.pop();
        }
        /* Insert the item. */
        nIos_++;
        nBlocks_ += r0.record().ioBlocks();
        std::vector<DiffRecIo> rv;
        if (0 < maxIoBlocks && maxIoBlocks < rec.ioBlocks()) {
            rv = r0.splitAll(maxIoBlocks);
        } else if (maxIoBlocks_ < rec.ioBlocks()) {
            rv = r0.splitAll(maxIoBlocks_);
        } else {
            rv.push_back(std::move(r0));
        }
        for (DiffRecIo &r : rv) {
            uint64_t addr = r.record().ioAddress();
            uint16_t blks = r.record().ioBlocks();
            map_.insert(std::make_pair(addr, std::move(r)));
            fileH_.setMaxIoBlocksIfNecessary(blks);
        }
    }
    bool sync() {
        /* do nothing. */
        return true;
    }
    void print(::FILE *fp) const {
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const WalbDiffRecord &rec = it->second.record();
            rec.printOneline(fp);
            ++it;
        }
    }
    void print() const { print(::stdout); }
    uint64_t getNBlocks() const { return nBlocks_; }
    uint64_t getNIos() const { return nIos_; }
    void checkStatistics() const {
        uint64_t nBlocks = 0;
        uint64_t nIos = 0;
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const WalbDiffRecord &rec = it->second.record();
            nBlocks += rec.ioBlocks();
            nIos++;
            ++it;
        }
        if (nBlocks_ != nBlocks) {
            throw RT_ERR("nBlocks_ %" PRIu64 " nBlocks %" PRIu64 "\n",
                         nBlocks_, nBlocks);
        }
        if (nIos_ != nIos) {
            throw RT_ERR("nIos_ %" PRIu64 " nIos %" PRIu64 "\n",
                         nIos_, nIos);
        }
    }
    WalbDiffFileHeader& header() { return fileH_; }
    bool writeTo(int outFd, bool isCompressed = true) {
        WalbDiffWriter writer(outFd);
        writer.writeHeader(fileH_);
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const DiffRecIo &r = it->second;
            assert(r.isValid());
            if (isCompressed) {
                writer.compressAndWriteDiff(r.record(), r.io());
            } else {
                auto iop = std::make_shared<BlockDiffIo>();
                if (r.record().isNormal()) {
                    *iop = r.io();
                }
                writer.writeDiff(r.record(), iop);
            }
            ++it;
        }
        writer.flush();
        return true;
    }
    void checkNoOverlappedAndSorted() const {
        auto it = map_.cbegin();
        const WalbDiffRecord *prev = nullptr;
        while (it != map_.cend()) {
            const WalbDiffRecord *curr = &it->second.record();
            if (prev) {
                if (!(prev->ioAddress() < curr->ioAddress())) {
                    throw RT_ERR("Not sorted.");
                }
                if (!(prev->endIoAddress() <= curr->ioAddress())) {
                    throw RT_ERR("Overlapped records exist.");
                }
            }
            prev = curr;
            ++it;
        }
    }

    using Map = std::map<uint64_t, DiffRecIo>;
    template <typename DiffMemory, typename MapIterator>
    class IteratorBase {
    protected:
        DiffMemory *mem_;
        MapIterator it_;
    public:
        explicit IteratorBase(DiffMemory *mem)
            : mem_(mem)
            , it_() {
            assert(mem);
        }
        IteratorBase(const IteratorBase &rhs)
            : mem_(rhs.mem_)
            , it_(rhs.it_) {
        }
        virtual ~IteratorBase() noexcept = default;
        IteratorBase &operator=(const IteratorBase &rhs) {
            mem_ = rhs.mem_;
            it_ = rhs.it_;
            return *this;
        }
        bool isValid() const { return it_ != mem_->map_.end(); }
        void begin() { it_ = mem_->map_.begin(); }
        void end() { it_ = mem_->map_.end(); }
        void next() { ++it_; }
        void prev() { --it_; }
        void lowerBound(uint64_t addr) {
            it_ = mem_->map_.lower_bound(addr);
        }
        void upperBound(uint64_t addr) {
            it_ = mem_->map_.upper_bound(addr);
        }
        WalbDiffRecord &record() {
            checkValid();
            return it_->second.record();
        }
        const WalbDiffRecord &record() const {
            checkValid();
            return it_->second.record();
        }
        char *rawData() {
            checkValid();
            return it_->second.rawData();
        }
        const char *rawData() const {
            checkValid();
            return it_->second.rawData();
        }
        uint32_t rawSize() const {
            checkValid();
            return it_->second.rawSize();
        }
    protected:
        void checkValid() const {
            if (!isValid()) {
                throw RT_ERR("Invalid iterator position.");
            }
        }
    };
    class ConstIterator
        : public IteratorBase<const WalbDiffMemory, Map::iterator>
    {
    public:
        explicit ConstIterator(const WalbDiffMemory *mem)
            : IteratorBase<const WalbDiffMemory, Map::iterator>(mem) {
        }
        ConstIterator(const ConstIterator &rhs)
            : IteratorBase<const WalbDiffMemory, Map::iterator>(rhs) {
        }
        ~ConstIterator() noexcept override = default;

    };
    class Iterator
        : public IteratorBase<WalbDiffMemory, Map::iterator>
    {
    public:
        explicit Iterator(WalbDiffMemory *mem)
            : IteratorBase<WalbDiffMemory, Map::iterator>(mem) {
        }
        Iterator(const Iterator &rhs)
            : IteratorBase<WalbDiffMemory, Map::iterator>(rhs) {
        }
        ~Iterator() noexcept override = default;
        /**
         * Erase the item on the iterator.
         * The iterator will indicate the next of the removed item.
         */
        void erase() {
            checkValid();
            mem_->nIos_--;
            mem_->nBlocks_ -= it_->second.record().ioBlocks();
            it_ = mem_->map_.erase(it_);
            if (mem_->map_.empty()) {
                mem_->fileH_.resetMaxIoBlocks();
            }
        }
        DiffRecIo &recIo() {
            return it_->second;
        }
    };
    Iterator iterator() {
        return Iterator(this);
    }
    ConstIterator constIterator() const {
        return ConstIterator(this);
    }
};

}} //namesapce walb::diff

#endif /* WALB_DIFF_UTIL_HPP */
