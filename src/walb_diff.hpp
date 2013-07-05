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
 * Simple compressor.
 */
bool compress(int type, const char *input, size_t inputSize,
              char *compressed, size_t &compressedSize)
{
    switch (type) {
    case ::WALB_DIFF_CMPR_NONE:
        if (compressedSize < inputSize) { return false; }
        ::memcpy(compressed, input, inputSize);
        compressedSize = inputSize;
        break;
    default:
        throw RT_ERR("Not yet implementation.");
    }
    return true;
}

/**
 * Simple uncompressor.
 */
bool uncompress(int type, const char *compressed, size_t compressedSize,
                char *output, size_t &outputSize)
{
    switch (type) {
    case ::WALB_DIFF_CMPR_NONE:
        if (outputSize < compressedSize) { return false; }
        ::memcpy(output, compressed, compressedSize);
        outputSize = compressedSize;
        break;
    default:
        throw RT_ERR("Not yet implementation.");
    }
    return true;
}

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
        setNormal();
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

    /**
     * For key.
     */
    WalbDiffRecord(uint64_t ioAddress0, uint16_t ioBlocks0)
        : rec_() {
        init();
        setIoAddress(ioAddress0);
        setIoBlocks(ioBlocks0);
    }

    /**
     * Clone and get a portion.
     */
    WalbDiffRecord(const WalbDiffRecord &rec,
                   uint64_t ioAddress0, uint16_t ioBlocks0,
                   int compressionType = ::WALB_DIFF_CMPR_NONE,
                   uint32_t compressedSize = 0)
        : rec_(*rec.ptr<const struct walb_diff_record>()) {

        /* Update metadata. */
        setPortion(ioAddress0, ioBlocks0);

        if (rec.isDiscard() || rec.isAllZero()) { return; }

        if (compressionType == ::WALB_DIFF_CMPR_NONE) {
            compressedSize = ioBlocks0 * LOGICAL_BLOCK_SIZE;
        }
        setCompressionType(compressionType);
        setDataSize(compressedSize);

        if (!isValid()) {
            throw RT_ERR("record invalid.");
        }
    }

    virtual ~WalbDiffRecord() noexcept = default;

    WalbDiffRecord &operator=(const WalbDiffRecord &rhs) {
        rec_ = rhs.rec_;
        return *this;
    }

    bool isValid() const {
        if (!isNormal()) {
            return dataSize() == 0;
        }
        if (::WALB_DIFF_CMPR_MAX <= compressionType()) { return false; }
        if (isAllZero() && isDiscard()) { return false; }
        if (ioBlocks() == 0) { return false; }
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

    bool isAllZero() const {
        return (rec_.flags & (1U << ::WALB_DIFF_FLAG_ALLZERO)) != 0;
    }
    bool isDiscard() const {
        return (rec_.flags & (1U << ::WALB_DIFF_FLAG_DISCARD)) != 0;
    }
    bool isNormal() const {
        return (rec_.flags & (1U << ::WALB_DIFF_FLAG_NORMAL)) != 0;
    }

    void print(::FILE *fp) const {
        ::fprintf(fp, "----------\n"
                  "ioAddress: %" PRIu64 "\n"
                  "ioBlocks: %u\n"
                  "compressionType: %u\n"
                  "dataOffset: %u\n"
                  "dataSize: %u\n"
                  "checksum: %08x\n"
                  "isNormal: %d\n"
                  "isAllZero: %d\n"
                  "isDiscard: %d\n",
                  ioAddress(), ioBlocks(),
                  compressionType(), dataOffset(), dataSize(),
                  checksum(), isNormal(), isAllZero(), isDiscard());
    }

    void print() const { print(::stdout); }

    void printOneline(::FILE *fp) const {
        ::fprintf(fp, "wdiff_rec:\t%" PRIu64 "\t%u\t%u\t%u\t%u\t%08x\t%d%d%d\n",
                  ioAddress(), ioBlocks(),
                  compressionType(), dataOffset(), dataSize(),
                  checksum(), isNormal(), isAllZero(), isDiscard());
    }

    void printOneline() const { printOneline(::stdout); }

    void setIoAddress(uint64_t ioAddress) { rec_.io_address = ioAddress; }
    void setIoBlocks(uint16_t ioBlocks) { rec_.io_blocks = ioBlocks; }
    void setCompressionType(uint8_t type) { rec_.compression_type = type; }
    void setDataOffset(uint32_t offset) { rec_.data_offset = offset; }
    void setDataSize(uint32_t size) { rec_.data_size = size; }
    void setChecksum(uint32_t csum) { rec_.checksum = csum; }

    void setNormal() {
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_NORMAL);
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_ALLZERO);
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_DISCARD);
    }
    void setAllZero() {
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_NORMAL);
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_ALLZERO);
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_DISCARD);
    }
    void setDiscard() {
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_NORMAL);
        rec_.flags &= ~(1U << ::WALB_DIFF_FLAG_ALLZERO);
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_DISCARD);
    }

private:
    void init() {
        ::memset(&rec_, 0, sizeof(rec_));
    }

    void setPortion(uint64_t ioAddress0, uint16_t ioBlocks0) {
        assert(ioAddress() <= ioAddress0);
        assert(ioAddress0 < ioAddress() + ioBlocks());
        assert(ioAddress0 + ioBlocks0 <= ioAddress() + ioBlocks());

        setIoAddress(ioAddress0);
        setIoBlocks(ioBlocks0);
    }
};

/**
 * Constant WalbDiffKeys.
 */
struct WalbDiffKey : public WalbDiffRecord
{
    WalbDiffKey(uint64_t ioAddress, uint16_t ioBlocks)
        : WalbDiffRecord(ioAddress, ioBlocks) {}
    ~WalbDiffKey() noexcept override = default;

    struct Min : public WalbDiffRecord {
        Min() : WalbDiffRecord(uint64_t(0), uint16_t(0)) {}
        ~Min() noexcept override = default;
    };

    struct Max : public WalbDiffRecord {
        Max() : WalbDiffRecord(uint64_t(-1), 0) {}
        ~Max() noexcept override = default;
    };
    struct Header : public WalbDiffRecord {
        Header() : WalbDiffRecord(uint64_t(-1), uint16_t(-1)) {}
        ~Header() noexcept override = default;
    };
};

/**
 * Block diff for an IO.
 */
class BlockDiffIo
    : public block_diff::BlockDiffValue<BlockDiffIo>
{
private:
    uint16_t ioBlocks_; /* [logical block]. */
    int compressionType_;
    std::vector<char> data_;

public:
    BlockDiffIo(uint16_t ioBlocks, int compressionType = ::WALB_DIFF_CMPR_NONE)
        : ioBlocks_(ioBlocks)
        , compressionType_(compressionType)
        , data_() {
    }

    BlockDiffIo()
        : ioBlocks_(0)
        , compressionType_(::WALB_DIFF_CMPR_NONE)
        , data_() {
    }

    BlockDiffIo(const BlockDiffIo &rhs) = delete;
    BlockDiffIo(BlockDiffIo &&rhs) = default;
    virtual ~BlockDiffIo() noexcept = default;

    void init(uint16_t ioBlocks, int compressionType = ::WALB_DIFF_CMPR_NONE) {
        ioBlocks_ = ioBlocks;
        compressionType_ = compressionType;
    }

    uint16_t getIoBlocks() const { return ioBlocks_; }
    int getCompressionType() const { return compressionType_; }
    bool isCompressed() const { return compressionType_ != ::WALB_DIFF_CMPR_NONE; }

    void copyFrom(const char *data, size_t size) {
        data_.resize(size);
        ::memcpy(&data_[0], data, size);
    }
    void moveFrom(std::vector<char> &&data) {
        data_ = std::move(data);
    }
    std::vector<char> &&forMove() { return std::move(data_); }

    const char *rawData() const override { return &data_[0]; }
    char *rawData() override { return &data_[0]; }
    size_t rawSize() const override { return data_.size(); }

    /**
     * Get a part of the diffIo.
     *
     * @offset [logical block]
     * @nBlocks [logical block]
     */
    std::shared_ptr<BlockDiffIo> getPortion(size_t offset, size_t nBlocks) const override {
        assert(offset < ioBlocks_);
        assert(0 < nBlocks);
        assert(nBlocks <= ioBlocks_);
        const size_t offB = offset * LOGICAL_BLOCK_SIZE;
        const size_t sizeB = nBlocks * LOGICAL_BLOCK_SIZE;
        auto iop = std::make_shared<BlockDiffIo>(nBlocks);
        if (isCompressed()) {
            std::vector<char> b(sizeB);
            size_t sizeB1 = sizeB;
            uncompress(getCompressionType(), rawData(), rawSize(), &b[0], sizeB1);
            assert(sizeB1 == sizeB);
            iop->copyFrom(&b[0] + offB, sizeB);
        } else {
            iop->copyFrom(rawData() + offB, sizeB);
        }
        return iop;
    }

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
    void writeDiff(const WalbDiffRecord &rec, std::shared_ptr<BlockDiffIo> iop) {
        if (!isWrittenHeader_) {
            throw RT_ERR("Call writeHeader() before calling writeDiff().");
        }

        /* Check data size. */
        if (0 < rec.dataSize() && rec.dataSize() != iop->rawSize()) {
            throw RT_ERR("rec.dataSize() %zu iop.rawSize %zu differ.",
                         rec.dataSize(), iop->rawSize());
        }

        /* Try to add. */
        if (pack_.add(*rec.rawRecord())) {
            ioPtrQ_.push(iop);
            return;
        }

        /* Flush. */
        writePack();

        /* Add. */
        UNUSED bool ret = pack_.add(*rec.rawRecord());
        assert(ret);
        ioPtrQ_.push(iop);
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
    std::vector<char> buf_;

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
     * DO NOT GC the *this before collecting the returned object.
     *
     * RETURN:
     *   a pair (diff record, diff io data).
     *   if the input stream reached the end, diff record will be nullptr.
     */
    std::pair<std::shared_ptr<WalbDiffRecord>, std::shared_ptr<BlockDiffIo> > readDiff() {
        if (pack_.isEnd() ||
            (recIdx_ == pack_.nRecords() && !readPackHeader())) {
            return std::make_pair(nullptr, nullptr);
        }
        auto recp = std::make_shared<WalbDiffRecord>(
            reinterpret_cast<const char*>(&pack_.record(recIdx_)),
            sizeof(struct walb_diff_record));
        std::shared_ptr<BlockDiffIo> iop = readDiffIo(*recp);

        assert(recp);
        if (recp->dataSize() == 0) {
            //recp->printOneline(); /* debug */
            assert(!iop);
        } else {
            assert(recp->dataSize() == iop->rawSize());
        }
        return std::make_pair(recp, iop);
    }

    /**
     * Read a diff IO.
     *
     * RETURN:
     *   False if the input stream reached the end.
     */
    bool readDiff(struct walb_diff_record &rec, std::vector<char> &data) {
        if (pack_.isEnd() ||
            (recIdx_ == pack_.nRecords() && !readPackHeader())) {
            return false;
        }
        rec = pack_.record(recIdx_);
        WalbDiffRecord recW(rec);
        data.resize(recW.dataSize());
        readDiffIo(recW, &data[0]);
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
     * RETURN:
     *   shared pointer of BlockDiffIo.
     *   If rec.dataSize() == 0, nullptr will be returned.
     */
    std::shared_ptr<BlockDiffIo> readDiffIo(const WalbDiffRecord &rec) {
        if (rec.dataOffset() != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.dataOffset(), totalSize_);
        }
        std::shared_ptr<BlockDiffIo> iop;
        if (0 < rec.dataSize()) {
            buf_.resize(rec.dataSize());
            iop = std::make_shared<BlockDiffIo>(rec.ioBlocks(), rec.compressionType());
        }
        readDiffIo(rec, &buf_[0]);
        if (0 < rec.dataSize()) {
            iop->copyFrom(&buf_[0], rec.dataSize());
        }
        return iop;
    }

    /**
     * Read a diff IO.
     * @rec corresponding diff record.
     * @data pointer to fill read IO data.
     *   buffer size must be >= rec.dataSize().
     */
    void readDiffIo(const WalbDiffRecord &rec, char *data) {
        if (rec.dataOffset() != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.dataOffset(), totalSize_);
        }
        if (0 < rec.dataSize()) {
            fdr_.read(data, rec.dataSize());
            uint32_t csum = cybozu::util::calcChecksum(data, rec.dataSize(), 0);
            if (rec.checksum() != csum) {
                throw RT_ERR("checksum invalid rec: %08x data: %08x.\n", rec.checksum(), csum);
            }
        }
        recIdx_++;
        totalSize_ += rec.dataSize();
    }
};

/**
 * Interface.
 */
struct WalbDiff
{
    virtual ~WalbDiff() noexcept = default;
    virtual bool init() = 0;
    virtual bool add(const WalbDiffRecord &rec, std::shared_ptr<BlockDiffIo> iop) = 0;
    virtual bool sync() = 0;
    virtual void print() const = 0;
    virtual uint64_t getNBlocks() const = 0;
    virtual uint64_t getNIos() const = 0;
    virtual WalbDiffFileHeader& header() = 0;
    virtual bool writeTo(int outFd) = 0;
    virtual void checkNoOverlappedAndSorted() const = 0;
};

/**
 * Diff record and its data.
 * Data compression is not supported.
 */
class RecData /* final */
{
private:
    WalbDiffRecord rec_;
    std::vector<char> data_;
public:
    struct walb_diff_record &rawRecord() { return *rec_.rawRecord(); }
    const struct walb_diff_record &rawRecord() const { return *rec_.rawRecord(); }
    WalbDiffRecord &record() { return rec_; }
    const WalbDiffRecord &record() const { return rec_; }

    char *rawData() { return &data_[0]; }
    const char *rawData() const { return &data_[0]; }
    uint32_t rawSize() const { return data_.size(); }

    void copyFrom(const struct walb_diff_record &rec, const void *data, size_t size) {
        rawRecord() = rec;
        if (rec_.isNormal()) {
            data_.resize(size);
            ::memcpy(&data_[0], data, size);
        } else {
            data_.resize(0);
        }
    }
    void copyFrom(const WalbDiffRecord &rec, const std::shared_ptr<BlockDiffIo> iop) {
        rec_ = rec;
        assert(!iop->isCompressed());
        if (rec_.isNormal()) {
            assert(rec_.ioBlocks() * LOGICAL_BLOCK_SIZE == iop->rawSize());
            data_.resize(iop->rawSize());
            ::memcpy(&data_[0], iop->rawData(), iop->rawSize());
        } else {
            data_.resize(0);
        }
    }
    void moveFrom(const WalbDiffRecord &rec, std::vector<char> &&data) {
        rec_ = rec;
        data_ = std::move(data);
    }
    std::vector<char> &&forMove() { return std::move(data_); }

    void updateChecksum() {
        rec_.setChecksum(calcCsum());
    }

    bool isValid(bool isChecksum = false) const {
        if (!rec_.isValid()) { return false; }
        if (!rec_.isNormal()) { return true; }
        if (rec_.isCompressed()) { return false; }
        if (rec_.dataSize() != data_.size()) { return false; }
        return isChecksum ? (rec_.checksum() == calcCsum()) : true;
    }

    void print(::FILE *fp) const {
        rec_.printOneline(fp);
        ::fprintf(fp, "size %zu checksum %0x\n"
                  , data_.size()
                  , cybozu::util::calcChecksum(&data_[0], data_.size(), 0));
    }

    void print() const { print(::stdout); }

    /**
     * Create (IO portions of rhs) - (that of *this).
     * If non-overlapped, throw runtime error.
     * The overlapped data of rhs will be used.
     * *this will not be changed.
     */
    std::vector<RecData> minus(const RecData &rhs) const {
        assert(isValid(true));
        assert(rhs.isValid(true));
        if (!rec_.isOverlapped(rhs.rec_)) {
            throw RT_ERR("Non-overlapped.");
        }
        std::vector<RecData> v;
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
            std::vector<char> data0(size0), data1(size1);
            if (rec_.isNormal()) {
                size_t off1 = (addr1 - rec_.ioAddress()) * LOGICAL_BLOCK_SIZE;
                assert(size0 + rhs.rec_.ioBlocks() * LOGICAL_BLOCK_SIZE + size1 == rec_.dataSize());
                rec0.setDataSize(size0);
                rec1.setDataSize(size1);
                ::memcpy(&data0[0], &data_[0], size0);
                ::memcpy(&data1[0], &data_[off1], size1);
            }

            if (0 < blks0) {
                RecData r;
                r.moveFrom(rec0, std::move(data0));
                r.updateChecksum();
                v.push_back(std::move(r));
            }
            if (0 < blks1) {
                RecData r;
                r.moveFrom(rec1, std::move(data1));
                r.updateChecksum();
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
                size = data_.size() - rblks * LOGICAL_BLOCK_SIZE;
            }
            std::vector<char> data(size);
            if (rec_.isNormal()) {
                assert(rec_.dataSize() == data_.size());
                rec.setDataSize(size);
                ::memcpy(&data[0], &data_[0], size);
            }

            RecData r;
            r.moveFrom(rec, std::move(data));
            r.updateChecksum();
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
            size = data_.size() - off;
        }
        std::vector<char> data(size);
        if (rec_.isNormal()) {
            assert(rec_.dataSize() == data_.size());
            rec.setDataSize(size);
            ::memcpy(&data[0], &data_[off], size);
        }
        assert(rhs.rec_.endIoAddress() == rec.ioAddress());
        RecData r;
        r.moveFrom(rec, std::move(data));
        r.updateChecksum();
        v.push_back(std::move(r));
        return std::move(v);
    }

private:
    uint32_t calcCsum() const {
        return cybozu::util::calcChecksum(&data_[0], data_.size(), 0);
    }
};

/**
 * Simpler implementation of in-memory walb diff data.
 * IO data compression is not supported.
 */
class WalbDiffMemory
    : public WalbDiff
{
private:
    std::map<uint64_t, RecData> map_;
    struct walb_diff_file_header h_;
    WalbDiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

public:
    WalbDiffMemory()
        : map_(), h_(), fileH_(h_), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~WalbDiffMemory() noexcept override = default;
    bool init() {
        /* Initialize always. */
        return false;
    }
    bool empty() const { return map_.empty(); }
    bool add(const WalbDiffRecord &rec, const void *data, size_t size) {
        /* Decide key range to search. */
        uint64_t addr0 = rec.ioAddress();
        if (addr0 <= fileH_.getMaxIoBlocks()) {
            addr0 = 0;
        } else {
            addr0 -= fileH_.getMaxIoBlocks();
        }
        /* Search overlapped items. */
        uint64_t addr1 = rec.endIoAddress();
        std::queue<RecData> q;
        auto it = map_.lower_bound(addr0);
        while (it != map_.end() && it->first < addr1) {
            RecData &r = it->second;
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
        RecData r0;
        r0.copyFrom(*rec.rawRecord(), data, size);
        while (!q.empty()) {
            std::vector<RecData> v = q.front().minus(r0);
            for (RecData &r : v) {
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
        map_.insert(std::make_pair(rec.ioAddress(), std::move(r0)));
        /* Update maxIoBlocks. */
        fileH_.setMaxIoBlocksIfNecessary(rec.ioBlocks());
        return true;
    }
    bool sync() override {
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
    void print() const override { print(::stdout); }
    uint64_t getNBlocks() const override { return nBlocks_; }
    uint64_t getNIos() const override { return nIos_; }
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
    WalbDiffFileHeader& header() override { return fileH_; }
    bool writeTo(int outFd) override {
        WalbDiffWriter writer(outFd);
        writer.writeHeader(fileH_);
        auto it = map_.cbegin();
        while (it != map_.cend()) {
            const RecData &r = it->second;
            auto iop = std::make_shared<BlockDiffIo>(r.record().ioBlocks());
            iop->copyFrom(r.rawData(), r.rawSize());
            writer.writeDiff(r.record(), iop);
            ++it;
        }
        writer.flush();
        return true;
    }
    void checkNoOverlappedAndSorted() const override {
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
    bool add(const WalbDiffRecord &rec, std::shared_ptr<BlockDiffIo> iop) override {
        if (rec.isNormal()) {
            return add(rec, iop->rawData(), iop->rawSize());
        } else {
            return add(rec, nullptr, 0);
        }
    }
    /**
     * C-like interface of add().
     */
    bool add(const struct walb_diff_record &rawRec, const void *data, size_t size) {
        return add(WalbDiffRecord(rawRec), data, size);
    }

    using Map = std::map<uint64_t, RecData>;
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
         * The iterator will indicate the removed item.
         * CAUSION:
         *   statistics will be incorrect.
         */
        void erase() {
            checkValid();
            mem_->nIos_--;
            mem_->nBlocks_ -= it_->second.record().ioBlocks();
            mem_->map_.erase(it_);
            if (mem_->map_.empty()) {
                mem_->fileH_.resetMaxIoBlocks();
            }
        }
        RecData &recData() {
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
