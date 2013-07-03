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
        if (isCheck && !rec.isValid()) { throw RT_ERR("invalid record."); }
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
        if (!isNormal()) { return true; }
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
    size_t rawSize() const override { return sizeof(rec_); }
    const char *rawData() const override { return ptr<char>(); }
    char *rawData() { return ptr<char>(); }
    const struct walb_diff_record *rawRecord() const { return &rec_; }

    uint8_t compressionType() const { return rec_.compression_type; }
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
    }
    void setAllZero() {
        rec_.flags |= (1U << ::WALB_DIFF_FLAG_ALLZERO);
    }
    void setDiscard() {
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
    virtual ~WalbDiffKey() noexcept override = default;

    struct Min : public WalbDiffRecord {
        Min() : WalbDiffRecord(uint64_t(0), uint16_t(0)) {}
        virtual ~Min() noexcept override = default;
    };

    struct Max : public WalbDiffRecord {
        Max() : WalbDiffRecord(uint64_t(-1), 0) {}
        virtual ~Max() noexcept override = default;
    };
    struct Header : public WalbDiffRecord {
        Header() : WalbDiffRecord(uint64_t(-1), uint16_t(-1)) {}
        virtual ~Header() noexcept override = default;
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
    std::shared_ptr<char> data_;
    size_t bufSize_; /* [byte]. */
    size_t compressedSize_; /* [byte]. */

public:
    BlockDiffIo(uint16_t ioBlocks, int compressionType = ::WALB_DIFF_CMPR_NONE)
        : ioBlocks_(ioBlocks)
        , compressionType_(compressionType)
        , data_()
        , bufSize_(0)
        , compressedSize_(0) {}

    BlockDiffIo()
        : ioBlocks_(0)
        , compressionType_(::WALB_DIFF_CMPR_NONE)
        , data_()
        , bufSize_(0)
        , compressedSize_(0) {}

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

    bool put(std::shared_ptr<char> inData) {
        const size_t size = ioBlocks_ * LOGICAL_BLOCK_SIZE;
        if (!isCompressed()) {
            data_ = inData;
            bufSize_ = size;
            compressedSize_ = size;
            return true;
        }
        if (bufSize_ < size) {
            data_ = cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, size);
            bufSize_ = size;
        }
        compressedSize_ = bufSize_;
        return compress(compressionType_, inData.get(), size,
                        data_.get(), compressedSize_);
    }

    /**
     * @inData compressed data as a byte array.
     * @size compressed size [byte].
     */
    void putCompressed(std::shared_ptr<char> inData, size_t size) {
        if (!inData) { assert(size == 0); }
        data_ = inData;
        bufSize_ = size;
        compressedSize_ = size;
    }

    std::shared_ptr<char> get() {
        if (!isCompressed()) {
            return data_;
        }
        return getDetail();
    }

    std::shared_ptr<char> get() const {
        return getDetail();
    }

    std::shared_ptr<char> getCompressed() {
        return data_;
    }

    const char *rawData() const override {
        if (!data_) { return nullptr; }
        return data_.get();
    }
    char *rawData() override {
        if (!data_) { return nullptr; }
        return data_.get();
    }
    size_t rawSize() const override { return compressedSize_; }

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

        std::shared_ptr<BlockDiffIo> iop(new BlockDiffIo(nBlocks));
        std::shared_ptr<char> b = cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, sizeB);

        std::shared_ptr<char> p = get();
        ::memcpy(b.get(), p.get() + offB, sizeB);

        iop->put(b);
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

private:
    std::shared_ptr<char> getDetail() const {
        if (!data_) {
            return std::shared_ptr<char>();
        }
        const size_t size = ioBlocks_ * LOGICAL_BLOCK_SIZE;
        size_t outSize = size;
        std::shared_ptr<char> outData = cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, size);
        LOGd_("ioBlocks %u size %zu outData %p\n", ioBlocks_, size, outData.get());
        bool ret = uncompress(compressionType_, data_.get(), compressedSize_,
                              outData.get(), outSize);
        if (!ret || outSize != size) {
            return std::shared_ptr<char>();
        }
        return outData;
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

    /* Buffers. */
    WalbDiffPack pack_;
    std::queue<std::shared_ptr<BlockDiffIo> > ioPtrQ_;

public:
    explicit WalbDiffWriter(int fd)
        : opener_(), fd_(fd), fdw_(fd)
        , isWrittenHeader_(false)
        , pack_()
        , ioPtrQ_() {}

    explicit WalbDiffWriter(const std::string &diffPath, int flags, mode_t mode)
        : opener_(new cybozu::util::FileOpener(diffPath, flags, mode))
        , fd_(opener_->fd())
        , fdw_(fd_)
        , isWrittenHeader_(false)
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
        flush();
        writeEof();
        if (opener_) { opener_->close(); }
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
     *
     * DO NOT GC the *this before collecting the returned object.
     */
    std::shared_ptr<WalbDiffFileHeader> readHeader() {
        if (isReadHeader_) {
            throw RT_ERR("Do not call readHeader() more than once.");
        }
        auto p = std::make_shared<WalbDiffFileHeaderWithBody>();
        fdr_.read(p->rawData(), p->rawSize());
        if (!p->isValid()) {
            throw RT_ERR("diff header invalid.\n");
        }
        isReadHeader_ = true;
        readPackHeader();
        return p;
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
        std::shared_ptr<char> p;
        if (0 < rec.dataSize()) {
            iop.reset(new BlockDiffIo(rec.ioBlocks(), rec.compressionType()));
            //rec.printOneline();
            if (rec.compressionType() != ::WALB_DIFF_CMPR_NONE) {
                p = cybozu::util::allocateMemory<char>(rec.dataSize());
            } else {
                if (rec.dataSize() % LOGICAL_BLOCK_SIZE != 0) {
                    throw RT_ERR("Uncompressed diff IO data size must multiple of logical block size.");
                }
                size_t nr = rec.dataSize() / LOGICAL_BLOCK_SIZE;
                p = cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, LOGICAL_BLOCK_SIZE, nr);
            }
            fdr_.read(p.get(), rec.dataSize());
            iop->putCompressed(p, rec.dataSize());
        }

        uint32_t csum = cybozu::util::calcChecksum(p.get(), rec.dataSize(), 0);
        if (rec.checksum() != csum) {
            throw RT_ERR("checksum invalid %08x %08x.\n", rec.checksum(), csum);
        }

        recIdx_++;
        totalSize_ += rec.dataSize();
        return iop;
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
 * In-memory walb diff manager.
 */
class WalbDiffMemory /* final */
    : public WalbDiff
{
private:
    using WalbDiffRecordPtr = std::shared_ptr<WalbDiffRecord>;
    using BlockDiffIoPtr = std::shared_ptr<BlockDiffIo>;
    using Key = std::pair<uint64_t, uint16_t>;
    using Value = std::pair<WalbDiffRecordPtr, BlockDiffIoPtr>;
    using Map = std::map<Key, Value>;

    Map map_;
    struct walb_diff_file_header h_;
    WalbDiffFileHeader fileH_;
    uint64_t nIos_; /* Number of IOs in the diff. */
    uint64_t nBlocks_; /* Number of logical blocks in the diff. */

    template <typename DiffMemory, typename MapIterator>
    class IteratorBase {
    protected:
        DiffMemory &wdiffMem_;
        MapIterator it_;

    public:
        explicit IteratorBase(DiffMemory &wdiffMem)
            : wdiffMem_(wdiffMem), it_() {}
        virtual ~IteratorBase() noexcept {}

        virtual bool isValid() const = 0;
        virtual void begin() = 0;
        virtual void end() = 0;

        void next() { ++it_; }
        void prev() { --it_; }
        void lowerBound(const WalbDiffKey &key) {
            it_ = wdiffMem_.map_.lower_bound(wdiffMem_.getKey(key));
        }
        void upperBound(const WalbDiffKey &key) {
            it_ = wdiffMem_.map_.upper_bound(wdiffMem_.getKey(key));
        }

        const WalbDiffRecord &record() const {
            if (!isValid()) { throw RT_ERR("invalid iterator position."); }
            Value v = it_->second;
            assert(v.first); /* must not be nullptr */
            return *v.first;
        }

        WalbDiffRecord &record() {
            if (!isValid()) { throw RT_ERR("invalid iterator position."); }
            Value v = it_->second;
            assert(v.first); /* must not be nullptr */
            return *v.first;
        }

#if 0
        const BlockDiffIo &diffIo() const {
            BlockDiffIoPtr iop = diffIoPtr();
            if (!iop) { throw RT_ERR("diffIo null."); }
            return *iop;
        }
#endif

        BlockDiffIoPtr diffIoPtr() const {
            if (!isValid()) { throw RT_ERR("invalid iterator position."); }
            Value v = it_->second;
            return v.second;
        }
    };
public:
    WalbDiffMemory()
        : map_(), h_(), fileH_(h_), nIos_(0), nBlocks_(0) {
        fileH_.init();
    }
    ~WalbDiffMemory() noexcept = default;

    bool init() {
        /* Initialize always. */
        return false;
    }

    /**
     * Add an IO.
     * Overlapped IOs will be updated appropriately.
     *
     * RETURN:
     *   never return false.
     */
    bool add(const WalbDiffRecord &rec, std::shared_ptr<BlockDiffIo> ioPtr) override {
        /* Decide key range of candidates. */
        uint64_t addr0 = rec.ioAddress();
        if (addr0 <= fileH_.getMaxIoBlocks()) {
            addr0 = 0;
        } else {
            addr0 -= fileH_.getMaxIoBlocks();
        }
        uint64_t addr1 = rec.ioAddress() + rec.ioBlocks();

        /* Search overlapped IOs. */
        Key key0 = std::make_pair(addr0, 0);
        Key key1 = std::make_pair(addr1, 0);
        Key key;
        std::queue<Value> q;
        Map::iterator it = map_.upper_bound(key0);
        while (it != map_.end() && (key = it->first, key < key1)) {
            Value v = it->second;
            WalbDiffRecordPtr recp = v.first;
            assert(recp->isValid());
            if (recp->isOverlapped(rec)) { q.push(v); }
            ++it;
        }
        while (!q.empty()) {
            Value v = q.front();
            q.pop();
            WalbDiffRecordPtr recp = v.first;
            BlockDiffIoPtr iop = v.second;
            if (!del(*recp)) { throw RT_ERR("delete failed"); }
            putNonOverlappedArea(*recp, *iop, rec);
        }

        /* Insert */
        auto r = std::make_shared<WalbDiffRecord>(rec);
        if (!put(r, ioPtr)) {
            throw RT_ERR("put failed.");
        }

        /* Update maxIoBlocks */
        fileH_.setMaxIoBlocksIfNecessary(rec.ioBlocks());
        return true;
    }
    bool sync() { /* do nothing. */ return true; }
    void print(::FILE *fp) const {
        Map::const_iterator it = map_.cbegin();
        while (it != map_.cend()) {
            WalbDiffRecord &rec = *it->second.first;
            rec.printOneline(fp);
            ++it;
        }
    }
    void print() const { print(::stdout); }

    void checkStatistics() const {
        uint64_t nBlocks = 0;
        uint64_t nIos = 0;
        Map::const_iterator it = map_.cbegin();
        while (it != map_.cend()) {
            WalbDiffRecord &rec = *it->second.first;
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

    uint64_t getNBlocks() const override { return nBlocks_; }
    uint64_t getNIos() const override { return nIos_; }

    WalbDiffFileHeader& header() override { return fileH_; }

    bool writeTo(int outFd) override {
        ::printf("writeTo %d\n", outFd); /* debug */
        WalbDiffWriter walbDiffWriter(outFd);
        walbDiffWriter.writeHeader(fileH_);
        Map::const_iterator it = map_.cbegin();
        while (it != map_.cend()) {
            Value v = it->second;
            WalbDiffRecordPtr recp = v.first;
            BlockDiffIoPtr iop = v.second;
            walbDiffWriter.writeDiff(*recp, iop);
            it++;
        }
        walbDiffWriter.flush();
        return true;
    }
    void compactRange(const WalbDiffRecord *, const WalbDiffRecord *) {
        /* do nothing. */
    }
    void checkNoOverlappedAndSorted() const override {
        Map::const_iterator it = map_.cbegin();
        WalbDiffRecord prevKey;
        while (it != map_.cend()) {
            WalbDiffRecord &key = *it->second.first;
            if (it != map_.cbegin()) {
                if (prevKey.isOverlapped(key)) {
                    LOGe("overlap!!! (%" PRIu64 " %u) (%" PRIu64 " %u)\n",
                         prevKey.ioAddress(), prevKey.ioBlocks(),
                         key.ioAddress(), key.ioBlocks());
                    throw RT_ERR("Overlapped records exist.");
                }
                if (key.ioAddress() <= prevKey.ioAddress()) {
                    throw RT_ERR("Not sorted.");
                }
            }
            prevKey.setIoAddress(key.ioAddress());
            prevKey.setIoBlocks(key.ioBlocks());
            ++it;
        }
    }

    bool empty() const { return map_.empty(); }

    class ConstIterator /* final */
        : public IteratorBase<const WalbDiffMemory, Map::const_iterator> {
    public:
        explicit ConstIterator(const WalbDiffMemory &wdiffMem)
            : IteratorBase<const WalbDiffMemory, Map::const_iterator>(wdiffMem) {}
        ~ConstIterator() noexcept override {}

        bool isValid() const override { return it_ != wdiffMem_.map_.cend(); }
        void begin() override { it_ = wdiffMem_.map_.cbegin(); }
        void end() override { it_ = wdiffMem_.map_.cend(); }
    };

    class Iterator /* final */
        : public IteratorBase<WalbDiffMemory, Map::iterator> {
    public:
        explicit Iterator(WalbDiffMemory &wdiffMem)
            : IteratorBase<WalbDiffMemory, Map::iterator>(wdiffMem) {}
        ~Iterator() noexcept override {}

        bool isValid() const override { return it_ != wdiffMem_.map_.end(); }
        void begin() override { it_ = wdiffMem_.map_.begin(); }
        void end() override { it_ = wdiffMem_.map_.end(); }

        /**
         * Erase the item on the iterator.
         * The iterator will indicate the removed item.
         *
         * CAUSION:
         *   statistics will be incorrect.
         */
        void erase() {
            if (!isValid()) {
                throw RT_ERR("tried to delete an item on the invalid iterator.");
            }
            wdiffMem_.map_.erase(it_);
        }
    };

    /**
     * Get an const iterator.
     *
     * You must not change the map contents during traverse of the map.
     */
    std::unique_ptr<ConstIterator> constIterator() const {
        std::unique_ptr<ConstIterator> p(new ConstIterator(*this));
        return std::move(p);
    }

    /**
     * Get an iterator.
     *
     * You can delete items on the iterator.
     */
    std::unique_ptr<Iterator> iterator() {
        std::unique_ptr<Iterator> p(new Iterator(*this));
        return std::move(p);
    }

private:
    void putNonOverlappedArea(
        WalbDiffRecord &rec, BlockDiffIo &io, const WalbDiffRecord &overlappedRec) {

        assert(rec.isOverlapped(overlappedRec));
        if (rec.isOverwrittenBy(overlappedRec)) { return; }
        LOGd_("deleted (%" PRIu64 " %u)\n", rec.ioAddress(), rec.ioBlocks());
        uint64_t off0, off1;
        uint16_t blocks;

        assert(rec.isValid());
        assert(overlappedRec.isValid());

        /* Left portion is not overlapped. */
        off0 = rec.ioAddress();
        off1 = overlappedRec.ioAddress();
        if (off0 < off1) {
            assert(off1 - off0 < (2U << 16));
            blocks = off1 - off0;
            LOGd_("try to add (%" PRIu64 " %u)\n", off0, blocks);
            auto mrec = std::make_shared<WalbDiffRecord>(rec, off0, blocks);
            assert(mrec->isValid());
            BlockDiffIoPtr iop;
            if (!mrec->isDiscard() && !mrec->isAllZero()) {
                iop = io.getPortion(0, blocks);
                mrec->setChecksum(iop->calcChecksum());
            }
            if (!put(mrec, iop)) {
                throw RT_ERR("Put left portion failed.");
            }
            LOGd_("added (%" PRIu64 " %u)\n", off0, blocks);
        }

        /* Right portion is not overlapped. */
        off0 = overlappedRec.ioAddress() + overlappedRec.ioBlocks();
        off1 = rec.ioAddress() + rec.ioBlocks();
        if (off0 < off1) {
            assert(off1 - off0 < (2U << 16));
            blocks = off1 - off0;
            LOGd_("try to add (%" PRIu64 " %u)\n", off0, blocks);
            auto mrec = std::make_shared<WalbDiffRecord>(rec, off0, blocks);
            assert(mrec->isValid());
            BlockDiffIoPtr iop;
            if (!mrec->isDiscard() && !mrec->isAllZero()) {
                iop = io.getPortion(rec.ioBlocks() - blocks, blocks);
                mrec->setChecksum(iop->calcChecksum());
            }
            if (!put(mrec, iop)) {
                throw RT_ERR("Put right portion failed.");
            }
            LOGd_("added (%" PRIu64 " %u)\n", off0, blocks);
        }
    }

    bool put(WalbDiffRecordPtr rec, BlockDiffIoPtr io) {
        assert(rec->isValid());
        std::pair<Map::iterator, bool> p =
            map_.insert(std::make_pair(getKey(*rec), std::make_pair(rec, io)));
        nIos_++;
        nBlocks_ += rec->ioBlocks();
        return p.second;
    }

    bool del(Map::iterator &it) {
        WalbDiffRecordPtr rec = it->second.first;
        nIos_--;
        nBlocks_ -= rec->ioBlocks();
        map_.erase(it);
        return true;
    }

    bool del(const WalbDiffRecord &rec) {
        nIos_--;
        nBlocks_ -= rec.ioBlocks();
        size_t n = map_.erase(getKey(rec));
        return n == 1;
    }

    Key getKey(const WalbDiffRecord &rec) const {
        return std::make_pair(rec.ioAddress(), rec.ioBlocks());
    }
};

}} //namesapce walb::diff

#endif /* WALB_DIFF_UTIL_HPP */
