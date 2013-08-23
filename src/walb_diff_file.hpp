/**
 * @file
 * @brief walb diff utiltities for files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "walb_diff_base.hpp"

#ifndef WALB_DIFF_FILE_HPP
#define WALB_DIFF_FILE_HPP

namespace walb {
namespace diff {

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
    char *buf_;
    bool mustDelete_;

public:
    WalbDiffPack() : buf_(allocStatic()), mustDelete_(true) {}
    /**
     * Buffer size must be ::WALB_DIFF_PACK_SIZE.
     */
    explicit WalbDiffPack(char *buf) : buf_(buf), mustDelete_(false) {
        assert(buf);
    }
    WalbDiffPack(const WalbDiffPack &) = delete;
    WalbDiffPack(WalbDiffPack &&rhs)
        : buf_(nullptr), mustDelete_(false) {
        *this = std::move(rhs);
    }
    ~WalbDiffPack() noexcept {
        if (mustDelete_) ::free(buf_);
    }
    WalbDiffPack &operator=(const WalbDiffPack &) = delete;
    WalbDiffPack &operator=(WalbDiffPack &&rhs) {
        if (mustDelete_) {
            ::free(buf_);
            buf_ = nullptr;
        }
        buf_ = rhs.buf_;
        mustDelete_ = rhs.mustDelete_;
        rhs.buf_ = nullptr;
        rhs.mustDelete_ = false;
        return *this;
    }

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
    static char *allocStatic() {
        void *p;
        int ret = ::posix_memalign(
            &p, ::WALB_DIFF_PACK_SIZE, ::WALB_DIFF_PACK_SIZE);
        if (ret) throw std::bad_alloc();
        assert(p);
        return reinterpret_cast<char *>(p);
    }

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

}} //namespace walb::diff

#endif /* WALB_DIFF_FILE_HPP */
