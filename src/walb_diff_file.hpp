#pragma once
/**
 * @file
 * @brief walb diff utiltities for files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "walb_diff_pack.hpp"

namespace walb {
namespace diff {

/**
 * Walb diff header data.
 */
class FileHeaderWrap
{
private:
    struct walb_diff_file_header &h_;

public:
    FileHeaderWrap(struct walb_diff_file_header &h) : h_(h) {}
    virtual ~FileHeaderWrap() noexcept = default;

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

    void setUuid(const void *uuid) {
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
class FileHeaderRaw
    : public FileHeaderWrap
{
private:
    struct walb_diff_file_header header_;

public:
    FileHeaderRaw()
        : FileHeaderWrap(header_), header_() {}
    ~FileHeaderRaw() noexcept = default;
};

/**
 * Walb diff writer.
 */
class Writer /* final */
{
private:
    std::shared_ptr<cybozu::util::FileOpener> opener_;
    int fd_;
    cybozu::util::FdWriter fdw_;
    bool isWrittenHeader_;
    bool isClosed_;

    /* Buffers. */
    PackHeader pack_;
    std::queue<IoData> ioQ_;

public:
    explicit Writer(int fd)
        : opener_(), fd_(fd), fdw_(fd)
        , isWrittenHeader_(false)
        , isClosed_(false)
        , pack_()
        , ioQ_() {}

    explicit Writer(const std::string &diffPath, int flags, mode_t mode)
        : opener_(new cybozu::util::FileOpener(diffPath, flags, mode))
        , fd_(opener_->fd())
        , fdw_(fd_)
        , isWrittenHeader_(false)
        , isClosed_(false)
        , pack_()
        , ioQ_() {
        assert(0 < fd_);
    }

    ~Writer() noexcept {
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
    void writeHeader(FileHeaderWrap &header) {
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
     * @data0 IO data.
     */
    void writeDiff(const walb_diff_record &rec0, const char *data0) {
        const RecordWrapConst rec(&rec0);
        std::vector<char> data(rec.dataSize());
        ::memcpy(&data[0], data0, rec.dataSize());
        writeDiff(rec0, std::move(data));
    }
    void writeDiff(const walb_diff_record &rec0, std::vector<char> &&data0) {
        checkWrittenHeader();
        const RecordWrapConst rec(&rec0);
        IoData io;
        io.set(rec0, std::move(data0));
        check(rec, io);

        /* Try to add. */
        if (pack_.add(rec.record())) {
            ioQ_.push(std::move(io));
            return;
        }

        /* Flush and add. */
        writePack();
        UNUSED bool ret = pack_.add(rec.record());
        assert(ret);
        ioQ_.push(std::move(io));
    }

    /**
     * Compress and write a diff data.
     *
     * @rec record.
     * @data IO data.
     */
    void compressAndWriteDiff(const walb_diff_record &rec, const char *data) {
        const RecordWrapConst rec0(&rec);
        if (rec0.isCompressed()) {
            writeDiff(rec, data);
            return;
        }
        IoWrap io0;
        io0.set(rec, data, rec0.dataSize());
        check(rec0, io0);

        if (!rec0.isNormal()) {
            assert(io0.empty());
            writeDiff(rec, {});
            return;
        }

        RecordRaw rec1(rec);
        IoData io1 = compressIoData(io0, ::WALB_DIFF_CMPR_SNAPPY);
        rec1.setCompressionType(::WALB_DIFF_CMPR_SNAPPY);
        rec1.setDataSize(io1.size);
        rec1.setChecksum(io1.calcChecksum());
        writeDiff(rec1.record(), io1.forMove());
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
        if (pack_.nRecords() == 0) {
            assert(ioQ_.empty());
            return;
        }

        size_t total = 0;
        pack_.updateChecksum();
        fdw_.write(pack_.rawData(), pack_.rawSize());

        assert(pack_.nRecords() == ioQ_.size());
        while (!ioQ_.empty()) {
            IoData io0 = std::move(ioQ_.front());
            ioQ_.pop();
            if (io0.empty()) continue;
            fdw_.write(io0.rawData(), io0.size);
            total += io0.size;
        }
        assert(total == pack_.totalSize());
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
private:
    /**
     * Check record and IO pair.
     */
    void check(UNUSED const RecordWrapConst &rec, UNUSED const IoWrap &io) const {
        assert(rec.isValid());
        assert(io.isValid());
        assert(rec.dataSize() == io.size);
        if (rec.isNormal()) {
            assert(rec.compressionType() == io.compressionType);
            assert(rec.ioBlocks() == io.ioBlocks);
            assert(rec.checksum() == io.calcChecksum());
        } else {
            assert(io.empty());
        }
    }
};

/**
 * Read walb diff data from an input stream.
 * usage1
 *   (1) call readHeader() just once.
 *   (2) call readDiff() / readAndUncompressDiff().
 *   (3) repeat (2) until readDiff() returns false.
 * usage2
 *   (1) call readHeaderWithoutReadingPackHeader() just once.
 *   (2) call readDiffIo() multiple times after readPackHeader() once.
 *   (3) repeat (2) until readPackHeader() returns false.
 */
class Reader
{
private:
    std::shared_ptr<cybozu::util::FileOpener> opener_;
    int fd_;
    cybozu::util::FdReader fdr_;
    bool isReadHeader_;

    /* Buffers. */
    PackHeader pack_;
    uint16_t recIdx_;
    uint32_t totalSize_;

public:
    explicit Reader(int fd)
        : opener_(), fd_(fd), fdr_(fd)
        , isReadHeader_(false)
        , pack_()
        , recIdx_(0)
        , totalSize_(0) {}

    explicit Reader(const std::string &diffPath, int flags)
        : opener_(new cybozu::util::FileOpener(diffPath, flags))
        , fd_(opener_->fd())
        , fdr_(fd_)
        , isReadHeader_(false)
        , pack_()
        , recIdx_(0)
        , totalSize_(0) {
        assert(0 < fd_);
    }

    ~Reader() noexcept {
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
    std::shared_ptr<FileHeaderWrap> readHeader() {
        auto p = std::make_shared<FileHeaderRaw>();
        readHeader(*p);
        return p;
    }

    /**
     * Read header data with another interface.
     */
    void readHeaderWithoutReadingPackHeader(FileHeaderWrap &head) {
        readHeader(head, false);
    }
    void readHeader(FileHeaderWrap &head, bool doReadHeader = true) {
        if (isReadHeader_) {
            throw RT_ERR("Do not call readHeader() more than once.");
        }
        fdr_.read(head.rawData(), head.rawSize());
        if (!head.isValid()) {
            throw RT_ERR("diff header invalid.\n");
        }
        isReadHeader_ = true;
        if (doReadHeader) readPackHeader();
    }

    /**
     * Read a diff IO.
     *
     * RETURN:
     *   false if the input stream reached the end.
     */
    bool readDiff(Record &rec, IoData &io) {
        if (!canRead()) return false;
        ::memcpy(rec.rawData(), &pack_.record(recIdx_), sizeof(struct walb_diff_record));

        if (!rec.isValid()) {
#ifdef DEBUG
            rec.print(::stderr);
            const RecordWrapConst rec1(&pack_.record(recIdx_));
            rec1.print(::stderr);
#endif
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
    bool readAndUncompressDiff(Record &rec, IoData &io) {
        RecordRaw rec0;
        IoData io0;
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
        io = uncompressIoData(io0);
        rec.setCompressionType(::WALB_DIFF_CMPR_NONE);
        rec.setDataSize(io.size);
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

    bool readPackHeader(PackHeader& pack) {
        try {
            fdr_.read(pack.rawData(), pack.rawSize());
        } catch (cybozu::util::EofError &e) {
            return false;
        }
        if (!pack.isValid()) {
            throw RT_ERR("pack header invalid.");
        }
        if (pack.isEnd()) { return false; }
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
    void readDiffIo(const Record &rec, IoData &io) {
        if (rec.dataOffset() != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.dataOffset(), totalSize_);
        }
        const size_t recSize = rec.dataSize();
        if (recSize > 0) {
            io.setByWritter(rec.ioBlocks(), rec.compressionType(), recSize, [&](char *p) {
                fdr_.read(p, recSize);
                return recSize;
            });
            uint32_t csum = cybozu::util::calcChecksum(io.rawData(), io.size, 0);
            if (rec.checksum() != csum) {
                throw RT_ERR("checksum invalid rec: %08x data: %08x.\n", rec.checksum(), csum);
            }
            totalSize_ += recSize;
        }
        recIdx_++;
    }
private:
    /**
     * Read pack header.
     *
     * RETURN:
     *   false if EofError caught.
     */
    bool readPackHeader() {
        return readPackHeader(pack_);
    }
};

}} //namespace walb::diff
