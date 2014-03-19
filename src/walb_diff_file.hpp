#pragma once
/**
 * @file
 * @brief walb diff utiltities for files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "walb_diff_pack.hpp"
#include "uuid.hpp"

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
    cybozu::Uuid getUuid2() const { return cybozu::Uuid(&h_.uuid[0]); }

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
    void writeDiff(const DiffRecord &rec0, const char *data0) {
        std::vector<char> data(data0, data0 + rec0.data_size);
        writeDiff(rec0, std::move(data));
    }
    void writeDiff(const DiffRecord &rec0, std::vector<char> &&data0) {
        checkWrittenHeader();
        IoData io;
        io.set(rec0);
        io.data.swap(data0);

        /* Try to add. */
        if (pack_.add(rec0)) {
            ioQ_.push(std::move(io));
            return;
        }

        /* Flush and add. */
        writePack();
        UNUSED bool ret = pack_.add(rec0);
        assert(ret);
        ioQ_.push(std::move(io));
    }

    /**
     * Compress and write a diff data.
     *
     * @rec record.
     * @data IO data.
     */
    void compressAndWriteDiff(const DiffRecord &rec, const char *data) {
        if (isCompressedRec(rec)) {
            writeDiff(rec, data);
            return;
        }
        if (!isNormalRec(rec)) {
            writeDiff(rec, {});
            return;
        }

        // QQQ refactor later
        IoData io1 = compressIoData(rec, data, ::WALB_DIFF_CMPR_SNAPPY);
        DiffRecord rec1 = rec;
        rec1.compression_type = ::WALB_DIFF_CMPR_SNAPPY;
        rec1.data_size = io1.data.size();
        rec1.checksum = io1.calcChecksum();
        writeDiff(rec1, std::move(io1.data));
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
            fdw_.write(io0.data.data(), io0.data.size());
            total += io0.data.size();
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
    bool readDiff(DiffRecord &rec, IoData &io) {
        if (!canRead()) return false;
        rec = pack_.record(recIdx_);

        if (!rec.isValid()) {
#ifdef DEBUG
            rec.print();
            pack_.record(recIdx_).print();
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
    bool readAndUncompressDiff(DiffRecord &rec, IoData &io) {
        IoData io0;
        if (!readDiff(rec, io0)) {
            rec.clearExists();
            io = std::move(io0);
            return false;
        }
        if (!isCompressedRec(rec)) {
            io = std::move(io0);
            return true;
        }
        io = uncompressIoData(io0);
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.data_size = io.data.size();
        rec.checksum = io.calcChecksum();
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
    void readDiffIo(const DiffRecord &rec, IoData &io) {
        if (rec.data_offset != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.data_offset, totalSize_);
        }
        const size_t recSize = rec.data_size;
        if (recSize > 0) {
            io.ioBlocks = rec.io_blocks;
            io.compressionType = rec.compression_type;
            io.data.resize(recSize);
            fdr_.read(io.data.data(), recSize);
            const uint32_t csum = cybozu::util::calcChecksum(io.data.data(), recSize, 0);
            if (rec.checksum != csum) {
                throw RT_ERR("checksum invalid rec: %08x data: %08x.\n", rec.checksum, csum);
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
