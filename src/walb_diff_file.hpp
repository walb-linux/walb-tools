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

/**
 * Walb diff header data.
 */
struct DiffFileHeader : walb_diff_file_header
{
    uint32_t getChecksum() const { return checksum; }
    uint16_t getMaxIoBlocks() const { return max_io_blocks; }
    const uint8_t *getUuid() const { return &uuid[0]; }
    cybozu::Uuid getUuid2() const { return cybozu::Uuid(&uuid[0]); }

    void setMaxIoBlocksIfNecessary(uint16_t ioBlocks) {
        if (max_io_blocks < ioBlocks) {
            max_io_blocks = ioBlocks;
        }
    }

    void resetMaxIoBlocks() { max_io_blocks = 0; }

	size_t getSize() const { return sizeof(walb_diff_file_header); }

    bool isValid() const {
        return cybozu::util::calcChecksum(this, getSize(), 0) == 0;
    }

    void updateChecksum() {
        checksum = 0;
        checksum = cybozu::util::calcChecksum(this, getSize(), 0);
    }

    void setUuid(const void *uuid) {
        ::memcpy(&this->uuid[0], uuid, UUID_SIZE);
    }

    void print(::FILE *fp) const {
        ::fprintf(fp, "-----walb_file_header-----\n"
                  "checksum: %08x\n"
                  "maxIoBlocks: %u\n"
                  "uuid: ",
                  checksum, max_io_blocks);
        for (size_t i = 0; i < UUID_SIZE; i++) {
            ::fprintf(fp, "%02x", getUuid()[i]);
        }
        ::fprintf(fp, "\n");
    }

    void print() const { print(::stdout); }

    void init() {
        ::memset(this, 0, getSize());
    }
};

namespace diff {

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
    DiffPackHeader pack_;
    std::queue<DiffIo> ioQ_;

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
    void writeHeader(DiffFileHeader &header) {
        if (isWrittenHeader_) {
            throw RT_ERR("Do not call writeHeader() more than once.");
        }
        header.updateChecksum();
        assert(header.isValid());
        fdw_.write(&header, header.getSize());
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
        DiffIo io;
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
        if (rec.isCompressed()) {
            writeDiff(rec, data);
            return;
        }
        if (!rec.isNormal()) {
            writeDiff(rec, {});
            return;
        }

        DiffRecord compRec;
        std::vector<char> compData = rec.compress(compRec, data);
        writeDiff(compRec, std::move(compData));
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
            DiffIo io0 = std::move(ioQ_.front());
            ioQ_.pop();
            if (io0.empty()) continue;
            fdw_.write(io0.get(), io0.getSize());
            total += io0.getSize();
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
    std::unique_ptr<cybozu::util::FileOpener> opener_;
    int fd_;
    cybozu::util::FdReader fdr_;
    bool isReadHeader_;

    /* Buffers. */
    DiffPackHeader pack_;
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
    void readHeader(DiffFileHeader &head, bool doReadHeader = true) {
        if (isReadHeader_) {
            throw RT_ERR("Do not call readHeader() more than once.");
        }
        fdr_.read(&head, head.getSize());
        if (!head.isValid()) {
            throw RT_ERR("diff header invalid.\n");
        }
        isReadHeader_ = true;
        if (doReadHeader) readPackHeader();
    }
    /**
     * Read header data with another interface.
     */
    void readHeaderWithoutReadingPackHeader(DiffFileHeader &head) {
        readHeader(head, false);
    }

    /**
     * Read a diff IO.
     *
     * RETURN:
     *   false if the input stream reached the end.
     */
    bool readDiff(DiffRecord &rec, DiffIo &io) {
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
    bool readAndUncompressDiff(DiffRecord &rec, DiffIo &io) {
        if (!readDiff(rec, io)) {
            rec.clearExists();
            return false;
        }
        if (!rec.isCompressed()) {
            return true;
        }
        io.uncompress();
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.data_size = io.getSize();
        rec.checksum = io.calcChecksum();
        return true;
    }

    bool canRead() {
        if (pack_.isEnd() ||
            (recIdx_ == pack_.nRecords() && !readPackHeader())) {
            return false;
        }
        return true;
    }

    bool readPackHeader(DiffPackHeader& pack) {
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
    void readDiffIo(const DiffRecord &rec, DiffIo &io) {
        if (rec.data_offset != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.data_offset, totalSize_);
        }
        const size_t recSize = rec.data_size;
        if (recSize > 0) {
            io.ioBlocks = rec.io_blocks;
            io.compressionType = rec.compression_type;
            io.data.resize(recSize);
            fdr_.read(io.get(), recSize);
            const uint32_t csum = cybozu::util::calcChecksum(io.get(), recSize, 0);
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
