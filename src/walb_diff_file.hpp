#pragma once
/**
 * @file
 * @brief walb diff utiltities for files.
 */
#include "walb_diff_pack.hpp"
#include "walb_diff_stat.hpp"
#include "uuid.hpp"
#include "cybozu/exception.hpp"

namespace walb {

/**
 * Walb diff header data.
 */
struct DiffFileHeader : walb_diff_file_header
{
    DiffFileHeader() {
        init();
    }
    uint32_t getChecksum() const { return checksum; }
    uint16_t getMaxIoBlocks() const { return max_io_blocks; }
    cybozu::Uuid getUuid() const { return cybozu::Uuid(&uuid[0]); }

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

    void setUuid(const cybozu::Uuid& uuid) {
        uuid.copyTo(this->uuid);
    }

    std::string str() const {
        return cybozu::util::formatString(
            "wdiff_h:\n"
            "  checksum: %08x\n"
            "  maxIoBlocks: %u\n"
            "  uuid: %s\n"
            , checksum, max_io_blocks, getUuid().str().c_str());
    }
    friend inline std::ostream& operator<<(std::ostream &os, const DiffFileHeader &fileH) {
        os << fileH.str();
        return os;
    }
    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s", str().c_str());
    }

    void init() {
        ::memset(this, 0, getSize());
    }
    template<class Writer>
    void writeTo(Writer& writer) {
        updateChecksum();
        writer.write(this, getSize());
    }
    template<class Reader>
    void readFrom(Reader& reader) {
        reader.read(this, getSize());
        if (!isValid()) throw cybozu::Exception("DiffFileHeader:readFrom:bad checksum");
    }
};

template <class Writer>
inline void writeDiffFileHeader(Writer& writer, uint16_t maxIoBlocks, const cybozu::Uuid &uuid)
{
    DiffFileHeader fileH;
    fileH.setMaxIoBlocksIfNecessary(maxIoBlocks);
    fileH.setUuid(uuid);
    fileH.writeTo(writer);
}

template<class Writer>
inline void writeDiffEofPack(Writer& writer)
{
    char buf[WALB_DIFF_PACK_SIZE];
    DiffPackHeader &pack = *(DiffPackHeader*)buf;
    pack.reset();
    pack.setEnd();
    pack.writeTo(writer);
}

/**
 * Walb diff writer.
 */
class DiffWriter /* final */
{
private:
    cybozu::util::File fileW_;
    bool isWrittenHeader_;
    bool isClosed_;

    /* Buffers. */
    char buf_[WALB_DIFF_PACK_SIZE];
    DiffPackHeader &pack_;

    std::queue<DiffIo> ioQ_;

    DiffStatistics stat_;

public:
    DiffWriter() : pack_(*(DiffPackHeader *)buf_) {
        init();
    }
    explicit DiffWriter(int fd) : DiffWriter() {
        fileW_.setFd(fd);
    }
    explicit DiffWriter(const std::string &diffPath, int flags, mode_t mode)
        : DiffWriter() {
        fileW_.open(diffPath, flags, mode);
    }
    ~DiffWriter() noexcept {
        try {
            close();
        } catch (...) {}
    }

    void close() {
        if (!isClosed_) {
            flush();
            writeEof();
            fileW_.close();
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
        header.writeTo(fileW_);
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
        std::vector<char> compData = rec.tryCompress(compRec, data);
        writeDiff(compRec, std::move(compData));
    }

    /**
     * Write buffered data.
     */
    void flush() {
        writePack();
    }

    const DiffStatistics& getStat() const {
        return stat_;
    }
private:
    void init() {
        isWrittenHeader_ = false;
        isClosed_ = false;
        pack_.reset();
        while (!ioQ_.empty()) ioQ_.pop();
        stat_.clear();
        stat_.wdiffNr = 1;
    }
    /* Write the buffered pack and its related diff ios. */
    void writePack() {
        if (pack_.n_records == 0) {
            assert(ioQ_.empty());
            return;
        }

        stat_.update(pack_);
        pack_.writeTo(fileW_);

        assert(pack_.n_records == ioQ_.size());
        size_t total = 0;
        while (!ioQ_.empty()) {
            DiffIo io0 = std::move(ioQ_.front());
            ioQ_.pop();
            if (io0.empty()) continue;
            io0.writeTo(fileW_);
            total += io0.getSize();
        }
        assert(total == pack_.total_size);
        pack_.reset();
    }
    void writeEof() {
        writeDiffEofPack(fileW_);
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
class DiffReader
{
private:
    cybozu::util::File fileR_;
    bool isReadHeader_;

    /* Buffers. */
    char buf_[WALB_DIFF_PACK_SIZE];
    DiffPackHeader &pack_;
    uint16_t recIdx_;
    uint32_t totalSize_;

    DiffStatistics stat_;

public:
    DiffReader() : pack_(*(DiffPackHeader *)buf_) {
        init();
    }
    explicit DiffReader(int fd) : DiffReader() {
        fileR_.setFd(fd);
    }
    // flags will be deprecated.
    explicit DiffReader(const std::string &diffPath, int flags = O_RDONLY) : DiffReader() {
        fileR_.open(diffPath, flags);
    }
    explicit DiffReader(cybozu::util::File &&fileR) : DiffReader() {
        fileR_ = std::move(fileR);
    }
    ~DiffReader() noexcept try {
        close();
    } catch (...) {
    }

    void close() {
        fileR_.close();
        pack_.setEnd();
    }
    void open(const std::string &diffPath) {
        close();
        init();
        fileR_.open(diffPath, O_RDONLY);
    }
    void setFd(int fd) {
        close();
        init();
        fileR_.setFd(fd);
    }

    /**
     * Read header data.
     * You must call this at first.
     */
    void readHeader(DiffFileHeader &head, bool doReadPackHeader = true) {
        if (isReadHeader_) {
            throw RT_ERR("Do not call readHeader() more than once.");
        }
        head.readFrom(fileR_);
        isReadHeader_ = true;
        if (doReadPackHeader) readPackHeader();
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
        if (!prepareRead()) return false;
        assert(pack_.n_records == 0 || recIdx_ < pack_.n_records);
        rec = pack_[recIdx_];

        if (!rec.isValid()) {
            throw cybozu::Exception(__func__)
                << "invalid record" << fileR_.fd() << recIdx_ << rec;
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
    bool prepareRead() {
        if (pack_.isEnd()) return false;
        bool ret = true;
        if (recIdx_ == pack_.n_records) {
            ret = readPackHeader();
        }
        return ret;
    }
    /**
     * Read a diff IO.
     * @rec diff record.
     * @io block IO to be filled.
     */
    void readDiffIo(const DiffRecord &rec, DiffIo &io) {
        if (rec.data_offset != totalSize_) {
            throw cybozu::Exception(__func__)
                << "data offset invalid" << rec.data_offset << totalSize_;
        }
        io.setAndReadFrom(rec, fileR_);
        totalSize_ += rec.data_size;
        recIdx_++;
    }
    const DiffStatistics& getStat() const {
        return stat_;
    }
private:
    bool readPackHeader() {
        try {
            pack_.readFrom(fileR_);
        } catch (cybozu::util::EofError &e) {
            pack_.setEnd();
            return false;
        }
        if (pack_.isEnd()) return false;
        recIdx_ = 0;
        totalSize_ = 0;
        stat_.update(pack_);
        return true;
    }
    void init() {
        pack_.reset();
        isReadHeader_ = false;
        recIdx_ = 0;
        totalSize_ = 0;
        stat_.clear();
        stat_.wdiffNr = 1;
    }
};

} //namespace walb
