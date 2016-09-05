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

union ExtendedDiffPackHeader {
    DiffPackHeader header;
    char buf[WALB_DIFF_PACK_SIZE];
};

/**
 * Walb diff header data.
 */
struct DiffFileHeader : walb_diff_file_header
{
    DiffFileHeader() {
        init();
    }
    uint32_t getChecksum() const { return checksum; }
    uint32_t getMaxIoBlocks() const { return max_io_blocks; }
    cybozu::Uuid getUuid() const { return cybozu::Uuid(&uuid[0]); }

    void setMaxIoBlocksIfNecessary(uint32_t ioBlocks) {
        if (max_io_blocks < ioBlocks) {
            max_io_blocks = ioBlocks;
        }
    }

    void resetMaxIoBlocks() { max_io_blocks = 0; }

    size_t getSize() const { return sizeof(walb_diff_file_header); }

    bool isValid() const { return verify(false); }
    bool verify(bool throwError = true) const {
        if (cybozu::util::calcChecksum(this, getSize(), 0) != 0) {
            if (throwError) {
                throw cybozu::Exception(__func__) << "invalid checksum";
            }
            return false;
        }
        if (version != WALB_DIFF_VERSION) {
            if (throwError) {
                throw cybozu::Exception(__func__)
                    << "invalid walb diff version" << version << WALB_DIFF_VERSION;
            }
            return false;
        }
        return true;
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
            "  version: %u\n"
            "  maxIoBlocks: %u\n"
            "  uuid: %s\n"
            , checksum, version, max_io_blocks, getUuid().str().c_str());
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
        version = WALB_DIFF_VERSION;
    }
    template<class Writer>
    void writeTo(Writer& writer) {
        updateChecksum();
        writer.write(this, getSize());
    }
    template<class Reader>
    void readFrom(Reader& reader) {
        reader.read(this, getSize());
        verify();
    }
};

template <class Writer>
inline void writeDiffFileHeader(Writer& writer, uint32_t maxIoBlocks, const cybozu::Uuid &uuid)
{
    DiffFileHeader fileH;
    fileH.setMaxIoBlocksIfNecessary(maxIoBlocks);
    fileH.setUuid(uuid);
    fileH.writeTo(writer);
}

template<class Writer>
inline void writeDiffEofPack(Writer& writer)
{
    ExtendedDiffPackHeader edp;
    DiffPackHeader &pack = edp.header;
    pack.clear();
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
    ExtendedDiffPackHeader edp_;
    DiffPackHeader &pack_;

    std::queue<DiffIo> ioQ_;

    DiffStatistics stat_;

public:
    DiffWriter() : pack_(edp_.header) {
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
            writePack(); // if buffered data exist.
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
     * rec.checksum must be set correctly before calling this function.
     *
     * @rec diff record
     * @io IO data.
     *    if rec is not normal, io must be empty.
     */
    void writeDiff(const DiffRecord &rec, DiffIo &&io) {
        checkWrittenHeader();
        assertRecAndIo(rec, io);

        if (addAndPush(rec, std::move(io))) return;
        writePack();
        const bool ret = addAndPush(rec, std::move(io));
        unusedVar(ret);
        assert(ret);
    }
    void writeDiff(const DiffRecord &rec, const char *data) {
        DiffIo io;
        io.set(rec);
        if (rec.isNormal()) {
            ::memcpy(io.get(), data, rec.data_size);
        }
        writeDiff(rec, std::move(io));
    }
    /**
     * Compress and write a diff data.
     * Do not use this for already compressed IOs. It works but inefficient.
     *
     * rec.checksum need not be set correctly before calling this function.
     *
     * @rec record.
     * @data IO data.
     */
    void compressAndWriteDiff(const DiffRecord &rec, const char *data,
                              int type = ::WALB_DIFF_CMPR_SNAPPY, int level = 0) {
        if (rec.isCompressed()) {
            writeDiff(rec, data);
            return;
        }
        if (!rec.isNormal()) {
            writeDiff(rec, DiffIo());
            return;
        }
        DiffRecord compRec;
        DiffIo compIo;
        compressDiffIo(rec, data, compRec, compIo.data, type, level);
        compIo.set(compRec);
        writeDiff(compRec, std::move(compIo));
    }
    const DiffStatistics& getStat() const {
        return stat_;
    }
private:
    void init() {
        isWrittenHeader_ = false;
        isClosed_ = false;
        pack_.clear();
        while (!ioQ_.empty()) ioQ_.pop();
        stat_.clear();
        stat_.wdiffNr = 1;
    }
    bool addAndPush(const DiffRecord &rec, DiffIo &&io) {
        if (pack_.add(rec)) {
            ioQ_.push(std::move(io));
            return true;
        }
        return false;
    }
#ifdef DEBUG
    static void assertRecAndIo(const DiffRecord &rec, const DiffIo &io) {
        if (rec.isNormal()) {
            assert(!io.empty());
            assert(io.compressionType == rec.compression_type);
            assert(rec.checksum == io.calcChecksum());
        } else {
            assert(io.empty());
        }
    }
#else
    static void assertRecAndIo(const DiffRecord &, const DiffIo &) {}
#endif
    /**
     * Write the buffered pack and its related diff ios.
     */
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
        pack_.clear();
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
    ExtendedDiffPackHeader edp_;
    DiffPackHeader &pack_;
    uint16_t recIdx_;
    uint32_t totalSize_;

    DiffStatistics stat_;

public:
    DiffReader() : pack_(edp_.header) {
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
    bool readAndUncompressDiff(DiffRecord &rec, DiffIo &io, bool calcChecksum = true) {
        if (!readDiff(rec, io)) {
            return false;
        }
        if (!rec.isNormal() || !rec.isCompressed()) {
            return true;
        }
        DiffRecord outRec;
        DiffIo outIo;
        uncompressDiffIo(rec, io.get(), outRec, outIo.data, calcChecksum);
        outIo.set(outRec);
        rec = outRec;
        io = std::move(outIo);
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
        pack_.clear();
        isReadHeader_ = false;
        recIdx_ = 0;
        totalSize_ = 0;
        stat_.clear();
        stat_.wdiffNr = 1;
    }
};

} //namespace walb
