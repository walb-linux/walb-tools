#pragma once
/**
 * @file
 * @brief walb diff utiltities for files.
 */
#include <unordered_map>
#include "walb_diff_pack.hpp"
#include "walb_diff_stat.hpp"
#include "uuid.hpp"
#include "mmap_file.hpp"
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
    bool verify(bool throwError = true) const;

    void updateChecksum() {
        checksum = 0;
        checksum = cybozu::util::calcChecksum(this, getSize(), 0);
    }

    void setUuid(const cybozu::Uuid& uuid) {
        uuid.copyTo(this->uuid);
    }

    std::string str() const;
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
        type = WALB_DIFF_TYPE_SORTED; // default.
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
void writeDiffFileHeader(Writer& writer, uint32_t maxIoBlocks, const cybozu::Uuid &uuid)
{
    DiffFileHeader fileH;
    fileH.setMaxIoBlocksIfNecessary(maxIoBlocks);
    fileH.setUuid(uuid);
    fileH.writeTo(writer);
}

template<class Writer>
void writeDiffEofPack(Writer& writer)
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

    void close();

    /**
     * Write header data.
     * You must call this at first.
     */
    void writeHeader(DiffFileHeader &header);

    /**
     * Write a diff data.
     *
     * rec.checksum must be set correctly before calling this function.
     *
     * @rec diff record
     * @io IO data.
     *    if rec is not normal, io must be empty.
     */
    void writeDiff(const DiffRecord &rec, DiffIo &&io);
    void writeDiff(const DiffRecord &rec, const char *data);

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
                              int type = ::WALB_DIFF_CMPR_SNAPPY, int level = 0);

    const DiffStatistics& getStat() const {
        return stat_;
    }
private:
    void init();
    bool addAndPush(const DiffRecord &rec, DiffIo &&io);
    static void assertRecAndIo(const DiffRecord &rec, const DiffIo &io);

    /**
     * Write the buffered pack and its related diff ios.
     */
    void writePack();

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
    constexpr static const char *NAME = "DiffReader";
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
    void readHeader(DiffFileHeader &head, bool doReadPackHeader = true);
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
    bool readDiff(DiffRecord &rec, DiffIo &io);
    /**
     * Read a diff IO and uncompress it.
     *
     * RETURN:
     *   false if the input stream reached the end.
     */
    bool readAndUncompressDiff(DiffRecord &rec, DiffIo &io, bool calcChecksum = true);

    bool prepareRead();
    /**
     * Read a diff IO.
     * @rec diff record.
     * @io block IO to be filled.
     */
    void readDiffIo(const DiffRecord &rec, DiffIo &io);

    const DiffStatistics& getStat() const {
        return stat_;
    }
private:
    bool readPackHeader();
    void init();
};


class DiffIndexMem
{
private:
    std::map<uint64_t, DiffIndexRecord> index_; // key: io_address.

    void addDetail(const DiffIndexRecord &rec);
public:
    void add(const DiffIndexRecord &rec) {
        for (const DiffIndexRecord& r : rec.split()) {
            addDetail(r);
        }
    }
    void clear() {
        index_.clear();
    }
    template<class Writer>
    void writeTo(Writer& writer) const {
        for (const std::pair<uint64_t, DiffIndexRecord>& pair : index_) {
            const DiffIndexRecord& rec = pair.second;
            writer.write(&rec, sizeof(rec));
        }
    }
    size_t size() const { return index_.size(); }

    /**
     * for debug and test.
     */
    void checkNoOverlappedAndSorted() const;
};


/**
 * QQQ update stat_.
 */
class IndexedDiffWriter /* final */
{
private:
    cybozu::util::File fileW_;
    bool isWrittenHeader_;
    bool isClosed_;
    uint64_t offset_;
    uint64_t n_data_;
    DiffIndexMem indexMem_;
    DiffStatistics stat_;
    AlignedArray buf_;

public:
    IndexedDiffWriter() {
        init();
    }
    ~IndexedDiffWriter() {
        finalize();
    }
    void setFd(int fd) {
        init();
        fileW_.setFd(fd);
    }
    void open(const std::string& diffPath, int flags, mode_t mode) {
        init();
        fileW_.open(diffPath, flags, mode);
    }
    void finalize();
    void writeHeader(DiffFileHeader &header);
    void writeDiff(const DiffIndexRecord &rec, const char *data);
    void compressAndWriteDiff(const DiffIndexRecord &rec, const char *data,
                              int type = ::WALB_DIFF_CMPR_SNAPPY, int level = 0);

    const DiffStatistics& getStat() const {
        return stat_;
    }

    /**
     * for debug and test.
     */
    void checkNoOverlappedAndSorted() const {
        indexMem_.checkNoOverlappedAndSorted();
    }

    static constexpr const char *NAME = "IndexedDiffWriter";

private:
    void init();
    void writeSuper();
    void checkWrittenHeader() const {
        if (!isWrittenHeader_) {
            throw cybozu::Exception(NAME) <<
                "checkWrittenHeader: call writeHeader() before writeDiff().";
        }
    }
};


class IndexedDiffCache /* final */
{
private:
    struct Item {
        uint64_t key;
        std::unique_ptr<AlignedArray> dataPtr;
    };

    using ListIt = std::list<Item>::iterator;

    size_t maxBytes_;
    size_t curBytes_;
    std::list<Item> list_;
    std::unordered_map<uint64_t, ListIt> map_;

public:
    IndexedDiffCache() : maxBytes_(0), curBytes_(0), list_(), map_() {}
    void setMaxSize(size_t bytes) { maxBytes_ = bytes; }
    AlignedArray* find(uint64_t key);
    void add(uint64_t key, std::unique_ptr<AlignedArray> &&dataPtr);
private:
    void evictOne();
};


/**
 * This use random access.
 */
class IndexedDiffReader /* final */
{
private:
    cybozu::util::MmappedFile memFile_;

    DiffFileHeader header_;
    size_t idxBgnOffset_;
    size_t idxEndOffset_;
    size_t idxOffset_;

    IndexedDiffCache cache_;

public:
    constexpr static const char *NAME = "IndexedDiffReader";
    IndexedDiffReader() { cache_.setMaxSize(32 * MEBI); } // QQQ
    void setFile(cybozu::util::File &&fileR);
    const DiffFileHeader& header() const { return header_; }
    /**
     * data will be uncompressed data.
     */
    bool readDiff(DiffIndexRecord &rec, AlignedArray &data);
private:
    bool getNextRec(DiffIndexRecord& rec);
};


} //namespace walb
