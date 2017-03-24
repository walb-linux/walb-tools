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
    constexpr static const char *NAME = "DiffFileHeader";
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

    bool isIndexed() const;
    std::string typeStr() const;

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
    void setFile(cybozu::util::File &&fileR) {
        close();
        init();
        fileR_ = std::move(fileR);
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
     * If other code read the header before using this class,
     * call this function to set header data.
     * The file position must be just after the header to continue reading.
     */
    void dontReadHeader(bool doReadPackHeader = true) {
        isReadHeader_ = true;
        if (doReadPackHeader) readPackHeader();
    }

    /**
     * Read a diff IO.
     *
     * RETURN:
     *   false if the input stream reached the end,
     *   or the record/data is invalid (only when throwError is false).
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
    void readDiffIo(const DiffRecord &rec, DiffIo &io, bool verifyChecksum = true);

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
    std::map<uint64_t, IndexedDiffRecord> index_; // key: io_address.

    void addDetail(const IndexedDiffRecord &rec);
public:
    void add(const IndexedDiffRecord &rec) {
        for (const IndexedDiffRecord& r : rec.split()) {
            addDetail(r);
        }
    }
    void clear() {
        index_.clear();
    }
    template<class Writer>
    void writeTo(Writer& writer, DiffStatistics *stat = nullptr) const {
        for (const std::pair<uint64_t, IndexedDiffRecord>& pair : index_) {
            const IndexedDiffRecord& rec = pair.second;
            writer.write(&rec, sizeof(rec));
            if (stat) stat->update(rec);
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
    void writeDiff(const IndexedDiffRecord &rec, const char *data);
    void compressAndWriteDiff(const IndexedDiffRecord &rec, const char *data,
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
public:
    struct Key {
        const void *tag;
        uint64_t addr;

        friend inline std::ostream& operator<<(std::ostream& os, const Key& key) {
            os << "(" << key.tag << "," << key.addr << ")";
            return os;
        }
    };
private:
    struct HashKey {
        size_t operator()(Key key) const {
            size_t h0 = std::hash<uintptr_t>()(uintptr_t(key.tag));
            size_t h1 = std::hash<uint64_t>()(key.addr);
            // like boost::hash_combine().
            return h0 ^ (h1 + 0x9e3779b9 + (h0 << 6) + (h0 >> 2));
        }
    };
    struct EqualKey {
        bool operator()(Key lhs, Key rhs) const {
            return lhs.tag == rhs.tag && lhs.addr == rhs.addr;
        }
    };
    struct Item {
        Key key;
        std::unique_ptr<AlignedArray> dataPtr;
    };

    using ListIt = std::list<Item>::iterator;

    size_t maxBytes_;
    size_t curBytes_;
    std::list<Item> lruList_;
    std::unordered_map<Key, ListIt, HashKey, EqualKey> map_;

public:
    IndexedDiffCache() : maxBytes_(0), curBytes_(0), lruList_(), map_() {}
    void setMaxSize(size_t bytes) { maxBytes_ = bytes; }
    AlignedArray* find(Key key);
    void add(Key key, std::unique_ptr<AlignedArray> &&dataPtr);
    void clear();
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

    IndexedDiffCache *cache_;
    DiffStatistics stat_;

public:
    constexpr static const char *NAME = "IndexedDiffReader";
    IndexedDiffReader()
        : memFile_(), header_(), idxBgnOffset_(), idxEndOffset_()
        , idxOffset_(), cache_(nullptr), stat_() {}
    void setFile(cybozu::util::File &&fileR, IndexedDiffCache &cache);
    const DiffFileHeader& header() const { return header_; }

    bool readDiffRecord(IndexedDiffRecord &rec, bool doVerify = true);
    /**
     * data will be uncompressed data.
     */
    void readDiffIo(const IndexedDiffRecord &rec, AlignedArray &data);
    bool readDiff(IndexedDiffRecord &rec, AlignedArray &data) {
        if (!readDiffRecord(rec)) return false;
        readDiffIo(rec, data);
        return true;
    }
    const DiffStatistics& getStat() const { return stat_; }
    void close() { memFile_.reset(); }

    /*
     * isOnCache() and loadToCache() are special interface for wdiff-show command.
     */
    bool isOnCache(const IndexedDiffRecord &rec) const;
    bool loadToCache(const IndexedDiffRecord &rec, bool throwError = true);
private:
    bool getNextRec(IndexedDiffRecord& rec);
    bool verifyIoData(uint64_t offset, uint32_t size, bool throwError) const;
};


} //namespace walb
