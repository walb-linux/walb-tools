#pragma once
/**
 * @file
 * @brief walb diff utiltities for files.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "walb_diff_base.hpp"
#include <exception>

namespace walb {
namespace diff {

/**
 * Walb diff header data.
 */
class FileHeaderRef
{
private:
    struct walb_diff_file_header &h_;

public:
    FileHeaderRef(struct walb_diff_file_header &h) : h_(h) {}
    virtual ~FileHeaderRef() noexcept = default;

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
class FileHeaderRaw
    : public FileHeaderRef
{
private:
    struct walb_diff_file_header header_;

public:
    FileHeaderRaw()
        : FileHeaderRef(header_), header_() {}
    ~FileHeaderRaw() noexcept = default;
};

/**
 * Walb diff pack wrapper.
 * This can be a wrapper or a manager of 4KiB memory image.
 */
class PackHeader /* final */
{
private:
    char *buf_;
    bool mustDelete_;
    size_t maxNumRecords_;
    size_t maxPackSize_; /* [byte] */
public:
    PackHeader()
        : buf_(allocStatic()), mustDelete_(true)
        , maxNumRecords_(::MAX_N_RECORDS_IN_WALB_DIFF_PACK)
        , maxPackSize_(::WALB_DIFF_PACK_MAX_SIZE) {
        reset();
    }
    /**
     * Buffer size must be ::WALB_DIFF_PACK_SIZE.
     * You must call reset() to initialize.
     */
    explicit PackHeader(char *buf)
        : buf_(buf), mustDelete_(false)
        , maxNumRecords_(::MAX_N_RECORDS_IN_WALB_DIFF_PACK)
        , maxPackSize_(::WALB_DIFF_PACK_MAX_SIZE) {
        assert(buf);
    }
    PackHeader(const PackHeader &) = delete;
    PackHeader(PackHeader &&rhs)
        : buf_(nullptr), mustDelete_(false) {
        *this = std::move(rhs);
    }
    ~PackHeader() noexcept {
        if (mustDelete_) ::free(buf_);
    }
    PackHeader &operator=(const PackHeader &) = delete;
    PackHeader &operator=(PackHeader &&rhs) {
        if (mustDelete_) {
            ::free(buf_);
            buf_ = nullptr;
        }
        buf_ = rhs.buf_;
        mustDelete_ = rhs.mustDelete_;
        rhs.buf_ = nullptr;
        rhs.mustDelete_ = false;
        maxNumRecords_ = rhs.maxNumRecords_;
        maxPackSize_ = rhs.maxPackSize_;
        return *this;
    }

    void resetBuffer(char *buf) {
        assert(buf);
        if (mustDelete_) ::free(buf_);
        mustDelete_ = false;
        buf_ = buf;
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
    uint32_t uncompressedTotalSize() const {
        uint32_t total = 0;
        for (uint16_t i = 0; i < nRecords(); i++) {
            total += record(i).io_blocks;
        }
        return total * LOGICAL_BLOCK_SIZE;
    }

    bool isEnd() const {
        const uint8_t mask = 1U << WALB_DIFF_PACK_END;
        return (header().flags & mask) != 0;
    }

    void setEnd() {
        const uint8_t mask = 1U << WALB_DIFF_PACK_END;
        header().flags |= mask;
    }

    bool canAdd(uint32_t dataSize) const {
        uint16_t nRec = header().n_records;
        if (maxNumRecords_ <= nRec) {
            return false;
        }
        if (0 < nRec && maxPackSize_ < header().total_size + dataSize) {
            return false;
        }
        return true;
    }

    /**
     * RETURN:
     *   true when added successfully.
     *   false when pack is full.
     */
    bool add(const struct walb_diff_record &inRec) {
#ifdef DEBUG
        RecordRaw r(reinterpret_cast<const char *>(&inRec), sizeof(inRec));
        assert(r.isValid());
#endif
        if (!canAdd(inRec.data_size)) { return false; }
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

    void print(::FILE *fp = ::stdout) const {
        const struct walb_diff_pack &h = header();
        ::fprintf(fp, "checksum %u\n"
                  "n_records: %u\n"
                  "total_size: %u\n"
                  , h.checksum
                  , h.n_records
                  , h.total_size);
        for (size_t i = 0; i < h.n_records; i++) {
            ::fprintf(fp, "record %zu: ", i);
            RecordRaw rec(reinterpret_cast<const char *>(&record(i)), sizeof(record(i)));
            rec.printOneline(fp);
        }
    }

    void setMaxNumRecords(size_t value) {
        maxNumRecords_ = std::min(value, ::MAX_N_RECORDS_IN_WALB_DIFF_PACK);
    }
    void setMaxPackSize(size_t value) {
        maxPackSize_ = std::min(value, ::WALB_DIFF_PACK_MAX_SIZE);
    }
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
};

/**
 * Pack interface.
 */
class PackBase
{
public:
    virtual ~PackBase() noexcept {}
    virtual const struct walb_diff_pack &header() const = 0;
    virtual struct walb_diff_pack &header() = 0;
    virtual const char *data(size_t i) const = 0;
    virtual char *data(size_t i) = 0;
    size_t size() const {
        return ::WALB_DIFF_PACK_SIZE + header().total_size;
    }
    bool isValid(std::string *errMsg = nullptr) const {
        auto err = [&errMsg](const char *fmt, ...) {
            if (!errMsg) throw std::exception();
            std::string msg;
            va_list args;
            va_start(args, fmt);
            try { msg = cybozu::util::formatStringV(fmt, args); } catch (...) {}
            va_end(args);
            *errMsg = msg;
            throw std::exception();
        };
        try {
            PackHeader packh((char *)&header());
            if (!packh.isValid()) err("pack header invalid.");
            for (size_t i = 0; i < packh.nRecords(); i++) {
                walb::diff::RecordRaw rec(packh.record(i));
                if (!rec.isValid()) err("record invalid: %zu.", i);
                uint32_t csum = cybozu::util::calcChecksum(data(i), rec.dataSize(), 0);
                if (csum != rec.checksum()) err("checksum of %zu differ %08x %08x."
                                                , i, rec.checksum(), csum);
            }
            return true;
        } catch (std::exception &) {
            return false;
        }
    }
};

/**
 * Manage a pack as a contiguous memory.
 */
class MemoryPack : public PackBase
{
private:
    std::unique_ptr<char[]> p_;

public:
    /* Copy data */
    explicit MemoryPack(const void *p) : p_() {
        const struct walb_diff_pack &packh = *reinterpret_cast<const struct walb_diff_pack *>(p);
        size_t size = WALB_DIFF_PACK_SIZE + packh.total_size;
        p_ = std::unique_ptr<char[]>(new char[size]);
        ::memcpy(p_.get(), p, size);
    }
    /* Move data */
    explicit MemoryPack(std::unique_ptr<char[]> &&p)
        : p_(std::move(p)) {}
    MemoryPack(const MemoryPack &rhs) : p_() {
        p_ = std::unique_ptr<char[]>(new char[rhs.size()]);
        ::memcpy(p_.get(), rhs.p_.get(), rhs.size());
    }
    MemoryPack(MemoryPack &&rhs) : p_(std::move(rhs.p_)) {}
    MemoryPack &operator=(const MemoryPack &rhs) {
        MemoryPack pack(rhs);
        p_ = std::move(pack.p_);
        return *this;
    }
    MemoryPack &operator=(MemoryPack &&rhs) {
        p_ = std::move(rhs.p_);
        return *this;
    }

    const struct walb_diff_pack &header() const override {
        return *ptr<struct walb_diff_pack>(0);
    }
    struct walb_diff_pack &header() override {
        return *ptr<struct walb_diff_pack>(0);
    }
    const char *data(size_t i) const override {
        assert(i < header().n_records);
        if (header().record[i].data_size == 0) return nullptr;
        return ptr<const char>(offset(i));
    }
    char *data(size_t i) override {
        assert(i < header().n_records);
        if (header().record[i].data_size == 0) return nullptr;
        return ptr<char>(offset(i));
    }
    const char *rawPtr() const {
        return p_.get();
    }
    std::unique_ptr<char[]> &&forMove() {
        return std::move(p_);
    }
private:
    template <typename T>
    const T *ptr(size_t off) const {
        return reinterpret_cast<const T *>(&p_[off]);
    }
    template <typename T>
    T *ptr(size_t off) {
        return reinterpret_cast<T *>(&p_[off]);
    }
    size_t offset(size_t i) const {
        return ::WALB_DIFF_PACK_SIZE + header().record[i].data_offset;
    }
};

/**
 * Manage a pack as a header data and multiple block diff IO data.
 */
class ScatterGatherPack : public PackBase
{
private:
    PackHeader pack_; /* pack header */
    std::vector<IoData> ios_;

public:
    /**
     * ios_[i] must be nullptr if pack_.record(i).data_size == 0.
     */
    ScatterGatherPack(PackHeader &&pack, std::vector<IoData> &&ios)
        : pack_(std::move(pack)), ios_(std::move(ios)) {
        assert(header().n_records == ios_.size());
    }

    const struct walb_diff_pack &header() const override {
        return pack_.header();
    }
    struct walb_diff_pack &header() override {
        return pack_.header();
    }
    /**
     * RETURN:
     *   data pointer for normal IOs.
     *   nullptr for non-normal IOs such as ALL_ZERO and DISCARD.
     */
    const char *data(size_t i) const override {
        return ios_[i].rawData();
    }
    char *data(size_t i) override {
        return ios_[i].rawData();
    }
    /**
     * Generate memory pack.
     */
    MemoryPack generateMemoryPack() const {
        assert(isValid());
        std::unique_ptr<char[]> up(new char[size()]);
        char *p = up.get();
        /* Copy pack header. */
        ::memcpy(p, pack_.rawData(), pack_.rawSize());
        p += pack_.rawSize();
        assert(pack_.nRecords() == ios_.size());
        uint32_t dataOffset = 0;
        for (size_t i = 0; i < pack_.nRecords(); i++) {
            /* Copy each IO data. */
            RecordWrapConst rec(&pack_.record(i));
            uint32_t dataSize = rec.dataSize();
            assert(rec.dataOffset() == dataOffset);
            if (rec.isNormal()) {
                assert(0 < dataSize);
                ::memcpy(p, data(i), dataSize);
                p += dataSize;
                dataOffset += dataSize;
            }
        }
        assert(pack_.rawSize() + dataOffset == size());
        MemoryPack mpack(std::move(up));
        assert(mpack.isValid());
        return mpack;
    }
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
    std::queue<std::shared_ptr<IoData> > ioPtrQ_;

public:
    explicit Writer(int fd)
        : opener_(), fd_(fd), fdw_(fd)
        , isWrittenHeader_(false)
        , isClosed_(false)
        , pack_()
        , ioPtrQ_() {}

    explicit Writer(const std::string &diffPath, int flags, mode_t mode)
        : opener_(new cybozu::util::FileOpener(diffPath, flags, mode))
        , fd_(opener_->fd())
        , fdw_(fd_)
        , isWrittenHeader_(false)
        , isClosed_(false)
        , pack_()
        , ioPtrQ_() {
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
    void writeHeader(FileHeaderRef &header) {
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
    void writeDiff(const RecordRaw &rec, const std::shared_ptr<IoData> &iop) {
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
    void compressAndWriteDiff(const RecordRaw &rec, const IoData &io) {
        assert(rec.isValid());
        assert(io.isValid());
        assert(rec.compressionType() == io.compressionType());
        assert(rec.dataSize() == io.rawSize());
        if (rec.isNormal()) {
            assert(rec.ioBlocks() == io.ioBlocks());
            assert(rec.checksum() == io.calcChecksum());
        }

        auto iop = std::make_shared<IoData>();
        if (!rec.isNormal()) {
            writeDiff(rec, iop);
            return;
        }

        RecordRaw rec0(rec);
        if (rec.isCompressed()) {
            /* copy */
            *iop = io;
        } else {
            *iop = compressIoData(io, ::WALB_DIFF_CMPR_SNAPPY);
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

        assert(pack_.nRecords() == ioPtrQ_.size());
        while (!ioPtrQ_.empty()) {
            std::shared_ptr<IoData> iop0 = ioPtrQ_.front();
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
    std::shared_ptr<FileHeaderRef> readHeader() {
        auto p = std::make_shared<FileHeaderRaw>();
        readHeader(*p);
        return p;
    }

    /**
     * Read header data with another interface.
     */
    void readHeader(FileHeaderRef &head) {
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
    bool readDiff(RecordRaw &rec, IoData &io) {
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
    bool readAndUncompressDiff(RecordRaw &rec, IoData &io) {
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
    void readDiffIo(const RecordRaw &rec, IoData &io) {
        if (rec.dataOffset() != totalSize_) {
            throw RT_ERR("data offset invalid %u %u.", rec.dataOffset(), totalSize_);
        }
        if (0 < rec.dataSize()) {
            io.resizeData(rec.dataSize());
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
