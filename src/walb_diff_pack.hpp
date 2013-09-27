#pragma once
/**
 * @file
 * @brief walb diff utiltities for packs.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "walb_diff_base.hpp"
#include <exception>

namespace walb {
namespace diff {

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
 * Generator of a pack as a memory image.
 */
class Packer
{
private:
    std::vector<char> data_;
    diff::PackHeader packh;

public:
    Packer() : data_(::WALB_DIFF_PACK_SIZE) , packh(&data_[0]) {
        packh.reset();
    }

    void setMaxNumRecords(size_t value) { packh.setMaxNumRecords(value); }
    void setMaxPackSize(size_t value) { packh.setMaxPackSize(value); }

    /**
     * You must care about IO insertion order and overlap.
     *
     * RETURN:
     *   false: failed. You need to create another pack.
     */
    bool add(uint64_t ioAddr, uint16_t ioBlocks, const std::vector<char> &data) {
        assert(ioBlocks != 0);
        uint32_t dataSize = ioBlocks * LOGICAL_BLOCK_SIZE;
        assert(data.size() == dataSize);
        if (!packh.canAdd(dataSize)) return false;

        bool isZero = isAllZero(data);
        diff::RecordRaw rec;
        rec.setIoAddress(ioAddr);
        rec.setIoBlocks(ioBlocks);
        rec.setCompressionType(::WALB_DIFF_CMPR_NONE);
        if (isZero) {
            rec.setAllZero();
            rec.setDataSize(0);
            rec.setChecksum(0);
        } else {
            rec.setNormal();
            rec.setDataSize(dataSize);
            rec.setChecksum(cybozu::util::calcChecksum(&data[0], data.size(), 0));
        }
        /* r must be true because we called canAdd() before. */
        bool r = packh.add(*rec.rawRecord());
        if (r && !isZero) extendAndCopy(data);
        return r;
    }
    /**
     * Get created pack image.
     */
    std::vector<char> getPack() {
        packh.updateChecksum();
        std::vector<char> ret = std::move(data_);
        data_.resize(::WALB_DIFF_PACK_SIZE);
        packh.resetBuffer(&data_[0]);
        packh.reset();
        return ret;
    }
private:
    static bool isAllZero(const std::vector<char> &data) {
        for (char c : data) {
            if (c != 0) return false;
        }
        return true;
    }
    void extendAndCopy(const std::vector<char> &data) {
        assert(!data.empty());
        size_t s0 = data_.size();
        size_t s1 = data.size();
        data_.resize(s0 + s1);
        ::memcpy(&data_[s0], &data[0], s1);
        packh.resetBuffer(&data_[0]);
    }
};

}} //namespace walb::diff
