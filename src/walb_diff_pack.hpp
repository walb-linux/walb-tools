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
    uint32_t wholePackSize() const { return WALB_DIFF_PACK_SIZE + totalSize(); }
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
        assert(isValidRec(inRec));
#endif
        if (!canAdd(inRec.data_size)) { return false; }
        walb_diff_record &outRec = record(header().n_records);
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
            const walb_diff_record& rec = record(i);
            printOnelineRec(rec, fp);
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
    virtual const char *data(size_t i) const = 0;
    size_t size() const {
        return ::WALB_DIFF_PACK_SIZE + header().total_size;
    }
    bool isValid(std::string *errMsg = nullptr) const {
        return isValid(true, errMsg);
    }
    bool isValid(bool checkHeaderCsum, std::string *errMsg = nullptr) const try {
        std::string err;
        PackHeader packh((char *)&header());
        if (checkHeaderCsum && !packh.isValid()) {
            err = "pack header invalid.";
        } else {
            for (size_t i = 0; i < packh.nRecords(); i++) {
                const walb_diff_record& rec = packh.record(i);
                if (!isValidRec(rec)) {
                    err = cybozu::util::formatString("record invalid: %zu.", i);
                    goto err_exit;
                }
                uint32_t csum = cybozu::util::calcChecksum(data(i), rec.data_size, 0);
                if (csum != rec.checksum) {
                    err = cybozu::util::formatString("checksum of %zu differ %08x %08x."
                                                , i, rec.checksum, csum);
                    goto err_exit;
                }
            }
            return true;
        }
    err_exit:
        if (errMsg) *errMsg = std::move(err);
        return false;
    } catch (std::exception &e) {
        if (errMsg) *errMsg = e.what();
        return false;
    }
};

/**
 * Manage a pack as a contiguous memory.
 */
class MemoryPack : public PackBase
{
private:
    const char *p_;

public:
    explicit MemoryPack(const char *p) : p_() {
        reset(p);
    }
    MemoryPack(const MemoryPack &rhs) : p_(rhs.p_) {}
    MemoryPack(MemoryPack &&) = delete;
    MemoryPack &operator=(const MemoryPack &rhs) {
        reset(rhs.p_);
        return *this;
    }
    MemoryPack &operator=(MemoryPack &&) = delete;

    const struct walb_diff_pack &header() const override {
        return *ptr<struct walb_diff_pack>(0);
    }
    const char *data(size_t i) const override {
        assert(i < header().n_records);
        if (header().record[i].data_size == 0) return nullptr;
        return ptr<const char>(offset(i));
    }
    const char *rawPtr() const {
        return p_;
    }
    void reset(const char *p) {
        if (!p) throw std::runtime_error("The pointer must not be null.");
        p_ = p;
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
    ScatterGatherPack(const ScatterGatherPack &) = delete;
    ScatterGatherPack(ScatterGatherPack &&) = default;
    ScatterGatherPack &operator=(const ScatterGatherPack &) = delete;
    ScatterGatherPack &operator=(ScatterGatherPack &&) = default;

    const struct walb_diff_pack &header() const override {
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
    /**
     * Generate memory pack.
     */
    std::unique_ptr<char[]> generateMemoryPack() const {
        assert(isValid());
        std::unique_ptr<char[]> up(new char[size()]);
        copyTo(up.get());
        MemoryPack mpack(up.get());
        assert(mpack.isValid());
        return up;
    }
    /**
     * @p a pointer that has at least size() bytes space.
     */
    void copyTo(char *p) const {
        /* Copy pack header. */
        ::memcpy(p, pack_.rawData(), pack_.rawSize());
        p += pack_.rawSize();
        assert(pack_.nRecords() == ios_.size());
        uint32_t dataOffset = 0;
        for (size_t i = 0; i < pack_.nRecords(); i++) {
            /* Copy each IO data. */
            const walb_diff_record& rec = pack_.record(i);
            uint32_t dataSize = rec.data_size;
            assert(rec.data_offset == dataOffset);
            if (isNormalRec(rec)) {
                assert(0 < dataSize);
                ::memcpy(p, data(i), dataSize);
                p += dataSize;
                dataOffset += dataSize;
            }
        }
        assert(pack_.rawSize() + dataOffset == size());
    }
};

/**
 * Generator of a pack as a memory image.
 */
class Packer
{
private:
    std::vector<char> data_;
    diff::PackHeader packh_;

public:
    Packer() : data_(::WALB_DIFF_PACK_SIZE) , packh_(&data_[0]) {
        packh_.reset();
    }

    void setMaxNumRecords(size_t value) { packh_.setMaxNumRecords(value); }
    void setMaxPackSize(size_t value) { packh_.setMaxPackSize(value); }

    /**
     * @ioBlocks [logical block]
     */
    bool canAddLb(uint16_t ioBlocks) const {
        return packh_.canAdd(ioBlocks * LOGICAL_BLOCK_SIZE);
    }

    /**
     * You must care about IO insertion order and overlap.
     *
     * RETURN:
     *   false: failed. You need to create another pack.
     */
    bool add(uint64_t ioAddr, uint16_t ioBlocks, const char *data) {
        assert(ioBlocks != 0);
        uint32_t dSize = ioBlocks * LOGICAL_BLOCK_SIZE;
        if (!packh_.canAdd(dSize)) return false;

        bool isZero = isAllZero(data, dSize);
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
            rec.setDataSize(dSize);
            rec.setChecksum(cybozu::util::calcChecksum(data, dSize, 0));
        }
        return add(rec.record(), data);
    }
    bool add(const struct walb_diff_record &rec, const char *data) {
        assert(isValidRec(rec));
        size_t dSize = rec.data_size;
        if (!packh_.canAdd(dSize)) return false;

        bool isNormal = isNormalRec(rec);
#ifdef WALB_DEBUG
        if (isNormal) {
            assert(rec.checksum == cybozu::util::calcChecksum(data, dSize, 0));
        }
#endif
        /* r must be true because we called canAdd() before. */
        bool r = packh_.add(rec);
        if (r && isNormal) extendAndCopy(data, dSize);
        return r;
    }

    bool empty() const {
        bool ret = packh_.nRecords() == 0;
        if (ret) assert(data_.size() == ::WALB_DIFF_PACK_SIZE);
        return ret;
    }
    bool isValid(bool checkHeaderCsum = true, std::string *errMsg = nullptr) const {
        MemoryPack mpack(&data_[0]);
        return mpack.isValid(checkHeaderCsum, errMsg);
    }
    void print(FILE *fp = ::stdout) const {
        packh_.print(fp);
        ::fprintf(fp, "pack size: %zu\n", data_.size());
    }

    /**
     * Get created pack image.
     */
    std::vector<char> getPackAsVector() {
        packh_.updateChecksum();
        assert(isValid());
        std::vector<char> ret = std::move(data_);
        reset();
        return ret;
    }
    std::unique_ptr<char[]> getPackAsUniquePtr() {
        packh_.updateChecksum();
        assert(isValid());
        std::unique_ptr<char[]> up(new char[data_.size()]);
        ::memcpy(up.get(), &data_[0], data_.size());
        reset();
        return up;
    }
    void reset() {
        data_.resize(::WALB_DIFF_PACK_SIZE);
        packh_.resetBuffer(&data_[0]);
        packh_.reset();
    }
private:
    static bool isAllZero(const char *data, size_t size) {
        for (size_t i = 0; i < size; i++) {
            if (data[i] != 0) return false;
        }
        return true;
    }
    void extendAndCopy(const char *data, size_t size) {
        assert(data);
        size_t s0 = data_.size();
        size_t s1 = size;
        data_.resize(s0 + s1);
        packh_.resetBuffer(&data_[0]);
        ::memcpy(&data_[s0], data, s1);
    }
};

}} //namespace walb::diff
