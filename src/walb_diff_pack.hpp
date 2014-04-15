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

/**
 * Walb diff pack wrapper.
 * This can be a wrapper or a manager of 4KiB memory image.
 */
class DiffPackHeader
{
private:
    char *buf_;
    bool mustDelete_;
    size_t maxNumRecords_;
    size_t maxPackSize_; /* [byte] */
public:
    DiffPackHeader()
        : buf_(allocStatic()), mustDelete_(true)
        , maxNumRecords_(::MAX_N_RECORDS_IN_WALB_DIFF_PACK)
        , maxPackSize_(::WALB_DIFF_PACK_MAX_SIZE) {
        reset();
    }
    /**
     * Buffer size must be ::WALB_DIFF_PACK_SIZE.
     * You must call reset() to initialize.
     */
    explicit DiffPackHeader(char *buf)
        : buf_(buf), mustDelete_(false)
        , maxNumRecords_(::MAX_N_RECORDS_IN_WALB_DIFF_PACK)
        , maxPackSize_(::WALB_DIFF_PACK_MAX_SIZE) {
        assert(buf);
    }
    DiffPackHeader(const DiffPackHeader &) = delete;
    DiffPackHeader(DiffPackHeader &&rhs)
        : buf_(nullptr), mustDelete_(false) {
        *this = std::move(rhs);
    }
    ~DiffPackHeader() noexcept {
        if (mustDelete_) ::free(buf_);
    }
    DiffPackHeader &operator=(const DiffPackHeader &) = delete;
    DiffPackHeader &operator=(DiffPackHeader &&rhs) {
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
    DiffRecord &record(size_t i) {
        checkRange(i);
        return static_cast<DiffRecord&>(header().record[i]);
    }
    const DiffRecord &record(size_t i) const {
        checkRange(i);
        return static_cast<const DiffRecord&>(header().record[i]);
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
    bool add(const walb::DiffRecord &inRec) {
#ifdef DEBUG
        assert(inRec.isValid());
#endif
        if (!canAdd(inRec.data_size)) { return false; }
        DiffRecord &outRec = record(header().n_records);
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
        try {
            verify();
            return true;
        } catch (std::exception &e) {
            LOGs.debug() << e.what();
            return false;
        } catch (...) {
            LOGs.debug() << "invalid";
            return false;
        }
    }
    void verify() const {
        const char *const NAME = "DiffPackHeader";
        if (cybozu::util::calcChecksum(rawData(), rawSize(), 0) != 0) {
            throw cybozu::Exception(NAME) << "invalid checksum";
        }
        if (nRecords() > MAX_N_RECORDS_IN_WALB_DIFF_PACK) {
            throw cybozu::Exception(NAME) << "invalid nRecoreds" << nRecords();
        }
        for (size_t i = 0; i < nRecords(); i++) {
            record(i).verify();
        }
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
            const DiffRecord& rec = record(i);
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

namespace diff {
/**
 * Manage a pack as a contiguous memory.
 */
class MemoryPack
{
private:
    const char *p_;
    size_t size_;
public:
    explicit MemoryPack(const char *p, size_t size) : p_(), size_(0) {
        reset(p, size);
    }
    MemoryPack(const MemoryPack &rhs) : p_(rhs.p_), size_(rhs.size_) {
    }
    MemoryPack(MemoryPack &&) = delete;
    MemoryPack &operator=(const MemoryPack &rhs) {
        reset(rhs.p_, rhs.size_);
        return *this;
    }
    MemoryPack &operator=(MemoryPack &&) = delete;

    size_t size() const { return size_; }

    const struct walb_diff_pack &header() const {
        return *reinterpret_cast<const walb_diff_pack*>(p_);
    }
    const char *data(size_t i) const {
        assert(i < header().n_records);
        if (header().record[i].data_size == 0) return nullptr;
        return &p_[offset(i)];
    }
    const char *rawPtr() const {
        return p_;
    }
    void reset(const char *p, size_t size) {
        if (!p) throw std::runtime_error("The pointer must not be null.");
        p_ = p;
        const size_t observed = ::WALB_DIFF_PACK_SIZE + header().total_size;
        if (size != observed) {
            throw cybozu::Exception(__func__) << "invalid pack size" << observed << size;
        }
        size_ = size;
        verify();
    }
private:
    void verify() const {
        const char *const NAME = "MemoryPack";
        DiffPackHeader packh((char *)&header());
        packh.verify();
        for (size_t i = 0; i < packh.nRecords(); i++) {
            const DiffRecord& rec = packh.record(i);
            if (offset(i) + rec.data_size > size_) {
                throw cybozu::Exception(NAME)
                    << "data_size out of range" << i << rec.data_size;
            }
            if (rec.isNormal()) {
                const uint32_t csum = cybozu::util::calcChecksum(data(i), rec.data_size, 0);
                if (csum != rec.checksum) {
                    throw cybozu::Exception(NAME)
                        << "invalid checksum" << csum << rec.checksum;
                }
            }
        }
    }
    size_t offset(size_t i) const {
        return ::WALB_DIFF_PACK_SIZE + header().record[i].data_offset;
    }
};

/**
 * Generator of a pack as a memory image.
 */
class Packer
{
private:
    std::vector<char> data_;
    DiffPackHeader packh_;

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
        DiffRecord rec;
        rec.io_address = ioAddr;
        rec.io_blocks = ioBlocks;
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        if (isZero) {
            rec.setAllZero();
            rec.data_size = 0;
            rec.checksum = 0;
        } else {
            rec.setNormal();
            rec.data_size = dSize;
            rec.checksum = cybozu::util::calcChecksum(data, dSize, 0);
        }
        return add(rec, data);
    }
    bool add(const DiffRecord &rec, const char *data) {
        assert(rec.isValid());
        size_t dSize = rec.data_size;
        if (!packh_.canAdd(dSize)) return false;

        bool isNormal = rec.isNormal();
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
    void verify() const {
        MemoryPack mpack(data_.data(), data_.size());
    }
    void print(FILE *fp = ::stdout) const {
        packh_.print(fp);
        ::fprintf(fp, "pack size: %zu\n", data_.size());
    }

    /**
     * Get created pack image and clear buffer
     */
    std::vector<char> getPackAsVector() {
        packh_.updateChecksum();
        verify();
        std::vector<char> ret = std::move(data_);
        reset();
        return ret;
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

} // namespace diff

inline void verifyDiffPack(const std::vector<char> &buf)
{
    diff::MemoryPack(buf.data(), buf.size());
}

} //namespace walb
