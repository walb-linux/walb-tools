#pragma once
/**
 * walb diff utiltities for packs.
 */
#include "walb_diff_base.hpp"
#include "append_buffer.hpp"
#include <exception>

namespace walb {

/**
 * Walb diff pack wrapper.
 * Some functions requires contigous 4KiB memory image.
 */
struct DiffPackHeader : walb_diff_pack
{
    DiffRecord &operator[](size_t i) {
        return static_cast<DiffRecord&>(record[i]);
    }
    const DiffRecord &operator[](size_t i) const {
        return static_cast<const DiffRecord&>(record[i]);
    }
    uint32_t wholePackSize() const { return WALB_DIFF_PACK_SIZE + total_size; }
    uint32_t uncompressedTotalSize() const {
        uint32_t total = 0;
        for (uint16_t i = 0; i < n_records; i++) {
            total += record[i].io_blocks;
        }
        return total * LOGICAL_BLOCK_SIZE;
    }
    bool isEnd() const {
        const uint8_t mask = 1U << WALB_DIFF_PACK_END;
        return (flags & mask) != 0;
    }
    void setEnd() {
        const uint8_t mask = 1U << WALB_DIFF_PACK_END;
        flags |= mask;
    }

    void *data() { return (void *)this; }
    const void *data() const { return (const void *)this; }
    size_t size() const { return WALB_DIFF_PACK_SIZE; }

    template <typename Writer>
    void writeTo(Writer &writer) {
        updateChecksum();
        writer.write(data(), size());
    }
    template <typename Reader>
    void readFrom(Reader &reader) {
        reader.read(data(), size());
        verify();
    }

    void updateChecksum() {
        checksum = 0;
        checksum = cybozu::util::calcChecksum(data(), size(), 0);
#ifndef NDEBUG
        verify();
#endif
    }
    bool isValid() const {
        try {
            verify();
            return true;
        } catch (...) {
            return false;
        }
    }
    void verify() const {
        const char *const NAME = "DiffPackHeader";
        if (cybozu::util::calcChecksum(data(), size(), 0) != 0) {
            throw cybozu::Exception(NAME) << "invalid checksum";
        }
        if (n_records > MAX_N_RECORDS_IN_WALB_DIFF_PACK) {
            throw cybozu::Exception(NAME) << "invalid n_recoreds" << n_records;
        }
        for (size_t i = 0; i < n_records; i++) {
            (*this)[i].verify();
        }
    }
    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "checksum %08x\n"
                  "n_records: %u\n"
                  "total_size: %u\n"
                  "isEnd: %d\n"
                  , checksum
                  , n_records
                  , total_size
                  , isEnd());
        for (size_t i = 0; i < n_records; i++) {
            ::fprintf(fp, "record %zu: ", i);
            (*this)[i].printOneline(fp);
        }
    }
    void clear() {
        ::memset(data(), 0, size());
    }
    bool canAdd(uint32_t dataSize) const {
        if (MAX_N_RECORDS_IN_WALB_DIFF_PACK <= n_records) {
            return false;
        }
        if (0 < n_records && WALB_DIFF_PACK_MAX_SIZE < total_size + dataSize) {
            return false;
        }
        return true;
    }
    /**
     * RETURN:
     *   true when added successfully.
     *   false when pack is full.
     */
    bool add(const DiffRecord &inRec) {
#ifdef DEBUG
        assert(inRec.isValid());
#endif
        if (!canAdd(inRec.data_size)) return false;
        DiffRecord &outRec = (*this)[n_records];
        outRec = inRec;
        assert(::memcmp(&outRec, &inRec, sizeof(outRec)) == 0);
        outRec.data_offset = total_size;
        n_records++;
        total_size += inRec.data_size;
        assert(outRec.data_size == inRec.data_size);
        return true;
    }
};

/**
 * Manage a pack as a contiguous memory.
 */
class MemoryDiffPack
{
private:
    const char *p_;
    size_t size_;
public:
    MemoryDiffPack(const char *p, size_t size, bool doVerify) : p_(), size_(0) {
        reset(p, size, doVerify);
    }
    MemoryDiffPack(const MemoryDiffPack &rhs) : p_(rhs.p_), size_(rhs.size_) {
    }
    MemoryDiffPack(MemoryDiffPack &&) = delete;
    MemoryDiffPack &operator=(const MemoryDiffPack &rhs) {
        reset(rhs.p_, rhs.size_, false);
        return *this;
    }
    MemoryDiffPack &operator=(MemoryDiffPack &&) = delete;

    size_t size() const { return size_; }

    const DiffPackHeader &header() const {
        return *reinterpret_cast<const DiffPackHeader*>(p_);
    }
    const char *data(size_t i) const {
        assert(i < header().n_records);
        if (header()[i].data_size == 0) return nullptr;
        return &p_[offset(i)];
    }
    const char *rawPtr() const {
        return p_;
    }
    void reset(const char *p, size_t size, bool doVerify) {
        if (!p) throw std::runtime_error("The pointer must not be null.");
        p_ = p;
        const size_t observed = ::WALB_DIFF_PACK_SIZE + header().total_size;
        if (size != observed) {
            throw cybozu::Exception(__func__) << "invalid pack size" << observed << size;
        }
        size_ = size;
        if (doVerify) verify();
    }
private:
    void verify() const {
        const char *const NAME = "MemoryDiffPack";
        header().verify();
        for (size_t i = 0; i < header().n_records; i++) {
            const DiffRecord& rec = header()[i];
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
        return ::WALB_DIFF_PACK_SIZE + header()[i].data_offset;
    }
};

/**
 * Generator of a pack as a memory image.
 */
class DiffPacker
{
private:
    AppendBuffer abuf_;
    DiffPackHeader *pack_;

public:
    DiffPacker()
        : abuf_()
        , pack_() {
        abuf_.append(::WALB_DIFF_PACK_SIZE);
        setPackPtr();
        pack_->clear();
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
        if (!pack_->canAdd(dSize)) return false;

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
        const size_t dSize = rec.data_size;
        if (!pack_->canAdd(dSize)) return false;

        bool isNormal = rec.isNormal();
#ifdef WALB_DEBUG
        if (isNormal) {
            assert(rec.checksum == cybozu::util::calcChecksum(data, dSize, 0));
        }
#endif
        /* r must be true because we called canAdd() before. */
        bool r = pack_->add(rec);
        if (r && isNormal) extendAndCopy(data, dSize);
        return r;
    }
    bool empty() const {
        bool ret = pack_->n_records == 0;
        if (ret) {
            assert(abuf_.size() == 1);
            assert(abuf_[0].size() == ::WALB_DIFF_PACK_SIZE);
        }
        return ret;
    }
    void verify() const {
#ifndef NDEBUG
        const AlignedArray ary = abuf_.getAsArray();
        MemoryDiffPack mpack(ary.data(), ary.size(), true);
#endif
    }
    void print(FILE *fp = ::stdout) const {
        pack_->print(fp);
        ::fprintf(fp, "pack size: %zu\n", abuf_.totalSize());
    }
    /**
     * Get created pack image and clear buffer
     */
    AlignedArray getPackAsArray() {
        pack_->updateChecksum();
        verify();
        AlignedArray ary = abuf_.getAsArray();
        clear();
        return ary;
    }
    void clear() {
        abuf_.resize(1);
        setPackPtr();
        pack_->clear();
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
        abuf_.append(data, size);
        setPackPtr();
    }
    void setPackPtr() {
        assert(abuf_.size() > 0);
        assert(abuf_[0].size() == ::WALB_DIFF_PACK_SIZE);
        pack_ = (DiffPackHeader *)abuf_[0].data();
    }
};

inline void verifyDiffPack(const std::vector<char> &buf)
{
    MemoryDiffPack(buf.data(), buf.size(), true);
}

inline void verifyDiffPackSize(size_t size, const char *msg)
{
    const size_t maxPackSize = ::WALB_DIFF_PACK_SIZE + ::WALB_DIFF_PACK_MAX_SIZE;
    if (size > maxPackSize) {
        throw cybozu::Exception(msg) << "too large pack size" << size << maxPackSize;
    }
}

} //namespace walb
