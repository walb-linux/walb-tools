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
    constexpr static const char *NAME = "DiffPackHeader";
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
    void readFrom(Reader &reader, bool doVerify = true) {
        reader.read(data(), size());
        if (doVerify) verify();
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
    void verify() const;
    void print(::FILE *fp = ::stdout) const;
    std::string toStr() const;
    friend std::ostream& operator<<(std::ostream& os, const DiffPackHeader& rhs) {
        os << rhs.toStr();
        return os;
    }
    void clear() {
        ::memset(data(), 0, size());
    }
    bool canAdd(uint32_t dataSize) const;
    /**
     * RETURN:
     *   true when added successfully.
     *   false when pack is full.
     */
    bool add(const DiffRecord &inRec);
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
    constexpr static const char *NAME = "MemoryDiffPack";
    MemoryDiffPack(const char *p, size_t size) : p_(), size_(0) {
        reset(p, size);
    }
    MemoryDiffPack(const MemoryDiffPack &rhs) : p_(rhs.p_), size_(rhs.size_) {
    }
    MemoryDiffPack(MemoryDiffPack &&) = delete;
    MemoryDiffPack &operator=(const MemoryDiffPack &rhs) {
        reset(rhs.p_, rhs.size_);
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
    void reset(const char *p, size_t size);
    void verify(bool doChecksum) const;
private:
    size_t offset(size_t i) const {
        return ::WALB_DIFF_PACK_SIZE + header()[i].data_offset;
    }
};

/**
 * Generator of a pack as a memory image.
 * IOs' checksum will neither be set nor checked.
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
     * Checksum will not be calculated.
     *
     * RETURN:
     *   false: failed. You need to create another pack.
     */
    bool add(uint64_t ioAddr, uint32_t ioBlocks, const char *data);
    bool add(const DiffRecord &rec, const char *data);
    bool empty() const;
    void verify() const {
#ifndef NDEBUG
        const AlignedArray ary = abuf_.getAsArray();
        MemoryDiffPack mpack(ary.data(), ary.size());
        mpack.verify(false);
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
    const DiffPackHeader& header() const {
        return *reinterpret_cast<const DiffPackHeader *>(abuf_[0].data());
    }
    /**
     * This is for debug.
     */
    size_t size() const {
        return abuf_.totalSize();
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
        pack_ = reinterpret_cast<DiffPackHeader *>(abuf_[0].data());
    }
};

inline void verifyDiffPack(const char *data, size_t size, bool doChecksum)
{
    MemoryDiffPack pack(data, size);
    pack.verify(doChecksum);
}

inline void verifyDiffPackSize(size_t size, const char *msg)
{
    const size_t maxPackSize = ::WALB_DIFF_PACK_SIZE + ::WALB_DIFF_PACK_MAX_SIZE;
    if (size > maxPackSize) {
        throw cybozu::Exception(msg) << "too large pack size" << size << maxPackSize;
    }
}

} //namespace walb
