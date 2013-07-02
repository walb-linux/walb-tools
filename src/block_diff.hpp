/**
 * @file
 * @brief block diff utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef BLOCK_DIFF_UTIL_HPP
#define BLOCK_DIFF_UTIL_HPP

#include <memory>
#include <cinttypes>

namespace block_diff {

/**
 * Block diff key interface.
 */
struct BlockDiffKey
{
public:
    /* Constructor(const char *data, size_t size) must be defined. */
    /* Constructor(uint64_t ioAddress, uint16_t ioBlocks, size_t size) must be defined. */
    virtual ~BlockDiffKey() noexcept = default;
    virtual uint64_t ioAddress() const = 0;
    virtual uint16_t ioBlocks() const = 0;
    virtual const char *rawData() const = 0;
    virtual size_t rawSize() const = 0;

    int compare(const BlockDiffKey &rhs) const {
        int c = compareInt(ioAddress(), rhs.ioAddress());
        if (c == 0) {
            return compareInt(ioBlocks(), rhs.ioBlocks());
        } else {
            return c;
        }
    }
    bool operator==(const BlockDiffKey &rhs) const {
        return compare(rhs) == 0;
    }
    bool operator<(const BlockDiffKey &rhs) const {
        return compare(rhs) < 0;
    }
    bool operator!=(const BlockDiffKey &rhs) const { return !(*this == rhs); }
    bool operator<=(const BlockDiffKey &rhs) const { return *this < rhs || *this == rhs; }
    bool operator>(const BlockDiffKey &rhs) const { return !(*this <= rhs); }
    bool operator>=(const BlockDiffKey &rhs) const { return !(*this < rhs); }

    bool isOverlapped(const BlockDiffKey &rhs) const {
        return ioAddress() < rhs.ioAddress() + rhs.ioBlocks() &&
            rhs.ioAddress() < ioAddress() + ioBlocks();
    }

    bool isOverwrittenBy(const BlockDiffKey &rhs) const {
        return rhs.ioAddress() <= ioAddress() &&
            ioAddress() + ioBlocks() <= rhs.ioAddress() + rhs.ioBlocks();
    }

private:
    template <typename IntType>
    static int compareInt(IntType a, IntType b) {
        if (a == b) { return 0; }
        return a < b ? -1 : 1;
    }
};

/**
 * Block diff value interface.
 */
template <class T>
struct BlockDiffValue
{
    virtual ~BlockDiffValue() noexcept = default;
    virtual const char *rawData() const = 0;
    virtual char *rawData() = 0;
    virtual size_t rawSize() const = 0;
    virtual std::shared_ptr<T> getPortion(size_t offset, size_t nBlocks) const = 0;
};

/**
 * Block diff database header interface.
 */
struct BlockDiffHeader
{
    virtual ~BlockDiffHeader() noexcept = default;
    virtual const char *rawData() const = 0;
    virtual size_t rawSize() const = 0;
    virtual void read(const void *p) = 0;

    virtual uint16_t getMaxIoBlocks() const = 0;
    virtual void setMaxIoBlocksIfNecessary(uint16_t ioBlocks) = 0;
    virtual void resetMaxIoBlocks() = 0;

    virtual void prepareToWrite() = 0;

    virtual bool isValidHeader() const = 0;
    virtual bool isValidStatistics() const = 0;
};

} //namespace block_diff

#endif /* BLOCK_DIFF_UTIL_HPP */
