#pragma once

#include <vector>

/**
 * Get aligned size of an address.
 * The returned value is limited to 32bit.
 *
 * Ex.
 * 0b0010111110000 --> 0b10000
 * 0b0111011000000 --> 0b1000000
 */
inline uint32_t getAlignedSize(uint64_t v)
{
    const uint32_t m = 0x1 << 31;
    if (v == 0) return m;
    uint c = 0;
    while ((v & 0x1) == 0) {
        c++;
        v >>= 1;
    }
    if (c >= 32) return m;
    return 0x1 << c;
}


inline bool isAlignedSize(uint32_t s)
{
    assert(s != 0);
    return __builtin_popcount(s) == 1;
}


inline bool isAlignedIo(uint64_t ioAddress, uint32_t ioBlocks)
{
    return ioBlocks <= getAlignedSize(ioAddress) && isAlignedSize(ioBlocks);
}

/**
 * Get max aliend size not greater than a given size.
 *
 * Examples:
 *   input -> output
 *   4095 -> 2048
 *   4096 -> 4096
 *   4097 -> 4096
 */
inline uint32_t getMaxAlignedSize(uint32_t s)
{
    if (s == 0) return 0;

    uint c = 0;
    while (s != 0) {
        s >>= 1;
        c++;
    }
    return 0x1 << (c - 1);
}


/**
 * Split an address range to alined ones.
 * maxIoBlocks: 0 means unlimit. It uses getMaxAlignedSize(maxIoBlocks).
 */
inline std::vector<uint32_t> splitIoToAligned(uint64_t ioAddress, uint32_t ioBlocks, uint32_t maxIoBlocks = 0)
{
    maxIoBlocks = getMaxAlignedSize(maxIoBlocks);
    std::vector<uint32_t> v;
    while (ioBlocks > 0) {
        uint32_t s = getAlignedSize(ioAddress);
        if (s > ioBlocks) {
            s = getMaxAlignedSize(ioBlocks);
        }
        if (maxIoBlocks > 0) {
            s = std::min(s, maxIoBlocks);
        }
        v.push_back(s);
        ioAddress += s;
        ioBlocks -= s;
    }
    return v;
}
