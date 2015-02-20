#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <cassert>

namespace cybozu {

template <size_t SIZE>
struct HashT
{
    uint8_t data[SIZE];
    void zeroClear() { ::memset(data, 0, SIZE); }
    std::string str() const {
        std::string s;
        s.resize(SIZE * 2 + 1);
        for (size_t i = 0; i < SIZE; i++) {
            ::snprintf(&s[i * 2], 3, "%02x", data[i]);
        }
        s.resize(SIZE * 2);
        return s;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const HashT& hash) {
        os << hash.str();
        return os;
    }
};

using Hash128 = HashT<16>;

/**
 * This code assume little-endian architecture.
 *
 * This code supports stream inputs, that is,
 * you can call compress() with read data fragments in line.
 * You need not care about size of data fragments.
 */
template <size_t C, size_t D>
class SipHashT
{
    uint64_t v0_;
    uint64_t v1_;
    uint64_t v2_;
    uint64_t v3_;
    size_t size_;
    char buf_[8];
    size_t bufSize_;
    static constexpr const uint64_t DEFAULT_K0 = 0x0706050403020100ULL;
    static constexpr const uint64_t DEFAULT_K1 = 0x0f0e0d0c0b0a0908ULL;
public:
    SipHashT(uint64_t k0 = DEFAULT_K0, uint64_t k1 = DEFAULT_K1) {
        init(k0, k1);
    }
    void init(uint64_t k0 = DEFAULT_K0, uint64_t k1 = DEFAULT_K1) {
        v0_ = k0 ^ 0x736f6d6570736575ULL;
        v1_ = k1 ^ 0x646f72616e646f6dULL;
        v2_ = k0 ^ 0x6c7967656e657261ULL;
        v3_ = k1 ^ 0x7465646279746573ULL;
        size_ = 0;
        bufSize_ = 0;
    }
    /**
     * You can call this many times.
     */
    void compress(const void *data, size_t size) {
        char *p = (char *)data;
        size_ += size;
        if (bufSize_ > 0) {
            const size_t s = std::min(8 - bufSize_, size);
            const size_t off = bufSize_;
            bufSize_ += s;
            ::memcpy(&buf_[off], p, s);
            p += s;
            size -= s;
            if (bufSize_ < 8) return;
            assert(bufSize_ == 8);
            uint64_t m;
            ::memcpy(&m, buf_, 8);
            compressMessage(m);
            bufSize_ = 0;
        }
        assert(bufSize_ == 0);
        while (size >= 8) {
            uint64_t m;
            ::memcpy(&m, p, 8);
            compressMessage(m);
            p += 8;
            size -= 8;
        }
        assert(bufSize_ == 0);
        if (size > 0) {
            ::memcpy(buf_, p, size);
            bufSize_ = size;
        }
    }
    /**
     * You must call this just once last.
     */
    uint64_t finalize() {
        finalizeDetail();
        return (v0_ ^ v1_) ^ (v2_ ^ v3_);
    }
    /**
     * This is not non-official version of 128bit output.
     */
    Hash128 finalize128() {
        finalizeDetail();
        const uint64_t v0 = v0_ ^ v1_;
        const uint64_t v1 = v2_ ^ v3_;
        Hash128 ret;
        ::memcpy(&ret.data[0], &v0, 8);
        ::memcpy(&ret.data[8], &v1, 8);
        return ret;
    }
private:
    static uint64_t rotate(uint64_t x, uint b) {
        return (x << b) | (x >> (64 - b));
    }
    void doSipRound() {
        v0_ += v1_; v2_ += v3_;
        v1_ = rotate(v1_, 13) ^ v0_;
        v3_ = rotate(v3_, 16) ^ v2_;
        v0_ = rotate(v0_, 32);
        v2_ += v1_; v0_ += v3_;
        v1_ = rotate(v1_, 17) ^ v2_;
        v3_ = rotate(v3_, 21) ^ v0_;
        v2_ = rotate(v2_, 32);
    }
    void finalizeDetail() {
        uint64_t m = uint64_t(size_ & 0xff) << 56; // MSB
        if (bufSize_ > 0) {
            assert(bufSize_ < 8);
            ::memcpy(&m, buf_, bufSize_);
            bufSize_ = 0;
        }
        compressMessage(m);
        v2_ ^= 0xff;
        for (size_t i = 0; i < D; i++) doSipRound();
    }
    void compressMessage(uint64_t m) {
        v3_ ^= m;
        for (size_t i = 0; i < C; i++) doSipRound();
        v0_ ^= m;
    }
};

using SipHash24 = SipHashT<2, 4>;

inline uint64_t sipHash24_64(void *data, size_t size)
{
    SipHash24 sh;
    sh.compress(data, size);
    return sh.finalize();
}

inline Hash128 sipHash24_128(void *data, size_t size)
{
    SipHash24 sh;
    sh.compress(data, size);
    return sh.finalize128();
}

} // namespace cybozu
