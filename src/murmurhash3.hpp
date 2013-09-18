#pragma once
/**
 * @file
 * @brief MurmurHash3 wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "MurmurHash3.h"
#include "cybozu/serializer.hpp"
#include <cstdio>
#include <cassert>
#include <string>
#include <cstring>

namespace cybozu {
namespace murmurhash3 {

const size_t HASH_SIZE = 16; /* 128 bits */

/**
 * 128bits hash data.
 *   str() for pretty printer.
 *   load(), save() for serializer.
 */
class Hash
{
private:
    uint8_t data_[HASH_SIZE];
public:
    bool operator==(const Hash &rhs) const {
        return ::memcmp(&data_[0], &rhs.data_[0], HASH_SIZE) == 0;
    }
    bool operator!=(const Hash &rhs) const {
        return ::memcmp(&data_[0], &rhs.data_[0], HASH_SIZE) != 0;
    }
    void *ptr() { return &data_[0]; }
    const void *ptr() const { return &data_[0]; }
    /**
     * hex string.
     */
    std::string str() const {
        std::string s;
        s.resize(HASH_SIZE * 2 + 1);
        for (size_t i = 0; i < HASH_SIZE; i++) {
            int r = ::snprintf(&s[i * 2], 3, "%02x", data_[i]);
            assert(r == 2);
        }
        s.resize(HASH_SIZE * 2);
        return s;
    }
    size_t size() const { return HASH_SIZE; }

    template <class InputStream>
    void load(InputStream &is) {
        cybozu::loadRange(&data_[0], HASH_SIZE, is);
    }
    template <class OutputStream>
    void save(OutputStream& os) const {
        cybozu::saveRange(os, &data_[0], HASH_SIZE);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const Hash &hash) {
        os << hash.str();
        return os;
    }
};

/**
 * Hash calclator.
 */
class Hasher
{
private:
    uint32_t seed_;
public:
    explicit Hasher(uint32_t seed = 0)
        : seed_(seed) {}

    Hash operator()(const void *key, size_t len) const {
        Hash h;
        ::MurmurHash3_x64_128(key, len, seed_, h.ptr());
        return h;
    }
};

}} //namespace cybozu::murmurhash3
