#pragma once
/**
 * @file
 * @brief MurmurHash3 wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "MurmurHash3.h"
#include "cybozu/itoa.hpp"
#include "cybozu/serializer.hpp"
#include "util.hpp"
#include "walb_util.hpp"
#include <cstdio>
#include <cassert>
#include <string>
#include <cstring>

namespace cybozu {
namespace murmurhash3 {

const size_t HASH_SIZE = 16; /* 128 bits */

struct Hash {
    uint8_t data[HASH_SIZE];
    std::string str() const {
        return walb::util::binaryToStr(&data[0], HASH_SIZE);
    }
    template <class InputStream>
    void load(InputStream &is) {
        cybozu::loadRange(data, HASH_SIZE, is);
    }
    template <class OutputStream>
    void save(OutputStream &os) const {
        cybozu::saveRange(os, data, HASH_SIZE);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const Hash &t) {
        os << t.str();
        return os;
    }
    bool operator==(const Hash& rhs) const {
        return ::memcmp(&data[0], &rhs.data[0], HASH_SIZE) == 0;
    }
    bool operator!=(const Hash& rhs) const { return !operator==(rhs); }
    void clear() {
        ::memset(data, 0, HASH_SIZE);
    }
    void doXor(const Hash& rhs) {
        for (size_t i = 0; i < HASH_SIZE; i++) {
            data[i] ^= rhs.data[i];
        }
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
        ::MurmurHash3_x64_128(key, len, seed_, &h.data[0]);
        return h;
    }
};

}} //namespace cybozu::murmurhash3
