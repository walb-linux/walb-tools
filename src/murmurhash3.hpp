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
#include "cybozu/itoa.hpp"
#include "util.hpp"
#include "util.hpp"
#include <cstdio>
#include <cassert>
#include <string>
#include <cstring>
#include "hex_byte_array.hpp"

namespace cybozu {
namespace murmurhash3 {

const size_t HASH_SIZE = 16; /* 128 bits */
class Hash : public HexByteArrayT<HASH_SIZE, Hash> {};

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
        ::MurmurHash3_x64_128(key, len, seed_, h.rawData());
        return h;
    }
};

}} //namespace cybozu::murmurhash3
