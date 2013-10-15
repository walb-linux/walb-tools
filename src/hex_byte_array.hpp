#pragma once
/**
 * @file
 * @brief Hex byte array utility.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include "cybozu/serializer.hpp"
#include "cybozu/atoi.hpp"

namespace cybozu {

/**
 * Hex byte array.
 *   rawData(), rawSize() for direct memory access.
 *   str() and operator<<() for pretty printer.
 *   load(), save() for serializer.
 */
template <size_t SIZE, class T>
class HexByteArrayT
{
private:
    uint8_t data_[SIZE];
public:
    bool operator==(const T &rhs) const {
        return ::memcmp(data_, rhs.data_, SIZE) == 0;
    }
    bool operator!=(const T &rhs) const {
        return ::memcmp(data_, rhs.data_, SIZE) != 0;
    }
    const void *rawData() const { return data_; }
    void *rawData() { return data_; }
    static size_t rawSize() { return SIZE; }
    /**
     * Hex string.
     */
    std::string str() const {
        std::string s;
        s.resize(SIZE * 2);
        for (size_t i = 0; i < SIZE; i++) {
            cybozu::itohex(&s[i * 2], 2, data_[i], false);
        }
        return s;
    }
    template <class InputStream>
    void load(InputStream &is) {
        cybozu::loadRange(data_, SIZE, is);
    }
    template <class OutputStream>
    void save(OutputStream &os) const {
        cybozu::saveRange(os, data_, SIZE);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const T &t) {
        os << t.str();
        return os;
    }
};

} //namespace cybozu
