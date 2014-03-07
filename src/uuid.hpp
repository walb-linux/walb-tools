#pragma once
/**
 * @file
 * @brief Uuid utility.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "walb/util.h"
#include "cybozu/serializer.hpp"
#include "walb_util.hpp"

namespace cybozu {

/**
 * Hex byte array.
 *   rawData(), rawSize() for direct memory access.
 *   str() and operator<<() for pretty printer.
 *   load(), save() for serializer.
 */
class Uuid
{
private:
    uint8_t data_[UUID_SIZE];
public:
    Uuid() : data_() {}
    explicit Uuid(const uint8_t *data) { set(data); }
    bool operator==(const Uuid &rhs) const { return cmp(rhs) == 0; }
    bool operator!=(const Uuid &rhs) const { return cmp(rhs) != 0; }
    const void *rawData() const { return data_; }
    void *rawData() { return data_; }
    static size_t rawSize() { return UUID_SIZE; }
    std::string str() const {
        return walb::util::binaryToStr(&data_[0], UUID_SIZE);
    }
    void set(const void *data) {
        ::memcpy(data_, data, UUID_SIZE);
    }
    template <class InputStream>
    void load(InputStream &is) {
        cybozu::loadRange(data_, UUID_SIZE, is);
    }
    template <class OutputStream>
    void save(OutputStream &os) const {
        cybozu::saveRange(os, data_, UUID_SIZE);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const Uuid &t) {
        os << t.str();
        return os;
    }
private:
    int cmp(const Uuid &rhs) const {
        return ::memcmp(data_, rhs.data_, UUID_SIZE);
    }
};

} //namespace cybozu
