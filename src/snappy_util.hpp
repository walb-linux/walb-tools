#pragma once

#include <snappy-c.h>
#include "cybozu/exception.hpp"
#include "walb_types.hpp"

namespace walb {

inline void compressSnappy(const AlignedArray& src, std::string& dst, const char *msg = "")
{
    assert(src.size() > 0);
    const size_t maxSize = ::snappy_max_compressed_length(src.size());
    dst.resize(maxSize);
    size_t encSize = maxSize;
    snappy_status st = ::snappy_compress(src.data(), src.size(), &dst[0], &encSize);
    if (st != SNAPPY_OK) {
        throw cybozu::Exception(msg) << "snappy_compress failed" << st;
    }
    assert(encSize > 0);
    dst.resize(encSize);
}

inline void uncompressSnappy(const AlignedArray &src, AlignedArray &dst, const char *msg = "")
{
    size_t decSize = dst.size();
    snappy_status st = ::snappy_uncompress((const char *)src.data(), src.size(), (char *)dst.data(), &decSize);
    if (st != SNAPPY_OK) {
        throw cybozu::Exception(msg) << "snappy_uncompress failed" << st;
    }
    assert(dst.size() == decSize);
}

} // namespace walb
