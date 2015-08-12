#pragma once

#include <snappy.h>
#include "cybozu/exception.hpp"
#include "walb_types.hpp"

namespace walb {

inline void compressSnappy(const AlignedArray& src, std::string& dst)
{
    assert(src.size() > 0);
    const size_t encSize = snappy::Compress(src.data(), src.size(), &dst);
    assert(encSize > 0);
    if (encSize != dst.size()) dst.resize(encSize);
}

inline void uncompressSnappy(const AlignedArray &src, AlignedArray &dst, const char *msg)
{
    size_t decSize;
    if (!snappy::GetUncompressedLength(src.data(), src.size(), &decSize)) {
        throw cybozu::Exception(msg) << "GetUncompressedLength" << src.size();
    }
    if (decSize != dst.size()) {
        throw cybozu::Exception(msg) << "decSize differs" << decSize << dst.size();
    }
    if (!snappy::RawUncompress(src.data(), src.size(), dst.data())) {
        throw cybozu::Exception(msg) << "RawUncompress";
    }
}

} // namespace walb
