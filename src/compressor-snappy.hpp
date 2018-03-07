#pragma once
#include <snappy-c.h>
#include <string>

#include "compressor_if.hpp"
#include "cybozu/exception.hpp"


struct CompressorSnappy : walb::compressor_local::CompressorIF {
    CompressorSnappy(size_t) {}
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize)
    {
        const size_t maxSize = ::snappy_max_compressed_length(inSize);
        *outSize = maxSize;
        if (maxSize <= maxOutSize) {
            return ::snappy_compress((const char*)in, inSize, (char *)out, outSize) == SNAPPY_OK;
        }
        std::string enc;
        enc.resize(maxSize);
        if (::snappy_compress((const char*)in, inSize, &enc[0], outSize) != SNAPPY_OK) {
            return false;
        }
        ::memcpy(out, &enc[0], *outSize);
        return true;
    }
};

struct UncompressorSnappy : walb::compressor_local::UncompressorIF {
    UncompressorSnappy(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        size_t decSize = maxOutSize;
        if (::snappy_uncompress((const char *)in, inSize, (char *)out, &decSize) != SNAPPY_OK) {
            throw cybozu::Exception("UncompressorSnappy:run:snappy_uncompress");
        }
        return decSize;
    }
};
