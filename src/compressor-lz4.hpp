#pragma once
#include <cassert>
#include <sys/types.h>

#include "lz4.h"

#include "compressor_if.hpp"
#include "cybozu/exception.hpp"


struct CompressorLz4 : walb::compressor_local::CompressorIF
{
    constexpr static const char *NAME = "CompressorLz4";
    CompressorLz4(size_t) {}
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize) {
        assert(outSize != nullptr);
        const ssize_t encSize = ::LZ4_compress_default((const char *)in, (char *)out, inSize, maxOutSize);
        if (encSize <= 0) return false;
        *outSize = encSize;
        return true;
    }
};

struct UncompressorLz4 : walb::compressor_local::UncompressorIF
{
    constexpr static const char *NAME = "UncompressorLz4";
    UncompressorLz4(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize) {
        const ssize_t decSize = ::LZ4_decompress_safe((const char *)in, (char *)out, inSize, maxOutSize);
        if (decSize <= 0) {
            throw cybozu::Exception(NAME) << "LZ4_decompress_safe failed" << decSize;
        }
        return decSize;
    }
};
