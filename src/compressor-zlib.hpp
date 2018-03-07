#pragma once
#include <cybozu/zlib.hpp>
#include <cybozu/stream.hpp>

#include "compressor_if.hpp"
#include "cybozu/exception.hpp"


struct CompressorZlib : walb::compressor_local::CompressorIF {
    size_t compressionLevel_;
    CompressorZlib(size_t compressionLevel)
        : compressionLevel_(compressionLevel == 0 ? Z_DEFAULT_COMPRESSION : compressionLevel)
    {
        if (compressionLevel > 9) throw cybozu::Exception("CompressorZlib:bad compressionLevel") << compressionLevel;
    }
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize)
        try
    {
        size_t size = cybozu::ZlibCompress(out, maxOutSize, in, inSize, compressionLevel_);
        if (size  == 0) return false;
        *outSize = size;
        return true;
    } catch (...) {
        return false;
    }
};

struct UncompressorZlib : walb::compressor_local::UncompressorIF {
    UncompressorZlib(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        return cybozu::ZlibUncompress(out, maxOutSize, in, inSize);
    }
};
