#pragma once
#include "zstd.h"
#include "walb_logger.hpp"

#include "compressor_if.hpp"
#include "cybozu/exception.hpp"


struct CompressorZstd : walb::compressor_local::CompressorIF
{
    constexpr static const char *NAME() { return "CompressorZstd"; };
    size_t level_;
    CompressorZstd(size_t level) : level_(level == 0 ? 1 : level) {
        if (level >= 20) {
            throw cybozu::Exception(NAME()) << "bad compression level" << level;
        }
    }
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize) {
        assert(outSize != nullptr);
        const size_t ret = ::ZSTD_compress(out, maxOutSize, in, inSize, level_);
        if (::ZSTD_isError(ret)) {
            LOGs.warn() << NAME() << ::ZSTD_getErrorName(ret);
            return false;
        }
        *outSize = ret;
        return true;
    }
};

struct UncompressorZstd : walb::compressor_local::UncompressorIF
{
    constexpr static const char *NAME() { return "UncompressorZstd"; }
    UncompressorZstd(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize) {
        const size_t ret = ::ZSTD_decompress(out, maxOutSize, in, inSize);
        if (::ZSTD_isError(ret)) {
            throw cybozu::Exception(NAME()) << "ZSTD_decompress failed" << ::ZSTD_getErrorName(ret);
        }
        return ret;
    }
};
