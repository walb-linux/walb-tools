#include "compressor.hpp"
#include "compressor-asis.hpp"
#include "compressor-snappy.hpp"
#include "compressor-zlib.hpp"
#include "compressor-xz.hpp"

#define IMPL_CSTR(className, ...) \
    switch (mode) { \
    case WALB_DIFF_CMPR_NONE: \
        engine_ = new className ## AsIs(__VA_ARGS__); \
        break; \
    case WALB_DIFF_CMPR_SNAPPY: \
        engine_ = new className ## Snappy(__VA_ARGS__); \
        break; \
    case WALB_DIFF_CMPR_GZIP: \
        engine_ = new className ## Zlib(__VA_ARGS__); \
        break; \
    case WALB_DIFF_CMPR_LZMA: \
        engine_ = new className ## Xz(__VA_ARGS__); \
        break; \
    default: \
        throw cybozu::Exception(#className ":invalid mode") << mode; \
    }

walb::Compressor::Compressor(int mode, size_t compressionLevel)
    : engine_(0)
{
    IMPL_CSTR(Compressor, compressionLevel)
}

size_t walb::Compressor::run(void *out, size_t maxOutSize, const void *in, size_t inSize)
{
    return engine_->run(out, maxOutSize, in, inSize);
}

walb::Compressor::~Compressor() throw()
{
    delete engine_;
}

walb::Uncompressor::Uncompressor(int mode, size_t para)
    : engine_(0)
{
    IMPL_CSTR(Uncompressor, para)
}

size_t walb::Uncompressor::run(void *out, size_t maxOutSize, const void *in, size_t inSize)
{
    return engine_->run(out, maxOutSize, in, inSize);
}

walb::Uncompressor::~Uncompressor() throw()
{
    delete engine_;
}

