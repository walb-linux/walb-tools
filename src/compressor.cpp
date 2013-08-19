#include "compressor.hpp"
#include "compressor-asis.hpp"
#include "compressor-snappy.hpp"

#define IMPL_CSTR(className, ...) \
    switch (mode) { \
    case Compressor::AsIs: \
        engine_ = (void*)new className ## AsIs(__VA_ARGS__); \
        break; \
    case Compressor::Snappy: \
        engine_ = (void*)new className ## Snappy(__VA_ARGS__); \
        break; \
    default: \
        throw walb::CompressorError("invalid mode"); \
    }

#define IMPL_DISPATCH(className, func, ...) \
    switch (mode_) { \
    case Compressor::AsIs: \
        return ((className ## AsIs*)engine_)->func(__VA_ARGS__); \
    case Compressor::Snappy: \
        return ((className ## Snappy*)engine_)->func(__VA_ARGS__); \
    default: \
        throw walb::CompressorError("invalid mode"); \
    }

#define IMPL_DSTR(className) \
    switch (mode_) { \
    case Compressor::AsIs: \
        delete (className ## AsIs*)engine_; \
        break; \
    case Compressor::Snappy: \
        delete (className ## Snappy*)engine_; \
        break; \
    default: \
        throw walb::CompressorError("invalid mode"); \
    }

walb::Compressor::Compressor(walb::Compressor::Mode mode, size_t maxInSize, size_t para)
    : mode_(mode), engine_(0)
{
    IMPL_CSTR(Compressor, maxInSize, para)
}

size_t walb::Compressor::getMaxOutSize() const
{
    IMPL_DISPATCH(const Compressor, getMaxOutSize)
}

size_t walb::Compressor::run(void *out, const void *in, size_t inSize)
{
    IMPL_DISPATCH(Compressor, run, out, in, inSize)
}

walb::Compressor::~Compressor() throw()
{
    IMPL_DSTR(Compressor)
}

walb::Uncompressor::Uncompressor(walb::Compressor::Mode mode, size_t para)
    : mode_(mode), engine_(0)
{
    IMPL_CSTR(Uncompressor, para)
}

size_t walb::Uncompressor::run(void *out, size_t maxOutSize, const void *in, size_t inSize)
{
    IMPL_DISPATCH(Uncompressor, run, out, maxOutSize, in, inSize)
}

walb::Uncompressor::~Uncompressor() throw()
{
    IMPL_DSTR(Uncompressor)
}

