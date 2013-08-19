#include "compressor.hpp"
#include "compressor-asis.hpp"
#include "compressor-snappy.hpp"
#include "compressor-zlib.hpp"

#define IMPL_CSTR(className, ...) \
    switch (mode) { \
    case Compressor::AsIs: \
        engine_ = new className ## AsIs(__VA_ARGS__); \
        break; \
    case Compressor::Snappy: \
        engine_ = new className ## Snappy(__VA_ARGS__); \
        break; \
    case Compressor::Zlib: \
        engine_ = new className ## Zlib(__VA_ARGS__); \
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
    return engine_->getMaxOutSize();
}

size_t walb::Compressor::run(void *out, const void *in, size_t inSize)
{
    return engine_->run(out, in, inSize);
}

walb::Compressor::~Compressor() throw()
{
    delete engine_;
}

walb::Uncompressor::Uncompressor(walb::Compressor::Mode mode, size_t para)
    : mode_(mode), engine_(0)
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

