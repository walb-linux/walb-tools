#pragma once
#include <cstddef>

namespace walb {

namespace compressor_local {

struct CompressorIF {
    virtual ~CompressorIF() throw() {}
    virtual bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize) = 0;
};

struct UncompressorIF {
    virtual ~UncompressorIF() throw() {}
    virtual size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize) = 0;
};

} } // walb::compressor_local
