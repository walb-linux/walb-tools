#pragma once
#include <string.h>

struct CompressorAsIs : walb::compressor_local::CompressorIF {
    CompressorAsIs(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        if (maxOutSize < inSize) throw cybozu::Exception("CompressorAsIs:run:small maxOutSize") << inSize << maxOutSize;
        memcpy(out, in, inSize);
        return inSize;
    }
};

struct UncompressorAsIs : walb::compressor_local::UncompressorIF {
    UncompressorAsIs(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        if (maxOutSize < inSize) throw cybozu::Exception("UncompressorAsIs:run:small maxOutSize") << inSize << maxOutSize;
        memcpy(out, in, inSize);
        return inSize;
    }
};

