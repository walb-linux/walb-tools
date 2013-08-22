#pragma once
#include <string.h>

struct CompressorAsIs : walb::compressor_local::CompressorIF {
    size_t maxInSize_;
    CompressorAsIs(size_t maxInSize, size_t)
        : maxInSize_(maxInSize) {}
    size_t getMaxOutSize() const
    {
        return maxInSize_;
    }
    size_t run(void *out, const void *in, size_t inSize)
    {
        if (inSize > maxInSize_) throw cybozu::Exception("CompressorAsIs:run:too large inSize") << inSize << maxInSize_;
        memcpy(out, in, inSize);
        return inSize;
    }
};

struct UncompressorAsIs : walb::compressor_local::UncompressorIF {
    UncompressorAsIs(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        if (inSize > maxOutSize) throw cybozu::Exception("UncompressorAsIs:run:too large inSize") << inSize << maxOutSize;
        memcpy(out, in, inSize);
        return inSize;
    }
};

