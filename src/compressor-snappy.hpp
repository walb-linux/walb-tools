#pragma once
#include <snappy.h>

struct CompressorSnappy : walb::compressor_local::CompressorIF {
    size_t maxInSize_;
    CompressorSnappy(size_t maxInSize, size_t)
        : maxInSize_(maxInSize) {}
    size_t getMaxOutSize() const
    {
        return snappy::MaxCompressedLength(maxInSize_);
    }
    size_t run(void *out, const void *in, size_t inSize)
    {
        if (inSize > maxInSize_) throw cybozu::Exception("CompressorSnappy:runtoo large inSize") << inSize << maxInSize_;
        size_t encSize;
        snappy::RawCompress((const char*)in, inSize, (char*)out, &encSize);
        return encSize;
    }
};

struct UncompressorSnappy : walb::compressor_local::UncompressorIF {
    UncompressorSnappy(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        const char *p = (const char *)in;
        size_t decSize;
        if (!snappy::GetUncompressedLength(p, inSize, &decSize)) {
            throw cybozu::Exception("UncompressorSnappy:run:GetUncompressedLength") << inSize;
        }
        if (decSize > maxOutSize) {
            throw cybozu::Exception("UncompressorSnappy:run:small maxOutSize") << decSize << maxOutSize;
        }
        if (!snappy::RawUncompress(p, inSize, (char*)out)) {
            throw cybozu::Exception("UncompressorSnappy:run:RawUncompress");
        }
        return decSize;
    }
};

