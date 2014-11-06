#pragma once
#include <snappy.h>
#include <string>

struct CompressorSnappy : walb::compressor_local::CompressorIF {
    CompressorSnappy(size_t) {}
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize)
    {
        const size_t maxCompressedSize = snappy::MaxCompressedLength(inSize);
        if (maxCompressedSize <= maxOutSize) {
            snappy::RawCompress((const char*)in, inSize, (char*)out, outSize);
            return true;
        }
        std::string enc;
        size_t encSize = snappy::Compress((const char*)in, inSize, &enc);
        if (maxOutSize < encSize) return false;
        memcpy(out, &enc[0], encSize);
        *outSize = encSize;
        return true;
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
        if (maxOutSize < decSize) {
            throw cybozu::Exception("UncompressorSnappy:run:small maxOutSize") << decSize << maxOutSize;
        }
        if (!snappy::RawUncompress(p, inSize, (char*)out)) {
            throw cybozu::Exception("UncompressorSnappy:run:RawUncompress");
        }
        return decSize;
    }
};

