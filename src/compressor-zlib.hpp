#pragma once
#include <cybozu/zlib.hpp>
#include <cybozu/stream.hpp>

struct CompressorZlib : walb::compressor_local::CompressorIF {
    size_t compressionLevel_;
    CompressorZlib(size_t compressionLevel)
        : compressionLevel_(compressionLevel == 0 ? Z_DEFAULT_COMPRESSION : compressionLevel)
    {
        if (compressionLevel > 9) throw cybozu::Exception("CompressorZlib:bad compressionLevel") << compressionLevel;
    }
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        cybozu::MemoryOutputStream os(out, maxOutSize);
        cybozu::ZlibCompressorT<cybozu::MemoryOutputStream> enc(os, false, compressionLevel_);
        enc.write(in, inSize);
        enc.flush();
        return os.pos;
    }
};

struct UncompressorZlib : walb::compressor_local::UncompressorIF {
    UncompressorZlib(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
       cybozu::MemoryInputStream is(in, inSize);
       cybozu::ZlibDecompressorT<cybozu::MemoryInputStream> dec(is);
       char *const top = (char *)out;
       size_t pos = 0;
       for (;;) {
           size_t readSize = dec.readSome(top + pos, maxOutSize - pos);
           if (readSize == 0) return pos;
           pos += readSize;
           if (pos == maxOutSize) {
               if (dec.isEmpty()) return pos;
               throw cybozu::Exception("UncompressorZlib:run:small maxOutSize") << maxOutSize;
           }
       }
    }
};

