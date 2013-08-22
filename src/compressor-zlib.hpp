#pragma once
#include <cybozu/zlib.hpp>
#include <cybozu/stream.hpp>

struct CompressorZlib : walb::compressor_local::CompressorIF {
    size_t maxInSize_;
    size_t compressionLevel_;
    CompressorZlib(size_t maxInSize, size_t compressionLevel)
        : maxInSize_(maxInSize), compressionLevel_(compressionLevel) {}
    size_t getMaxOutSize() const
    {
        return maxInSize_ + 0x100; /* ad hoc margin */
    }
    size_t run(void *out, const void *in, size_t inSize)
    {
        if (inSize > maxInSize_) throw cybozu::Exception("CompressorZlib:run:too large inSize") << inSize << maxInSize_;
        cybozu::MemoryOutputStream os(out, getMaxOutSize());
        cybozu::ZlibCompressorT<cybozu::MemoryOutputStream> enc(os, false, compressionLevel_ == 0 ? Z_DEFAULT_COMPRESSION : compressionLevel_);
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

