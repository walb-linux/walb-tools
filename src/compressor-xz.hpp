#pragma once
/**
 * see http://users.sosdg.org/~qiyong/mxr/source/external/public-domain/xz/dist/src/liblzma/api/lzma/container.h
 * http://s-yata.jp/docs/xz-utils/
 */
#include <lzma.h>

struct CompressorXz : walb::compressor_local::CompressorIF {
    uint32_t present_;
    /* default compression level is 6 */
    CompressorXz(size_t compressionLevel) : present_(compressionLevel == 0 ? 6 : compressionLevel)
    {
        if (present_ > 9) throw cybozu::Exception("CompressorXz:bad compressionLevel") << compressionLevel;
    }
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize)
    {
//        const size_t requiredSize = lzma_stream_buffer_bound(inSize);
//        if (maxOutSize < requiredSize) throw cybozu::Exception("CompressorXz:run:small maxOutSize") << requiredSize << maxOutSize;
        lzma_allocator *allocator = NULL;
        *outSize = 0;
        lzma_ret ret = lzma_easy_buffer_encode(present_, LZMA_CHECK_CRC64, allocator,
            (const uint8_t*)in, inSize, (uint8_t*)out, outSize, maxOutSize);
        return ret == LZMA_OK;
    }
};

struct UncompressorXz : walb::compressor_local::UncompressorIF {
    uint64_t memLimit_;
    UncompressorXz(size_t memLimit) : memLimit_(memLimit) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        /* compressionLevel_ 6 requires 64MiB or a little bigger memory */
        uint64_t memlimit = memLimit_ == 0 ? 16 * 1024 * 1024 : memLimit_;
        uint32_t flags = 0;
        lzma_allocator *allocator = NULL;
        size_t in_pos = 0;
        size_t out_pos = 0;
        lzma_ret ret = lzma_stream_buffer_decode(&memlimit, flags, allocator,
            (const uint8_t*)in, &in_pos, inSize, (uint8_t*)out, &out_pos, maxOutSize);
        if (ret != LZMA_OK) {
            throw cybozu::Exception("UncompressorXz:run:lzma_stream_buffer_decode") << ret;
        }
        return out_pos;
    }
};

