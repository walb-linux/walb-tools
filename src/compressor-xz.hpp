#pragma once
/**
 * see http://users.sosdg.org/~qiyong/mxr/source/external/public-domain/xz/dist/src/liblzma/api/lzma/container.h
 * http://s-yata.jp/docs/xz-utils/
 */
#include <lzma.h>

struct CompressorXz : walb::compressor_local::CompressorIF {
    size_t maxInSize_;
    size_t compressionLevel_;
    CompressorXz(size_t maxInSize, size_t compressionLevel)
        : maxInSize_(maxInSize), compressionLevel_(compressionLevel)
    {
        if (compressionLevel > 9) throw walb::CompressorError("bad compressionLevel");
    }
    size_t getMaxOutSize() const
    {
        return lzma_stream_buffer_bound(maxInSize_);
    }
    size_t run(void *out, const void *in, size_t inSize)
    {
        if (inSize > maxInSize_) throw walb::CompressorError("too large inSize");
        size_t out_size = getMaxOutSize();
        /* default compression level is 6 */
        const uint32_t present = compressionLevel_ == 0 ? 6 : compressionLevel_;
        lzma_allocator *allocator = NULL;
        size_t out_pos = 0;
        lzma_ret ret = lzma_easy_buffer_encode(present, LZMA_CHECK_CRC64, allocator,
            (const uint8_t*)in, inSize, (uint8_t*)out, &out_pos, out_size);
        if (ret != LZMA_OK) {
            throw walb::CompressorError("lzma_easy_buffer_encode");
        }
        return out_pos;
    }
};

struct UncompressorXz : walb::compressor_local::UncompressorIF {
    uint64_t memLimit_;
    UncompressorXz(size_t memLimit) : memLinit_(memLimit) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        /* compressionLevel_ 6 requires 64MiB or a little bigger memory */
        uint64_t memlimit = memLimit_ == 0 ? 80 * 1024 * 1024 : memLimit_;
        uint32_t flags = 0;
        lzma_allocator *allocator = NULL;
        size_t in_pos = 0;
        size_t out_pos = 0;
        lzma_ret ret = lzma_stream_buffer_decode(&memlimit, flags, allocator,
            (const uint8_t*)in, &in_pos, inSize, (uint8_t*)out, &out_pos, maxOutSize);
        if (ret != LZMA_OK) {
            throw walb::CompressorError("lzma_easy_buffer_encode");
        }
        return out_pos;
    }
};

