#pragma once
#include <snappy.h>

struct CompressorSnappy {
    size_t maxInSize_;
    CompressorSnappy(size_t maxInSize, size_t)
		: maxInSize_(maxInSize) {}
    size_t getMaxOutSize() const
	{
		return snappy::MaxCompressedLength(maxInSize_);
	}
    size_t run(void *out, const void *in, size_t inSize)
    {
        if (inSize > maxInSize_) throw walb::CompressorError("too large inSize");
        size_t encSize;
        snappy::RawCompress((const char*)in, inSize, (char*)out, &encSize);
        return encSize;
    }
};

struct UncompressorSnappy {
    UncompressorSnappy(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
		const char *p = (const char *)in;
		size_t decSize;
		if (!snappy::GetUncompressedLength(p, inSize, &decSize)) {
			throw walb::CompressorError("snappy::GetUncompressedLength");
		}
		if (decSize > maxOutSize) {
			throw walb::CompressorError("too small maxOutSize");
		}
		if (!snappy::RawUncompress(p, inSize, (char*)out)) {
			throw walb::CompressorError("snappy::RawUncompress");
		}
        return decSize;
    }
};

