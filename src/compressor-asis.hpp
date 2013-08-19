#pragma once
#include <string.h>

struct CompressorAsIs {
	size_t maxInSize_;
    CompressorAsIs(size_t maxInSize, size_t)
		: maxInSize_(maxInSize) {}
	size_t getMaxOutSize() const
    {
		return maxInSize_;
	}
    size_t run(void *out, const void *in, size_t inSize)
    {
        if (inSize > maxInSize_) throw walb::CompressorError("too large inSize");
        memcpy(out, in, inSize);
        return inSize;
    }
};

struct UncompressorAsIs {
    UncompressorAsIs(size_t) {}
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        if (inSize > maxOutSize) throw walb::CompressorError("too large inSize");
        memcpy(out, in, inSize);
        return inSize;
    }
};

