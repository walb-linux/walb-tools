#include "compressed_data.hpp"

namespace walb {

namespace cmpr_local {

bool compressToVec(const void *data, size_t size, AlignedArray &outV)
{
    outV.resize(size * 2); // margin to encode
    size_t outSize;
    if (getSnappyCompressor().run(outV.data(), &outSize, outV.size(), data, size) && outSize < size) {
        outV.resize(outSize);
        return true;
    } else {
        ::memcpy(outV.data(), data, size);
        outV.resize(size);
        return false;
    }
}

void uncompressToVec(const void *data, size_t size, AlignedArray &outV, size_t outSize)
{
    outV.resize(outSize);
    const size_t s = getSnappyUncompressor().run(&outV[0], outV.size(), data, size);
    if (s != outSize) throw cybozu::Exception(__func__) << "invalid outSize" << outSize << s;
}

} // namespace cmpr_local

} //namespace walb
