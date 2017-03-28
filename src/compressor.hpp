#pragma once
/**
 * @file
 * @brief compressor/uncompressor class
 * @author MITSUNARI Shigeo
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cybozu/exception.hpp>
#include "walb_diff.h"

namespace walb {

namespace compressor_local {

struct CompressorIF {
    virtual ~CompressorIF() throw() {}
    virtual bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize) = 0;
};

struct UncompressorIF {
    virtual ~UncompressorIF() throw() {}
    virtual size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize) = 0;
};

} } // walb::compressor_local

#include "compressor-asis.hpp"
#include "compressor-snappy.hpp"
#include "compressor-zlib.hpp"
#include "compressor-xz.hpp"

namespace walb {
/**
 * compression class
 */
class Compressor
{
public:
    /**
     * @param mode [in] select compressor mode(WALB_DIFF_CMPR_{NONE,GZIP,SNAPPY,LZMA}
     * @param compressionLevel [in] compression level
     *                  not used for AsIs, Snappy
     *                  [0, 9] (default 6) for Zlib, Xz
     */
    explicit Compressor(int mode, size_t compressionLevel = 0)
        : engine_(nullptr)
    {
        switch (mode) {
        case WALB_DIFF_CMPR_NONE:
            engine_ = new CompressorAsIs(compressionLevel);
            break;
        case WALB_DIFF_CMPR_SNAPPY:
            engine_ = new CompressorSnappy(compressionLevel);
            break;
        case WALB_DIFF_CMPR_GZIP:
            engine_ = new CompressorZlib(compressionLevel);
            break;
        case WALB_DIFF_CMPR_LZMA: \
            engine_ = new CompressorXz(compressionLevel);
            break;
        default:
            throw cybozu::Exception("Compressor:invalid mode") << mode;
        }
    }
    ~Compressor() throw()
    {
        delete engine_;
    }
    /**
     * compress data
     * @param out [out] compressed data
     * @param outSize [out] compressed size
     * @param maxOutSize [in] maximum output size
     *        it is better that maxOutSize has margin for snappy.
     * @param in [in] input data
     * @param inSize [in] input size
     * @return success
     */
    bool run(void *out, size_t *outSize, size_t maxOutSize, const void *in, size_t inSize)
    {
        return engine_->run(out, outSize, maxOutSize, in, inSize);
    }
private:
    Compressor(const Compressor&) = delete;
    void operator=(const Compressor&) = delete;
    compressor_local::CompressorIF *engine_;
};

/**
 * uncompression class
 */
class Uncompressor
{
public:
    /**
     * @param mode [in] select compressor mode
     * @param para [in] extra parameter
     *                  not used for AsIs, Snappy, Zlib
     *                  memLimit(default 16MiB) for Xz
     */
    explicit Uncompressor(int mode, size_t para = 0)
        : engine_(nullptr)
    {
        switch (mode) {
        case WALB_DIFF_CMPR_NONE:
            engine_ = new UncompressorAsIs(para);
            break;
        case WALB_DIFF_CMPR_SNAPPY:
            engine_ = new UncompressorSnappy(para);
            break;
        case WALB_DIFF_CMPR_GZIP:
            engine_ = new UncompressorZlib(para);
            break;
        case WALB_DIFF_CMPR_LZMA:
            engine_ = new UncompressorXz(para);
            break;
        default:
            throw cybozu::Exception("Uncompressor:invalid mode") << mode;
        }
    }
    ~Uncompressor() throw()
    {
        delete engine_;
    }
    /**
     * uncompress data
     * @param out [out] uncompressed data
     * @param maxOutSize [in] maximum output size
     * @param in [in] input compressed data
     * @param inSize [in] input size
     * @return uncompressed size
     *
     */
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize)
    {
        return engine_->run(out, maxOutSize, in, inSize);
    }
private:
    Uncompressor(const Uncompressor&) = delete;
    void operator=(const Uncompressor&) = delete;
    compressor_local::UncompressorIF *engine_;
};

} // walb
