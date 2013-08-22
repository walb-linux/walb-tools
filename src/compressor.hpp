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

namespace walb {

namespace compressor_local {

struct CompressorIF {
    virtual ~CompressorIF() throw() {}
    virtual size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize) = 0;
};

struct UncompressorIF {
    virtual ~UncompressorIF() throw() {}
    virtual size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize) = 0;
};

}

/**
 * compression class
 */
class Compressor
{
public:
    enum Mode {
        AsIs,
        Snappy,
        Zlib,
        Xz
    };
    /**
     * @param mode [in] select compressor mode
     * @param compressionLevel [in] compression level
     *                  not used for AsIs, Snappy
     *                  [0, 9] (default 6) for Zlib, Xz
     */
    explicit Compressor(Mode mode, size_t compressionLevel = 0);
    ~Compressor() throw();
    /**
     * compress data
     * @param out [out] compressed data
     * @param maxOutSize [in] maximum output size
     * @param in [in] input data
     * @param inSize [in] input size
     * @return compressed size
     *
     */
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize);
private:
    Compressor(const Compressor&);
    void operator=(const Compressor&);
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
    explicit Uncompressor(Compressor::Mode mode, size_t para = 0);
    ~Uncompressor() throw();
    /**
     * uncompress data
     * @param out [out] uncompressed data
     * @param maxOutSize [in] maximum output size
     * @param in [in] input compressed data
     * @param inSize [in] input size
     * @return uncompressed size
     *
     */
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize);
private:
    Uncompressor(const Uncompressor&);
    void operator=(const Uncompressor&);
    compressor_local::UncompressorIF *engine_;
};

} //namespace walb

