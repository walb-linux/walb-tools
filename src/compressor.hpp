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
	virtual size_t getMaxOutSize() const = 0;
	virtual size_t run(void *out, const void *in, size_t inSize) = 0;
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
     * @param maxInSize [in] maximum inSize(byte) used in run
     * @param compressionLevel [in] compression level
     *                  not used for AsIs, Snappy
     *                  [0, 9] (default 6) for Zlib, Xz
     */
    Compressor(Mode mode, size_t maxInSize, size_t compressionLevel = 0);
    ~Compressor() throw();
    /**
     * return maximum out buffer size
     * out must have the size
     */
    size_t getMaxOutSize() const;
    /**
     * compress data
     * @param out [out] compressed data, this must have the getMaxOutSize() size
     * @param in [in] input data
     * @param inSize [in] input data size
     * @return compressed size
     *
     */
    size_t run(void *out, const void *in, size_t inSize);
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
     *                  memLimit(default 80MiB) for Xz
     */
    explicit Uncompressor(Compressor::Mode mode, size_t para = 0);
    ~Uncompressor() throw();
    /**
     * uncompress data
     * @param out [out] compressed data
     * @param maxOutSize [in] maximum output data size
     * @param in [in] input data
     * @param inSize [in] input data size
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

