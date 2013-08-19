#pragma once
/**
 * @file
 * @brief compressor/uncompressor class
 * @author MITSUNARI Shigeo
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>

namespace walb {

class CompressorError : public std::exception {
    std::string str_;
public:
    CompressorError(const std::string& str) : str_(str) {}
    ~CompressorError() throw() {}
    const char *what() { return str_.c_str(); }
};

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
    Compressor(Mode mode, size_t maxInSize, size_t para = 0);
    ~Compressor() throw();
    size_t getMaxOutSize() const;
    size_t run(void *out, const void *in, size_t inSize);
private:
    Compressor(const Compressor&);
    void operator=(const Compressor&);
    Mode mode_;
    compressor_local::CompressorIF *engine_;
};

/**
 * uncompression class
 */
class Uncompressor
{
public:
    explicit Uncompressor(Compressor::Mode mode, size_t para = 0);
    ~Uncompressor() throw();
    size_t run(void *out, size_t maxOutSize, const void *in, size_t inSize);
private:
    Uncompressor(const Uncompressor&);
    void operator=(const Uncompressor&);
    Compressor::Mode mode_;
    compressor_local::UncompressorIF *engine_;
};

} //namespace walb

