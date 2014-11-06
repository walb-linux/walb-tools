#pragma once
/**
 * @file
 * @brief Compressed data and workers for them
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <cassert>
#include <memory>
#include "util.hpp"
#include "packet.hpp"
#include "thread_util.hpp"
#include "compressor.hpp"

namespace walb {

namespace cmpr_local {

inline Compressor &getSnappyCompressor() {
    static Compressor cmpr(WALB_DIFF_CMPR_SNAPPY, 0);
    return cmpr;
}

inline Uncompressor &getSnappyUncompressor() {
    static Uncompressor uncmpr(WALB_DIFF_CMPR_SNAPPY);
    return uncmpr;
}

/**
 * RETURN:
 *   true when successfully compressed, false when copied.
 */
template <typename CharT>
bool compressToVec(const void *data, size_t size, std::vector<CharT> &outV)
{
    outV.resize(size * 2); // margin to encode
    size_t outSize;
    if (getSnappyCompressor().run(outV.data(), &outSize, outV.size(), data, size) && outSize < size) {
        outV.resize(outSize);
        return true;
    } else {
        ::memcpy(outV.data(), data, size);
        return false;
    }
}

/**
 * Assume uncompressed size must be outSize.
 */
template <typename CharT>
void uncompressToVec(const void *data, size_t size, std::vector<CharT> &outV, size_t outSize)
{
    outV.resize(outSize);
    const size_t s = getSnappyUncompressor().run(&outV[0], outV.size(), data, size);
    if (s != outSize) throw cybozu::Exception(__func__) << "invalid outSize" << outSize << s;
}

} // namespace cmpr_local

/**
 * Compressed and uncompressed data.
 * This uses snappy only.
 */
class CompressedData
{
private:
    uint32_t cmprSize_; /* compressed size [byte]. 0 means not compressed. */
    uint32_t origSize_; /* original size [byte]. must not be 0. */
    std::vector<char> data_;
public:
    const char *rawData() const { return &data_[0]; }
    size_t rawSize() const { return data_.size(); }
    bool isCompressed() const { return cmprSize_ != 0; }
    size_t originalSize() const { return origSize_; }
    /**
     * Send data to the remote host.
     */
    void send(packet::Packet &packet) const {
        verify();
        packet.write(cmprSize_);
        packet.write(origSize_);
        packet.write(&data_[0], data_.size());
    }
    /**
     * Receive data from the remote host.
     */
    void recv(packet::Packet &packet) {
        packet.read(cmprSize_);
        packet.read(origSize_);
        data_.resize(dataSize());
        packet.read(&data_[0], data_.size());
        verify();
    }
    void setUncompressed(std::vector<char> &&data) {
        if (data.empty()) throw cybozu::Exception(__func__) << "empty";
        setSizes(0, data.size());
        data_ = std::move(data);
        verify();
    }
    void setUncompressed(const void *data, uint32_t size) {
        if (size == 0) throw cybozu::Exception(__func__) << "empty";
        setSizes(0, size);
        data_.resize(size);
        ::memcpy(&data_[0], data, size);
        verify();
    }
    void compressFrom(const void *data, uint32_t size) {
        if (cmpr_local::compressToVec(data, size, data_)) {
            setSizes(data_.size(), size);
        } else {
            setSizes(0, size);
        }
        verify();
    }
    template <typename CharT>
    void getUncompressed(std::vector<CharT> &outV) const {
        if (isCompressed()) {
            cmpr_local::uncompressToVec(&data_[0], data_.size(), outV, origSize_);
        } else {
            outV.resize(data_.size());
            ::memcpy(&outV[0], &data_[0], outV.size());
        }
    }
    CompressedData compress() const {
        verifyUncompressed(__func__);
        CompressedData ret;
        ret.compressFrom(&data_[0], data_.size());
        return ret;
    }
    CompressedData uncompress() const {
        verifyCompressed(__func__);
        std::vector<char> dst;
        getUncompressed(dst);
        CompressedData ret;
        ret.setUncompressed(std::move(dst));
        return ret;
    }
private:
    void verify() const {
        if (origSize_ == 0) throw RT_ERR("origSize must not be 0.");
        if (dataSize() != data_.size()) {
            throw RT_ERR("data size must be %zu but really %zu."
                         , dataSize(), data_.size());
        }
    }
    void verifyCompressed(const char *msg) const {
        if (!isCompressed()) throw cybozu::Exception(msg) << "must be compressed";
    }
    void verifyUncompressed(const char *msg) const {
        if (isCompressed()) throw cybozu::Exception(msg) << "must be uncompressed";
    }
    void setSizes(uint32_t cmprSize, uint32_t origSize) {
        cmprSize_ = cmprSize;
        origSize_ = origSize;
        if (dataSize() == 0) throw RT_ERR("dataSize() must not be 0.");
    }
    size_t dataSize() const {
        return cmprSize_ == 0 ? origSize_ : cmprSize_;
    }
};

class CompressWorker
{
private:
    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData>;
    BoundedQ &inQ_; /* Uncompressed data. */
    BoundedQ &outQ_; /* Compressed data (may include uncompressed data). */
public:
    CompressWorker(BoundedQ &inQ, BoundedQ &outQ)
        : inQ_(inQ), outQ_(outQ) {}
    void operator()() try {
        CompressedData d;
        while (inQ_.pop(d)) {
            if (d.isCompressed()) {
                outQ_.push(std::move(d));
            } else {
                outQ_.push(d.compress());
            }
        }
        outQ_.sync();
    } catch (...) {
        inQ_.fail();
        outQ_.fail();
        throw;
    }
};

class UncompressWorker
{
private:
    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData>;
    BoundedQ &inQ_; /* Compressed data (may include uncompressed data). */
    BoundedQ &outQ_; /* Uncompressed data. */
public:
    UncompressWorker(BoundedQ &inQ, BoundedQ &outQ)
        : inQ_(inQ), outQ_(outQ) {}
    void operator()() try {
        CompressedData d;
        while (inQ_.pop(d)) {
            if (d.isCompressed()) {
                outQ_.push(d.uncompress());
            } else {
                outQ_.push(std::move(d));
            }
        }
        outQ_.sync();
    } catch (...) {
        inQ_.fail();
        outQ_.fail();
        throw;
    }
};

} //namespace walb
