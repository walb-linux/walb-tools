#pragma once
/**
 * @file
 * @brief Walb log compressor.
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
namespace log {

/**
 * Compressed and uncompressed data.
 * This uses snappy only.
 *
 * Checksum is not required here
 * because wlog itself can validate itself.
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
    void moveFrom(uint32_t cmprSize, uint32_t origSize, std::vector<char> &&data) {
        assert(!data.empty());
        setSizes(cmprSize, origSize);
        data_ = std::move(data);
        check();
    }
    void copyFrom(uint32_t cmprSize, uint32_t origSize, const void *data) {
        setSizes(cmprSize, origSize);
        data_.resize(dataSize());
        ::memcpy(&data_[0], data, data_.size());
        check();
    }
    std::vector<char> moveTo() { return std::move(data_); }
    std::vector<char> copyTo() const { return data_; }
    void copyTo(void *data, size_t size) const {
        if (data_.size() != size) {
            throw RT_ERR("size differs %zu %zu.", data_.size(), size);
        }
        ::memcpy(data, &data_[0], size);
    }
    /**
     * Send data to the remote host.
     */
    void send(packet::Packet &packet) const {
        check();
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
        check();
    }
    void compressFrom(const void *data, uint32_t size) {
        data_.resize(size);
        std::vector<char> &dst = data_;
        uint32_t cSize = 0;
        try {
            cSize = compressor().run(&dst[0], size, data, size);
            dst.resize(cSize);
        } catch (cybozu::Exception &) {
            ::memcpy(&dst[0], data, size);
        }
        setSizes(cSize, size);
        check();
    }
    /**
     * @data Output buffer which size must be more than originalSize().
     */
    void uncompressTo(void *data) const {
        assert(isCompressed());
        uint32_t s0 = origSize_;
        size_t s1 = uncompressor().run(data, s0, &data_[0], data_.size());
        if (s0 != s1) {
            throw RT_ERR("uncompressed data size differ: "
                         "expected %zu real %zu.", s0, s1);
        }
    }
    CompressedData compress() const {
        assert(!isCompressed());
        CompressedData ret;
        ret.compressFrom(&data_[0], data_.size());
        return ret;
    }
    CompressedData uncompress() const {
        assert(isCompressed());
        std::vector<char> dst(origSize_);
        uncompressTo(&dst[0]);
        CompressedData ret;
        ret.moveFrom(0, origSize_, std::move(dst));
        ret.check();
        return ret;
    }
    // QQQ : different from dataSize()
    const char *getData() const { return data_.data(); }
    size_t getDataSize() const { return data_.size(); }
private:
    void check() const {
        if (origSize_ == 0) throw RT_ERR("origSize must not be 0.");
        if (dataSize() != data_.size()) {
            throw RT_ERR("data size must be %zu but really %zu."
                         , dataSize(), data_.size());
        }
    }
    void setSizes(uint32_t cmprSize, uint32_t origSize) {
        cmprSize_ = cmprSize;
        origSize_ = origSize;
        if (dataSize() == 0) throw RT_ERR("dataSize() must not be 0.");
    }
    size_t dataSize() const {
        return cmprSize_ == 0 ? origSize_ : cmprSize_;
    }
    static walb::Compressor &compressor() {
        static walb::Compressor cmpr(WALB_DIFF_CMPR_SNAPPY, 0);
        return cmpr;
    }
    static walb::Uncompressor &uncompressor() {
        static walb::Uncompressor uncmpr(WALB_DIFF_CMPR_SNAPPY);
        return uncmpr;
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

}} //namespace walb::log
