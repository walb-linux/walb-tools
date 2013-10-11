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
    void moveFrom(uint32_t cmprSize, uint32_t origSize, std::vector<char> &&data) {
        assert(!data.empty());
        setSizes(cmprSize, origSize);
        data_ = std::move(data);
        check();
    }
    void copyFrom(uint32_t cmprSize, uint32_t origSize, const char *data) {
        setSizes(cmprSize, origSize);
        data_.resize(dataSize());
        ::memcpy(&data_[0], data, data_.size());
        check();
    }
    std::vector<char> moveTo() { return std::move(data_); }
    std::vector<char> copyTo() const { return data_; }
    void copyTo(char *data, size_t size) const {
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
    CompressedData compress() const {
        assert(!isCompressed());
        const std::vector<char> &src = data_;
        std::vector<char> dst(origSize_);
        uint32_t cSize = 0;
        try {
            cSize = compressor().run(&dst[0], dst.size(), &src[0], src.size());
            dst.resize(cSize);
        } catch (cybozu::Exception &) {
            ::memcpy(&dst[0], &src[0], dst.size());
        }
        CompressedData ret;
        ret.moveFrom(cSize, origSize_, std::move(dst));
        ret.check();
        return ret;
    }
    CompressedData uncompress() const {
        assert(isCompressed());
        const std::vector<char> &src = data_;
        std::vector<char> dst(origSize_);
        size_t oSize = uncompressor().run(&dst[0], dst.size(), &src[0], src.size());
        if (oSize != origSize_) {
            throw RT_ERR("uncompressed data size differ %zu %zu."
                         , oSize, origSize_);
        }
        CompressedData ret;
        ret.moveFrom(0, origSize_, std::move(dst));
        ret.check();
        return ret;
    }
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
        static walb::Compressor cmpr(walb::Compressor::Snappy, 0);
        return cmpr;
    }
    static walb::Uncompressor &uncompressor() {
        static walb::Uncompressor uncmpr(walb::Compressor::Snappy);
        return uncmpr;
    }
};

/**
 * Compressor.
 * You can run this using cybozu::thread::ThreadRunner.
 */
class CompressWorker : public cybozu::thread::Runnable
{
private:
    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData, true>;
    BoundedQ &inQ_;
    BoundedQ &outQ_;
public:
    CompressWorker(BoundedQ &inQ, BoundedQ &outQ)
        : inQ_(inQ), outQ_(outQ) {}
    void operator()() override {
        try {
            while (!inQ_.isEnd()) {
                CompressedData d = inQ_.pop();
                if (d.isCompressed()) {
                    outQ_.push(std::move(d));
                } else {
                    outQ_.push(d.compress());
                }
            }
            outQ_.sync();
            done();
        } catch (...) {
            throwErrorLater();
            inQ_.error();
            outQ_.error();
        }
    }
};

/**
 * Uncompressor.
 * You can run this using cybozu::thread::ThreadRunner.
 */
class UncompressWorker : public cybozu::thread::Runnable
{
private:
    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData, true>;
    BoundedQ &inQ_;
    BoundedQ &outQ_;
public:
    UncompressWorker(BoundedQ &inQ, BoundedQ &outQ)
        : inQ_(inQ), outQ_(outQ) {}
    void operator()() override {
        try {
            while (!inQ_.isEnd()) {
                CompressedData d = inQ_.pop();
                if (d.isCompressed()) {
                    outQ_.push(d.uncompress());
                } else {
                    outQ_.push(std::move(d));
                }
            }
            outQ_.sync();
            done();
        } catch (...) {
            throwErrorLater();
            inQ_.error();
            outQ_.error();
        }
    }
};

}} //namespace walb::log
