#pragma once
/**
 * @file
 * @brief Walb log device utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <memory>
#include <queue>
#include <cstdlib>
#include <functional>
#include <type_traits>

#include "util.hpp"
#include "checksum.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "walb/super.h"
#include "walb/log_device.h"
#include "walb/log_record.h"
#include "walb_log.h"

namespace walb {
namespace device {

constexpr size_t DEFAULT_BUFFER_SIZE = 4U << 20; /* 4MiB */
constexpr size_t DEFAULT_MAX_IO_SIZE = 64U << 10; /* 64KiB. */

/**
 * WalB super sector.
 */
class SuperBlock
{
private:
    /* Log device. */
    cybozu::util::BlockDevice& bd_;
    /* Physical block size */
    const unsigned int pbs_;
    /* Super block offset in the log device [physical block]. */
    const uint64_t offset_;

    /* Super block data. */
    std::shared_ptr<uint8_t> data_;

public:
    explicit SuperBlock(cybozu::util::BlockDevice& bd)
        : bd_(bd)
        , pbs_(bd.getPhysicalBlockSize())
        , offset_(get1stSuperBlockOffsetStatic(pbs_))
        , data_(cybozu::util::allocateBlocks<uint8_t>(pbs_, pbs_)) {
#if 0
        ::printf("offset %" PRIu64 " pbs %u\n", offset_ * pbs_, pbs_);
#endif
        /* Read the superblock. */
        read();
#if 0
        print(); //debug
#endif
    }

    uint16_t getSectorType() const { return super()->sector_type; }
    uint16_t getVersion() const { return super()->version; }
    uint32_t getChecksum() const { return super()->checksum; }
    uint32_t getLogicalBlockSize() const { return super()->logical_bs; }
    uint32_t getPhysicalBlockSize() const { return super()->physical_bs; }
    uint32_t getMetadataSize() const { return super()->snapshot_metadata_size; }
    uint32_t getLogChecksumSalt() const { return super()->log_checksum_salt; }
    const uint8_t* getUuid() const { return super()->uuid; }
    const char* getName() const { return super()->name; }
    uint64_t getRingBufferSize() const { return super()->ring_buffer_size; }
    uint64_t getOldestLsid() const { return super()->oldest_lsid; }
    uint64_t getWrittenLsid() const { return super()->written_lsid; }
    uint64_t getDeviceSize() const { return super()->device_size; }

    void setOldestLsid(uint64_t oldestLsid) {
        super()->oldest_lsid = oldestLsid;
    }
    void setWrittenLsid(uint64_t writtenLsid) {
        super()->written_lsid = writtenLsid;
    }
    void setDeviceSize(uint64_t deviceSize) {
        super()->device_size = deviceSize;
    }
    void setLogChecksumSalt(uint32_t salt) {
        super()->log_checksum_salt = salt;
    }
    void setUuid(const uint8_t *uuid) {
        ::memcpy(super()->uuid, uuid, UUID_SIZE);
    }
    void updateChecksum() {
        super()->checksum = 0;
        super()->checksum = ::checksum(data_.get(), pbs_, 0);
    }

    /*
     * Offset and size.
     */

    uint64_t get1stSuperBlockOffset() const {
        return offset_;
    }

    uint64_t getMetadataOffset() const {
        return ::get_metadata_offset_2(super());
    }

    uint64_t get2ndSuperBlockOffset() const {
        UNUSED uint64_t oft = ::get_super_sector1_offset_2(super());
        assert(oft == getMetadataOffset() + getMetadataSize());
        return ::get_super_sector1_offset_2(super());
    }

    uint64_t getRingBufferOffset() const {
        uint64_t oft = ::get_ring_buffer_offset_2(super());
        assert(oft == get2ndSuperBlockOffset() + 1);
        return oft;
    }

    /**
     * Convert lsid to the position in the log device.
     *
     * @lsid target log sequence id.
     *
     * RETURN:
     *   Offset in the log device [physical block].
     */
    uint64_t getOffsetFromLsid(uint64_t lsid) const {
        if (lsid == INVALID_LSID) {
            throw RT_ERR("Invalid lsid.");
        }
        uint64_t s = getRingBufferSize();
        if (s == 0) {
            throw RT_ERR("Ring buffer size must not be 0.");
        }
        return (lsid % s) + getRingBufferOffset();
    }

    /**
     * Read super block from the log device.
     */
    void read() {
        bd_.read(offset_ * pbs_, pbs_, ptr<char>());
        if (!isValid()) {
            throw RT_ERR("super block is invalid.");
        }
    }

    /**
     * Write super block to the log device.
     */
    void write() {
        updateChecksum();
        if (!isValid()) {
            throw RT_ERR("super block is invalid.");
        }
        bd_.write(offset_ * pbs_, pbs_, ptr<char>());
    }

    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "sectorType: %u\n"
                  "version: %u\n"
                  "checksum: %u\n"
                  "lbs: %u\n"
                  "pbs: %u\n"
                  "metadataSize: %u\n"
                  "logChecksumSalt: %u\n"
                  "name: %s\n"
                  "ringBufferSize: %" PRIu64 "\n"
                  "oldestLsid: %" PRIu64 "\n"
                  "writtenLsid: %" PRIu64 "\n"
                  "deviceSize: %" PRIu64 "\n"
                  "ringBufferOffset: %" PRIu64 "\n",
                  getSectorType(),
                  getVersion(),
                  getChecksum(),
                  getLogicalBlockSize(),
                  getPhysicalBlockSize(),
                  getMetadataSize(),
                  getLogChecksumSalt(),
                  getName(),
                  getRingBufferSize(),
                  getOldestLsid(),
                  getWrittenLsid(),
                  getDeviceSize(),
                  getRingBufferOffset());
        ::fprintf(fp, "uuid: ");
        for (int i = 0; i < UUID_SIZE; i++) {
            ::fprintf(fp, "%02x", getUuid()[i]);
        }
        ::fprintf(fp, "\n");
    }

private:
    static uint64_t get1stSuperBlockOffsetStatic(unsigned int pbs) {
        return ::get_super_sector0_offset(pbs);
    }

    template <typename T>
    T *ptr() {
        return reinterpret_cast<T *>(data_.get());
    }

    template <typename T>
    const T *ptr() const {
        return reinterpret_cast<const T *>(data_.get());
    }

    struct walb_super_sector* super() {
        return ptr<struct walb_super_sector>();
    }

    const struct walb_super_sector* super() const {
        return ptr<const struct walb_super_sector>();
    }

    bool isValid(bool isChecksum = true) const {
        if (::is_valid_super_sector_raw(super(), pbs_) == 0) {
            return false;
        }
        if (isChecksum) {
            return true;
        } else {
            return ::checksum(data_.get(), pbs_, 0) == 0;
        }
    }
};

/**
 * Walb log device reader using aio.
 */
class AsyncWldevReader
{
private:
    cybozu::util::BlockDevice bd_;
    const uint32_t pbs_;
    const uint32_t bufferPb_; /* [physical block]. */
    const uint32_t maxIoPb_;
    std::shared_ptr<char> buffer_;
    SuperBlock super_;
    cybozu::aio::Aio aio_;
    uint64_t aheadLsid_;
    std::queue<std::pair<uint32_t, uint32_t> > ioQ_; /* aioKey, ioPb */
    uint32_t aheadIdx_;
    uint32_t readIdx_;
    uint32_t pendingPb_;
    uint32_t remainingPb_;
public:
    /**
     * @wldevPath walb log device path.
     * @bufferSize buffer size to read ahead [byte].
     * @maxIoSize max IO size [byte].
     */
    AsyncWldevReader(const std::string &wldevPath,
                   uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
                   uint32_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : bd_(wldevPath.c_str(), O_RDONLY | O_DIRECT)
        , pbs_(bd_.getPhysicalBlockSize())
        , bufferPb_(bufferSize / pbs_)
        , maxIoPb_(maxIoSize / pbs_)
        , buffer_(cybozu::util::allocateBlocks<char>(pbs_, pbs_, bufferPb_))
        , super_(bd_)
        , aio_(bd_.getFd(), bufferPb_)
        , aheadLsid_(0)
        , ioQ_()
        , aheadIdx_(0)
        , readIdx_(0)
        , pendingPb_(0)
        , remainingPb_(0) {
        if (bufferPb_ == 0) {
            throw RT_ERR("bufferSize must be more than physical block size.");
        }
        if (maxIoPb_ == 0) {
            throw RT_ERR("maxIoSize must be more than physical block size.");
        }
    }
    ~AsyncWldevReader() noexcept {
        while (!ioQ_.empty()) {
            try {
                uint32_t aioKey, ioPb;
                std::tie(aioKey, ioPb) = ioQ_.front();
                aio_.waitFor(aioKey);
            } catch (...) {
            }
            ioQ_.pop();
        }
    }
    uint32_t pbs() const { return pbs_; }
    uint32_t queueSize() const { return bufferPb_; }
    SuperBlock &super() { return super_; }
    /**
     * Read data.
     * @data buffer pointer to fill.
     * @pb size [physical block].
     */
    void read(char *data, size_t pb) {
        while (0 < pb) {
            readBlock(data);
            data += pbs_;
            pb--;
        }
    }
    /**
     * Invoke asynchronous read IOs to fill the buffer.
     */
    void readAhead() {
        size_t n = 0;
        while (remainingPb_ + pendingPb_ < bufferPb_) {
            prepareAheadIo();
            n++;
        }
        if (0 < n) {
            aio_.submit();
        }
    }
    /**
     * Invoke readAhead() if the buffer usage is less than a specified ratio.
     * @ratio 0.0 <= ratio <= 1.0
     */
    void readAhead(float ratio) {
        float used = remainingPb_ + pendingPb_;
        float total = bufferPb_;
        if (used / total < ratio) {
            readAhead();
        }
    }
    /**
     * Reset current IOs and start read from a lsid.
     */
    void reset(uint64_t lsid) {
        /* Wait for all pending aio(s). */
        while (!ioQ_.empty()) {
            uint32_t aioKey, ioPb;
            std::tie(aioKey, ioPb) = ioQ_.front();
            aio_.waitFor(aioKey);
            ioQ_.pop();
        }
        /* Reset indicators. */
        aheadLsid_ = lsid;
        readIdx_ = 0;
        aheadIdx_ = 0;
        pendingPb_ = 0;
        remainingPb_ = 0;
    }
private:
    char *getBuffer(size_t idx) {
        return buffer_.get() + idx * pbs_;
    }
    void plusIdx(uint32_t &idx, size_t diff) {
        idx += diff;
        assert(idx <= bufferPb_);
        if (idx == bufferPb_) {
            idx = 0;
        }
    }
    uint32_t decideIoPb() const {
        uint64_t ioPb = maxIoPb_;
        /* available buffer size. */
        uint64_t availBufferPb0 = bufferPb_ - (remainingPb_ + pendingPb_);
        ioPb = std::min(ioPb, availBufferPb0);
        /* Internal ring buffer edge. */
        uint64_t availBufferPb1 = bufferPb_ - aheadIdx_;
        ioPb = std::min(ioPb, availBufferPb1);
        /* Log device ring buffer edge. */
        uint64_t s = super_.getRingBufferSize();
        ioPb = std::min(ioPb, s - aheadLsid_ % s);
        assert(ioPb <= std::numeric_limits<uint32_t>::max());
        return ioPb;
    }
    void prepareAheadIo() {
        /* Decide IO size. */
        uint32_t ioPb = decideIoPb();
        assert(aheadIdx_ + ioPb <= bufferPb_);
        uint64_t off = super_.getOffsetFromLsid(aheadLsid_);
#ifdef DEBUG
        uint64_t off1 = super_.getOffsetFromLsid(aheadLsid_ + ioPb);
        assert(off < off1 || off1 == super_.getRingBufferOffset());
#endif

        /* Prepare an IO. */
        char *buf = getBuffer(aheadIdx_);
        uint32_t aioKey = aio_.prepareRead(off * pbs_, ioPb * pbs_, buf);
        assert(aioKey != 0);
        ioQ_.emplace(aioKey, ioPb);
        aheadLsid_ += ioPb;
        pendingPb_ += ioPb;
        plusIdx(aheadIdx_, ioPb);
    }
    void readBlock(char *data) {
        if (remainingPb_ == 0 && pendingPb_ == 0) {
            readAhead();
        }
        if (remainingPb_ == 0) {
            assert(0 < pendingPb_);
            assert(!ioQ_.empty());
            uint32_t aioKey, ioPb;
            std::tie(aioKey, ioPb) = ioQ_.front();
            ioQ_.pop();
            aio_.waitFor(aioKey);
            remainingPb_ = ioPb;
            assert(ioPb <= pendingPb_);
            pendingPb_ -= ioPb;
        }
        assert(0 < remainingPb_);
        ::memcpy(data, getBuffer(readIdx_), pbs_);
        remainingPb_--;
        plusIdx(readIdx_, 1);
    }
};

}} //namespace walb::device
