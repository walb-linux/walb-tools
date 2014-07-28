#pragma once
/**
 * @file
 * @brief Walb log device utilities.
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
#include "walb_log_file.hpp"
#include "walb_util.hpp"
#include "aio_util.hpp"
#include "bdev_util.hpp"
#include "random.hpp"
#include "walb/super.h"
#include "walb/log_device.h"
#include "walb/log_record.h"
#include "walb_log.h"


namespace walb {
namespace device {

constexpr size_t DEFAULT_BUFFER_SIZE = 4U << 20; /* 4MiB */
constexpr size_t DEFAULT_MAX_IO_SIZE = 64U << 10; /* 64KiB. */

namespace local {

inline void verifySizeIsMultipleOfPbs(size_t size, uint32_t pbs, const char *msg)
{
    if (size % pbs == 0) return;
    throw cybozu::Exception(msg) << "size must be multiples of pbs" << size << pbs;
}

} // namespace local

/**
 * WalB super sector.
 *
 * You should call read(), copyFrom(), or format() at first.
 */
class SuperBlock
{
private:
    /* Physical block size */
    uint32_t pbs_;
    /* Super block offset in the log device [physical block]. */
    uint64_t offset_;

    /* Super block data. */
    walb::AlignedArray data_;

public:
    uint16_t getSectorType() const { return super()->sector_type; }
    uint16_t getVersion() const { return super()->version; }
    uint32_t getChecksum() const { return super()->checksum; }
    uint32_t getLogicalBlockSize() const { return super()->logical_bs; }
    uint32_t getPhysicalBlockSize() const { return super()->physical_bs; }
    uint32_t pbs() const { return super()->physical_bs; } // alias.
    uint32_t getMetadataSize() const { return super()->metadata_size; }
    uint32_t getLogChecksumSalt() const { return super()->log_checksum_salt; }
    uint32_t salt() const { return super()->log_checksum_salt; } // alias.
    cybozu::Uuid getUuid() const { return cybozu::Uuid(super()->uuid); }
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
    void setUuid(const cybozu::Uuid &uuid) {
        uuid.copyTo(super()->uuid);
    }
    void updateChecksum() {
        super()->checksum = 0;
        super()->checksum = ::checksum(data_.data(), pbs_, 0);
    }

    /*
     * Offset and size.
     */

    uint64_t get1stSuperBlockOffset() const {
        return offset_;
    }
#if 0
    uint64_t getMetadataOffset() const {
        return ::get_metadata_offset_2(super());
    }
#endif

    uint64_t get2ndSuperBlockOffset() const {
        UNUSED uint64_t oft = ::get_super_sector1_offset_2(super());
#if 0
        assert(oft == getMetadataOffset() + getMetadataSize());
#endif
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

    void format(uint32_t pbs, uint64_t ddevLb, uint64_t ldevLb, const std::string &name) {
        init(pbs);
        super()->sector_type = SECTOR_TYPE_SUPER;
        super()->version = WALB_LOG_VERSION;
        super()->logical_bs = LBS;
        super()->physical_bs = pbs;
        super()->metadata_size = 0; // deprecated.
        cybozu::util::Random<uint32_t> rand;
        cybozu::Uuid uuid;
        rand.fill(uuid.rawData(), uuid.rawSize());
        setUuid(uuid);
        super()->log_checksum_salt = rand.get32();
        super()->ring_buffer_size = ::addr_pb(pbs, ldevLb) - ::get_ring_buffer_offset(pbs);
        super()->oldest_lsid = 0;
        super()->written_lsid = 0;
        super()->device_size = ddevLb;
        if (name.empty()) {
            super()->name[0] = '\0';
        } else {
            snprintf(super()->name, DISK_NAME_LEN, "%s", name.c_str());
        }
        if (!isValid(false)) {
            cybozu::Exception(__func__) << "invalid super block.";
        }
    }
    void copyFrom(const SuperBlock &rhs) {
        init(rhs.pbs());
        data_ = rhs.data_;
    }
    /**
     * Read super block from the log device.
     */
    void read(int fd) {
        cybozu::util::File file(fd);
        init(cybozu::util::getPhysicalBlockSize(fd));
        file.pread(data_.data(), pbs_, offset_ * pbs_);
        if (!isValid()) {
            throw RT_ERR("super block is invalid.");
        }
    }

    /**
     * Write super block to the log device.
     */
    void write(int fd) {
        updateChecksum();
        if (!isValid()) {
            throw RT_ERR("super block is invalid.");
        }
        cybozu::util::File file(fd);
        file.pwrite(data_.data(), pbs_, offset_ * pbs_);
    }
    std::string str() const {
        return cybozu::util::formatString(
            "sectorType: %u\n"
            "version: %u\n"
            "checksum: %u\n"
            "lbs: %u\n"
            "pbs: %u\n"
            "metadataSize: %u\n"
            "salt: %u\n"
            "name: %s\n"
            "ringBufferSize: %" PRIu64 "\n"
            "oldestLsid: %" PRIu64 "\n"
            "writtenLsid: %" PRIu64 "\n"
            "deviceSize: %" PRIu64 "\n"
            "ringBufferOffset: %" PRIu64 "\n"
            "uuid: %s\n"
            , getSectorType()
            , getVersion()
            , getChecksum()
            , getLogicalBlockSize()
            , pbs()
            , getMetadataSize()
            , salt()
            , getName()
            , getRingBufferSize()
            , getOldestLsid()
            , getWrittenLsid()
            , getDeviceSize()
            , getRingBufferOffset()
            , getUuid().str().c_str());
    }
    friend inline std::ostream& operator<<(std::ostream& os, const SuperBlock& super) {
        os << super.str();
        return os;
    }
    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s", str().c_str());
    }
private:
    void init(uint32_t pbs) {
        if (!::is_valid_pbs(pbs)) {
            throw cybozu::Exception(__func__) << "invalid pbs";
        }
        pbs_ = pbs;
        offset_ = get1stSuperBlockOffsetStatic(pbs);
        data_.resize(pbs);
    }
    static uint64_t get1stSuperBlockOffsetStatic(uint32_t pbs) {
        return ::get_super_sector0_offset(pbs);
    }

    walb_super_sector* super() {
        return reinterpret_cast<walb_super_sector*>(data_.data());
    }

    const walb_super_sector* super() const {
        return reinterpret_cast<const walb_super_sector*>(data_.data());
    }

    bool isValid(bool isChecksum = true) const {
        if (::is_valid_super_sector_raw(super(), pbs_) == 0) {
            return false;
        }
        if (isChecksum) {
            return true;
        } else {
            return ::checksum(data_.data(), pbs_, 0) == 0;
        }
    }
};

/**
 * Walb log device reader using synchronous read() system call.
 */
class SimpleWldevReader
{
private:
    cybozu::util::File file_;
    SuperBlock super_;
    uint32_t pbs_;
    uint64_t lsid_;
public:
    explicit SimpleWldevReader(cybozu::util::File &&file)
        : file_(std::move(file))
        , super_(), pbs_(), lsid_() {
        init();
    }
    explicit SimpleWldevReader(const std::string &wldevPath)
        : SimpleWldevReader(cybozu::util::File(wldevPath, O_RDONLY | O_DIRECT)) {
        init();
    }
    SuperBlock &super() { return super_; }
    void reset(uint64_t lsid) {
        lsid_ = lsid;
        seek();
    }
    void read(void *data, size_t size) {
        local::verifySizeIsMultipleOfPbs(size, pbs_, __func__);
        readPb(data, size / pbs_);
    }
    void skip(size_t size) {
        local::verifySizeIsMultipleOfPbs(size, pbs_, __func__);
        skipPb(size / pbs_);
    }
private:
    void init() {
        super_.read(file_.fd());
        pbs_ = super_.getPhysicalBlockSize();
        lsid_ = 0;
    }
    void readBlock(void *data) {
        file_.read(data, pbs_);
        lsid_++;
        if (lsid_ % ringBufPb() == 0) {
            seek();
        }
    }
    void seek() {
        file_.lseek(super_.getOffsetFromLsid(lsid_) * pbs_);
    }
    void verifySizePb(size_t sizePb) {
        if (sizePb >= ringBufPb()) {
            throw cybozu::Exception(__func__)
                << "too large sizePb" << sizePb << ringBufPb();
        }
    }
    uint64_t ringBufPb() const {
        return super_.getRingBufferSize();
    }
    void readPb(void *data, size_t sizePb) {
        if (sizePb == 0) return;
        verifySizePb(sizePb);

        char *p = (char *)data;
        while (sizePb > 0) {
            readBlock(p);
            p += pbs_;
            sizePb--;
        }
    }
    void skipPb(size_t sizePb) {
        if (sizePb == 0) return;
        verifySizePb(sizePb);
        lsid_ += sizePb;
        seek();
    }
};

/**
 * Walb log device reader using aio.
 */
class AsyncWldevReader
{
private:
    cybozu::util::File file_;
    const uint32_t pbs_;
    const uint32_t bufferPb_; /* [physical block]. */
    const uint32_t maxIoPb_;
    AlignedArray buffer_;
    SuperBlock super_;
    cybozu::aio::Aio aio_;
    uint64_t aheadLsid_;
    std::queue<std::pair<uint32_t, uint32_t> > ioQ_; /* aioKey, ioPb */
    uint32_t aheadIdx_;
    uint32_t readIdx_;
    uint32_t pendingPb_; /* submitted but not completed. */
    uint32_t remainingPb_; /* completed but not read (and collected). */
public:
    /**
     * @wldevPath walb log device path.
     * @bufferSize buffer size to read ahead [byte].
     * @maxIoSize max IO size [byte].
     */
    AsyncWldevReader(cybozu::util::File &&wldevFile,
                   uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
                   uint32_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : file_(std::move(wldevFile))
        , pbs_(cybozu::util::getPhysicalBlockSize(file_.fd()))
        , bufferPb_(bufferSize / pbs_)
        , maxIoPb_(maxIoSize / pbs_)
        , buffer_(bufferPb_ * pbs_)
        , super_()
        , aio_(file_.fd(), bufferPb_)
        , aheadLsid_(0)
        , ioQ_()
        , aheadIdx_(0)
        , readIdx_(0)
        , pendingPb_(0)
        , remainingPb_(0) {
        init();
    }
    AsyncWldevReader(const std::string &wldevPath,
                     uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
                     uint32_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : AsyncWldevReader(
            cybozu::util::File(wldevPath, O_RDONLY | O_DIRECT),
            bufferSize, maxIoSize) {
        init();
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
    uint32_t queueSize() const { return bufferPb_; }
    SuperBlock &super() { return super_; }
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
    void read(void *data, size_t size) {
        local::verifySizeIsMultipleOfPbs(size, pbs_, __func__);
        readPb(data, size / pbs_);
    }
    void skip(size_t size) {
        local::verifySizeIsMultipleOfPbs(size, pbs_, __func__);
        skipPb(size / pbs_);
    }
private:
    void init() {
        if (bufferPb_ == 0) {
            throw RT_ERR("bufferSize must be more than physical block size.");
        }
        if (maxIoPb_ == 0) {
            throw RT_ERR("maxIoSize must be more than physical block size.");
        }
        super_.read(file_.fd());
    }
    char *getBuffer(size_t idx) {
        return &buffer_[idx * pbs_];
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
    void prepareBlock() {
        if (remainingPb_ + pendingPb_ <= bufferPb_ / 2) {
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
    }
    void readBlock(void *data) {
        prepareBlock();
        ::memcpy(data, getBuffer(readIdx_), pbs_);
        remainingPb_--;
        plusIdx(readIdx_, 1);
    }
    void trashBlock() {
        prepareBlock();
        remainingPb_--;
        plusIdx(readIdx_, 1);
    }
    /**
     * Read data.
     * @data buffer pointer to fill.
     * @pb size [physical block].
     */
    void readPb(void *data, size_t pb) {
        char *p = (char*)data;
        while (0 < pb) {
            readBlock(p);
            p += pbs_;
            pb--;
        }
    }
    void skipPb(size_t sizePb) {
        while (sizePb > 0) {
            trashBlock();
            sizePb--;
        }
    }
};

inline void initWalbMetadata(
    int fd, uint32_t pbs, uint64_t ddevLb, uint64_t ldevLb, const std::string &name)
{
    assert(fd > 0);
    assert(pbs > 0);
    assert(ddevLb > 0);
    assert(ldevLb > 0);
    // name can be empty.

    SuperBlock super;
    super.format(pbs, ddevLb, ldevLb, name);
    super.updateChecksum();
    super.write(fd);
}

}} //namespace walb::device
