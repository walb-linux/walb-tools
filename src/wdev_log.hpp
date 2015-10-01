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
#include "bdev_reader.hpp"
#include "random.hpp"
#include "linux/walb/super.h"
#include "linux/walb/log_device.h"
#include "linux/walb/log_record.h"
#include "walb_log.h"

namespace walb {
namespace device {

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
        const uint64_t oft = ::get_super_sector1_offset_2(super());
        unusedVar(oft);
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
        init(pbs, true);
        super()->sector_type = SECTOR_TYPE_SUPER;
        super()->version = WALB_LOG_VERSION;
        super()->logical_bs = LBS;
        super()->physical_bs = pbs;
        super()->metadata_size = 0; // deprecated.
        cybozu::util::Random<uint32_t> rand;
        cybozu::Uuid uuid;
        uuid.setRand(rand);
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
        init(rhs.pbs(), false);
        data_ = rhs.data_;
    }
    /**
     * Read super block from the log device.
     */
    void read(int fd) {
        cybozu::util::File file(fd);
        init(cybozu::util::getPhysicalBlockSize(fd), false);
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
    void init(uint32_t pbs, bool doZeroClear) {
        if (!::is_valid_pbs(pbs)) {
            throw cybozu::Exception(__func__) << "invalid pbs";
        }
        pbs_ = pbs;
        offset_ = get1stSuperBlockOffsetStatic(pbs);
        data_.resize(pbs, doZeroClear);
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
    }
    SuperBlock &super() { return super_; }
    void reset(uint64_t lsid) {
        lsid_ = lsid;
        seek();
    }
    void read(void *data, size_t size) {
        assert(size % pbs_ == 0);
        readPb(data, size / pbs_);
    }
    void skip(size_t size) {
        assert(size % pbs_ == 0);
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
    const size_t pbs_;
    const size_t maxIoSize_;

    SuperBlock super_;
    cybozu::aio::Aio aio_;
    uint64_t aheadLsid_;
    RingBufferForSeqRead ringBuf_;

    struct Io
    {
        uint32_t key;
        size_t size;
    };
    std::queue<Io> ioQ_;

    static constexpr size_t DEFAULT_BUFFER_SIZE = 4U << 20; /* 4MiB */
    static constexpr size_t DEFAULT_MAX_IO_SIZE = 64U << 10; /* 64KiB. */
public:
    static constexpr const char *NAME() { return "AsyncWldevReader"; }
    /**
     * @wldevPath walb log device path.
     * @bufferSize buffer size to read ahead [byte].
     * @maxIoSize max IO size [byte].
     */
    AsyncWldevReader(cybozu::util::File &&wldevFile,
                     size_t bufferSize = DEFAULT_BUFFER_SIZE,
                     size_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : file_(std::move(wldevFile))
        , pbs_(cybozu::util::getPhysicalBlockSize(file_.fd()))
        , maxIoSize_(maxIoSize)
        , super_()
        , aio_(file_.fd(), bufferSize / pbs_ + 1)
        , aheadLsid_(0)
        , ringBuf_()
        , ioQ_() {
        assert(pbs_ != 0);
        verifyMultiple(bufferSize, pbs_, "bad bufferSize");
        verifyMultiple(maxIoSize_, pbs_, "bad maxIoSize");
        super_.read(file_.fd());
        ringBuf_.init(bufferSize);
    }
    AsyncWldevReader(const std::string &wldevPath,
                     size_t bufferSize = DEFAULT_BUFFER_SIZE,
                     size_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : AsyncWldevReader(
            cybozu::util::File(wldevPath, O_RDONLY | O_DIRECT),
            bufferSize, maxIoSize) {
    }
    ~AsyncWldevReader() noexcept {
        while (!ioQ_.empty()) {
            try {
                waitForIo();
            } catch (...) {
            }
        }
    }
    SuperBlock &super() { return super_; }
    /**
     * Reset current IOs and start read from a lsid.
     */
    void reset(uint64_t lsid) {
        /* Wait for all pending aio(s). */
        while (!ioQ_.empty()) {
            waitForIo();
        }
        /* Reset indicators. */
        aheadLsid_ = lsid;
        ringBuf_.reset();
    }
    void read(void *data, size_t size) {
        char *ptr = (char *)data;
        while (size > 0) {
            prepareReadableData();
            const size_t s = ringBuf_.read(ptr, size);
            ptr += s;
            size -= s;
            readAhead();
        }
    }
    void skip(size_t size) {
        while (size > 0) {
            prepareReadableData();
            const size_t s = ringBuf_.skip(size);
            size -= s;
            readAhead();
        }
    }
private:
    void verifyMultiple(uint64_t size, size_t pbs, const char *msg) const {
        if (size == 0 || size % pbs != 0) {
            throw cybozu::Exception(NAME()) << msg << size << pbs;
        }
    }
    size_t waitForIo() {
        assert(!ioQ_.empty());
        const Io io = ioQ_.front();
        ioQ_.pop();
        aio_.waitFor(io.key);
        return io.size;
    }
    void prepareReadableData() {
        if (ringBuf_.getReadableSize() > 0) return;
        if (ioQ_.empty()) readAhead();
        assert(!ioQ_.empty());
        ringBuf_.complete(waitForIo());
    }
    void readAhead() {
        size_t n = 0;
        while (prepareAheadIo()) n++;
        if (n > 0) aio_.submit();
    }
    bool prepareAheadIo() {
        if (aio_.isQueueFull()) return false;
        const size_t ioSize = decideIoSize();
        if (ioSize == 0) return false;
        char *ptr = ringBuf_.prepare(ioSize);

        const uint64_t offPb = super_.getOffsetFromLsid(aheadLsid_);
        const size_t ioPb = ioSize / pbs_;
#ifdef DEBUG
        const uint64_t offPb1 = super_.getOffsetFromLsid(aheadLsid_ + ioPb);
        assert(offPb < offPb1 || offPb1 == super_.getRingBufferOffset());
#endif
        const uint64_t off = offPb * pbs_;
        const uint32_t aioKey = aio_.prepareRead(off, ioSize, ptr);
        assert(aioKey > 0);
        aheadLsid_ += ioPb;
        ioQ_.push({aioKey, ioSize});
        return true;
    }
    size_t decideIoSize() const {
        if (ringBuf_.getFreeSize() < maxIoSize_) {
            /* There is not enough free space. */
            return 0;
        }
        size_t ioSize = maxIoSize_;
        /* Log device ring buffer edge. */
        uint64_t s = super_.getRingBufferSize();
        s = s - aheadLsid_ % s;
        ioSize = std::min<uint64_t>(ioSize / pbs_, s) * pbs_;
        /* Ring buffer available size. */
        return std::min(ioSize, ringBuf_.getAvailableSize());
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

inline void fillZeroToLdev(const std::string& ldevPath, uint64_t bgnLsid, uint64_t endLsid)
{
    assert(bgnLsid < endLsid);
    cybozu::util::File file(ldevPath, O_RDWR | O_DIRECT);
    SuperBlock super;
    super.read(file.fd());
    const uint32_t pbs = super.pbs();
    const uint64_t rbOff = super.getRingBufferOffset();
    const uint64_t rbSize = super.getRingBufferSize();
    const uint64_t oldestLsid = super.getOldestLsid();
    if (endLsid > oldestLsid) {
        throw cybozu::Exception("bad endLsid") << endLsid << oldestLsid;
    }
    const uint64_t writtenLsid = super.getWrittenLsid();
    if (rbSize < writtenLsid && bgnLsid < writtenLsid - rbSize) {
        throw cybozu::Exception("bad bgnLsid") << bgnLsid << writtenLsid;
    }

    size_t bufPb = MEBI / pbs;
    AlignedArray buf(bufPb * pbs);
    ::memset(buf.data(), 0, buf.size());

    uint64_t lsid = bgnLsid;
    while (lsid < endLsid) {
        const uint64_t tmpPb0 = endLsid - lsid;
        const uint64_t tmpPb1 = rbSize - (lsid % rbSize); // ring buffer end.
        const uint64_t tmpPb2 = std::min<uint64_t>(tmpPb0, tmpPb1);
        const size_t minPb = std::min<uint64_t>(bufPb, tmpPb2);
        const uint64_t offPb = lsid % rbSize + rbOff;
        file.pwrite(buf.data(), minPb * pbs, offPb * pbs);
        lsid += minPb;
    }
}

}} //namespace walb::device
