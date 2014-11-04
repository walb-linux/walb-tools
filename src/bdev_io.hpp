#pragma once
#include "aio_util.hpp"
#include "fileio.hpp"
#include "walb_types.hpp"
#include "bdev_util.hpp"
#include "cybozu/exception.hpp"

namespace walb {

/**
 * First of all, call init().
 *
 * Steps:
 *   call s0 = getAvailableSize()
 *   if s0 is too small, you must complete and read before.
 *   prepare s1 (<= s0)
 *   call char *p = prepare(s1)
 *   fill the buffer [p, p + s1) as you like.
 *   ...
 *   call comlete(s1)
 *   call s2 = getReadableSize()
 *   prepare s3 (<= s2)
 *   call read(buf, s3) or consume(s3)
 */
class RingBufferForSeqRead
{
private:
    AlignedArray buf_;
    size_t aheadOff_;
    size_t readOff_;
    bool isFull_;
    size_t readableSize_;

    /*
     * Ring buffer layout.
     *
     * |___XXXXXXYYYYYYYYYY______|
     *     ^     ^         ^
     *     |     |         |
     *     |     |         aheadOffset_
     *     |     completeOffset
     *     readOffset_
     *
     * ___: free area.
     * XXX: completed IOs but not be read.
     * YYY: submitted IOs but not be completed.
     *
     * (readOffset_ + readableSize) % bufferSize is completeOffset.
     */

public:
    static constexpr const char *NAME() { return "RingBufferForSeqRead"; }
    void init(size_t size) {
        if (size == 0) {
            throw cybozu::Exception(NAME()) << __func__ << "size must not be 0.";
        }
        buf_.resize(size, false);
        reset();
    }
    void reset() {
        aheadOff_ = 0;
        readOff_ = 0;
        isFull_ = false;
        readableSize_ = 0;
    }
    size_t getFreeSize() const {
        if (isFull_) {
            return 0;
        } else if (aheadOff_ == readOff_) {
            return buf_.size();
        } else if (aheadOff_ > readOff_) {
            return readOff_ + buf_.size() - aheadOff_;
        } else {
            return readOff_ - aheadOff_;
        }
    }
    /**
     * Max size of the next contiguous memory.
     */
    size_t getAvailableSize() const {
        /* There is right edge limitation to get contiguous memory area. */
        return std::min(getFreeSize(), buf_.size() - aheadOff_);
    }
    char *prepare(size_t size) {
        assert(0 < size);
        assert(size <= getAvailableSize());
        char *data = &buf_[aheadOff_];
        proceedOff(aheadOff_, size);
        if (aheadOff_ == readOff_) isFull_ = true;
        return data;
    }
    void complete(size_t size) {
        readableSize_ += size;
    }
    size_t getReadableSize() const {
        return readableSize_;
    }
    size_t read(void *data, size_t size) {
        return consume(data, size, true);
    }
    size_t skip(size_t size) {
        return consume(nullptr, size, false);
    }
private:
    void proceedOff(size_t &off, size_t value) {
        off = (off + value) % buf_.size();
    }
    size_t consume(void *data, size_t size, bool doCopy) {
        const size_t s = std::min(size, readableSize_);
        if (doCopy) {
            assert(data);
            ::memcpy(data, &buf_[readOff_], s);
        }
        proceedOff(readOff_, s);
        readableSize_ -= s;
        if (isFull_) isFull_ = false;
        return s;
    }
};

/**
 * Asynchronous sequential reader of block device using O_DIRECT.
 * Minimum IO size is physical block size.
 */
class AsyncBdevReader
{
private:
    cybozu::util::File file_;
    size_t pbs_;
    uint64_t devOffset_;
    uint64_t devTotal_;
    size_t maxIoSize_;
    RingBufferForSeqRead ringBuf_;
    cybozu::aio::Aio aio_;
    struct Io {
        uint32_t key;
        size_t size;
    };
    std::queue<Io> ioQ_;

    static constexpr size_t DEFAULT_BUFFER_SIZE = 4U << 20; /* 4MiB */
    static constexpr size_t DEFAULT_MAX_IO_SIZE = 64U << 10; /* 64KiB. */
public:
    static constexpr const char * NAME() { return "AsyncBdevReader"; }
    /**
     * @bdevPath block device path.
     * @bufferSize buffer size to read ahead [byte].
     * @maxIoSize max IO size [byte].
     *   maxioSize <= bufferSize must be satisfied.
     */
    AsyncBdevReader(const std::string &bdevPath,
                    size_t bufferSize = DEFAULT_BUFFER_SIZE,
                    size_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : file_(bdevPath, O_RDONLY | O_DIRECT)
        , pbs_(cybozu::util::getPhysicalBlockSize(file_.fd()))
        , devOffset_(0)
        , devTotal_(cybozu::util::getBlockDeviceSize(file_.fd()))
        , maxIoSize_(maxIoSize)
        , ringBuf_()
        , aio_(file_.fd(), bufferSize / pbs_)
        , ioQ_() {
        if (bufferSize < maxIoSize) {
            throw cybozu::Exception(NAME())
                << "bufferSize must be >= maxIoSize" << bufferSize << maxIoSize;
        }
        verifyMultiple(devTotal_, pbs_, "bad device size");
        verifyMultiple(maxIoSize_, pbs_, "bad maxIoSize");
        verifyMultiple(bufferSize, pbs_, "bad bufferSize");
        ringBuf_.init(bufferSize);
        readAhead();
    }
    ~AsyncBdevReader() noexcept {
        while (!ioQ_.empty()) {
            try {
                waitForIo();
            } catch (...) {
            }
        }
    }
    /**
     * @data buffer to store read data.
     * @size read size [byte].
     */
    void read(void *data, size_t size) {
        char *ptr = (char *)data;
        while (size > 0) {
            prepareAvailableData();
            const size_t s = ringBuf_.read(ptr, size);
            ptr += s;
            size -= s;
            readAhead();
        }
    }
private:
    void verifyMultiple(uint64_t size, size_t pbs, const char *msg) const {
        assert(pbs != 0);
        if (size == 0 || size % pbs != 0) {
            throw cybozu::Exception(NAME()) << msg << size << pbs;
        }
    }
    bool prepareAheadIo() {
        if (aio_.isQueueFull()) return false;
        const size_t ioSize = decideIoSize();
        if (ioSize == 0) return false;

        char *ptr = ringBuf_.prepare(ioSize);
        const uint32_t aioKey = aio_.prepareRead(devOffset_, ioSize, ptr);
        assert(aioKey > 0);
        devOffset_ += ioSize;
        ioQ_.push({aioKey, ioSize});
        return true;
    }
    void readAhead() {
        size_t n = 0;
        while (prepareAheadIo()) n++;
        if (n > 0) aio_.submit();
    }
    size_t waitForIo() {
        assert(!ioQ_.empty());
        const Io io = ioQ_.front();
        ioQ_.pop();
        aio_.waitFor(io.key);
        return io.size;
    }
    void prepareAvailableData() {
        if (ringBuf_.getReadableSize() > 0) return;
        if (ioQ_.empty()) readAhead();
        if (ioQ_.empty()) {
            throw cybozu::Exception(NAME()) << "Reached the end of the device";
        }
        ringBuf_.complete(waitForIo());
    }
    size_t decideIoSize() const {
        if (ringBuf_.getFreeSize() < maxIoSize_) {
            /* There is not enough buffer size. */
            return 0;
        }
        size_t s = maxIoSize_;
        /* Available size in ring buffer. */
        s = std::min(s, ringBuf_.getAvailableSize());
        /* Block device remaining size. */
        s = std::min(s, devTotal_ - devOffset_);
        /* Here, 0 means the file offset reached the end of the device. */
        assert(s % pbs_ == 0);
        return s;
    }
};

} // namespace walb
