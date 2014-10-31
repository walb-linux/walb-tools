#pragma once
#include "aio_util.hpp"
#include "fileio.hpp"
#include "walb_types.hpp"
#include "bdev_util.hpp"
#include "cybozu/exception.hpp"

namespace walb {

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
    size_t aheadOffset_;
    size_t readOffset_;
    size_t availableSize_;
    AlignedArray ringBuf_;
    cybozu::aio::Aio aio_;

    struct Io {
        uint32_t key;
        size_t size;
    };
    std::queue<Io> ioQ_;

    /*
     * Internal ring buffer layout.
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
     * (readOffset_ + availableSize) % bufferSize is completeOffset.
     */

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
        , aheadOffset_(0)
        , readOffset_(0)
        , availableSize_(0)
        , ringBuf_(bufferSize)
        , aio_(file_.fd(), bufferSize / pbs_)
        , ioQ_() {
        if (bufferSize < maxIoSize) {
            throw cybozu::Exception(NAME())
                << "bufferSize must be >= maxIoSize" << bufferSize << maxIoSize;
        }
        verifyAligned(devTotal_, pbs_, "bad device size");
        verifyAligned(maxIoSize_, pbs_, "bad maxIoSize");
        verifyAligned(ringBuf_.size(), pbs_, "bad bufferSize");
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
            const size_t s = readFromBuffer(ptr, size);
            ptr += s;
            size -= s;
        }
    }
private:
    void verifyAligned(uint64_t size, size_t pbs, const char *msg) const {
        assert(pbs != 0);
        if (size == 0 || size % pbs != 0) {
            throw cybozu::Exception(NAME()) << msg << size << pbs;
        }
    }
    void proceedOffset(size_t &offset, size_t value) {
        offset = (offset + value) % ringBuf_.size();
    }
    bool prepareAheadIo() {
        if (getFreeSize() < maxIoSize_) {
            /* There is not enough buffer size. */
            return false;
        }
        const size_t ioSize = decideIoSize();
        if (ioSize == 0) {
            /* Reached to the end of the device. Do nothing. */
            return false;
        }
        const uint32_t aioKey = aio_.prepareRead(devOffset_, ioSize, &ringBuf_[aheadOffset_]);
        if (aioKey == 0) {
            throw cybozu::Exception(NAME())
                << __func__ << "preapreRead failed" << devOffset_ << ioSize << aheadOffset_;
        }
        devOffset_ += ioSize;
        proceedOffset(aheadOffset_, ioSize);
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
        if (availableSize_ > 0) return;
        if (ioQ_.empty()) readAhead();
        if (ioQ_.empty()) {
            throw cybozu::Exception(NAME()) << "Reached the end of the device";
        }
        availableSize_ += waitForIo();
    }
    size_t readFromBuffer(void *data, size_t size) {
        const size_t s = std::min(size, availableSize_);
        ::memcpy(data, &ringBuf_[readOffset_], s);
        proceedOffset(readOffset_, s);
        availableSize_ -= s;
        readAhead();
        return s;
    }
    size_t getFreeSize() const {
        if (aheadOffset_ > readOffset_) {
            return readOffset_ + ringBuf_.size() - aheadOffset_;
        } else if (aheadOffset_ < readOffset_) {
            return readOffset_ - aheadOffset_;
        } else if (ioQ_.empty() && availableSize_ == 0) {
            return ringBuf_.size();
        } else {
            return 0;
        }
    }
    size_t decideIoSize() const {
        size_t s = maxIoSize_;
        /* Free buffer size. */
        s = std::min(s, getFreeSize());
        /* Internal ring buffer edge. */
        s = std::min(s, ringBuf_.size() - aheadOffset_);
        /* Block device remaining size. */
        s = std::min(s, devTotal_ - devOffset_);
        assert(s % pbs_ == 0);
        return s;
    }
};

} // namespace walb
