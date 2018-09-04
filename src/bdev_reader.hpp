#pragma once
#include <memory>
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
    size_t getFreeSize() const;
    size_t getPendingSize() const {
        return buf_.size() - getFreeSize();
    }

    /**
     * Max size of the next contiguous memory.
     */
    size_t getAvailableSize() const {
        /* There is right edge limitation to get contiguous memory area. */
        return std::min(getFreeSize(), buf_.size() - aheadOff_);
    }
    char *prepare(size_t size);

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
    size_t consume(void *data, size_t size, bool doCopy);
};



namespace bdev_reader_local {

const size_t DEFAULT_BUFFER_SIZE = 4U << 20; /* 4MiB */
const size_t DEFAULT_MAX_IO_SIZE = 64U << 10; /* 64KiB. */

} // namesapce bdev_reader_local


/**
 * Asynchronous sequential reader of block device using O_DIRECT.
 * Minimum IO size is physical block size.
 */
class AsyncBdevReader
{
private:
    cybozu::util::File file_;
    size_t pbs_;
    uint64_t devOffset_;  // offset of the next preparing IO. [byte].
    uint64_t devTotal_;  // [byte]
    size_t maxIoSize_;  // [byte]
    uint64_t readAheadLimitOffset_; // [byte]
    RingBufferForSeqRead ringBuf_;
    cybozu::aio::Aio aio_;
    struct Io {
        uint32_t key;
        size_t size;
    };
    std::queue<Io> ioQ_;

public:
    static constexpr const char * NAME() { return "AsyncBdevReader"; }
    /**
     * @bdevPath block device path.
     * @offsetLb start offset [logical block]
     * @bufferSize buffer size to read ahead [byte].
     * @maxIoSize max IO size [byte].
     *   maxioSize <= bufferSize must be satisfied.
     */
    AsyncBdevReader(const std::string &bdevPath,
                    uint64_t offsetLb = 0,
                    size_t bufferSize = bdev_reader_local::DEFAULT_BUFFER_SIZE,
                    size_t maxIoSize = bdev_reader_local::DEFAULT_MAX_IO_SIZE)
        : file_(bdevPath, O_RDONLY | O_DIRECT)
        , pbs_(cybozu::util::getPhysicalBlockSize(file_.fd()))
        , devOffset_(offsetLb * LOGICAL_BLOCK_SIZE)
        , devTotal_(cybozu::util::getBlockDeviceSize(file_.fd()))
        , maxIoSize_(maxIoSize)
        , readAheadLimitOffset_(devOffset_)  // no read ahead is default behavior.
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
     * @size maximum read size [byte].
     */
    size_t readsome(void *data, size_t size);
    /**
     * @data buffer to store data.
     * @size exact read size [byte].
     */
    void read(void *data, size_t size);
    /**
     * Seek an appropriate position.
     * read-ahead data will be cleared.
     * readAheadLimit will be set to 0.
     *
     * @offsetLb [logical block]
     */
    void seek(uint64_t offsetLb);
    /**
     * Set read ahead limit size from current offset.
     * @size [byte]
     */
    void setReadAheadLimit(size_t size) {
        uint64_t curOff = getCurrentOffset();
        readAheadLimitOffset_ = curOff + std::min<uint64_t>(UINT64_MAX - curOff, size);
    }
    void setReadAheadUnlimited() {
        readAheadLimitOffset_ = UINT64_MAX;
    }
    /**
     * @return [bytes]
     */
    uint64_t getCurrentOffset() const {
        assert(devOffset_ >= ringBuf_.getPendingSize());
        return devOffset_ - ringBuf_.getPendingSize();
    }
private:
    void verifyMultiple(uint64_t size, size_t pbs, const char *msg) const {
        assert(pbs != 0);
        if (size == 0 || size % pbs != 0) {
            throw cybozu::Exception(NAME()) << msg << size << pbs;
        }
    }
    bool prepareAheadIo(bool force);
    void readAhead(bool force) {
        size_t n = 0;
        while (prepareAheadIo(force)) n++;
        if (n > 0) aio_.submit();
    }
    size_t waitForIo();
    bool prepareAvailableData();
    size_t decideIoSize() const;
};


/**
 * A wrapper of AsyncBdevReader
 */
class AsyncBdevFileReader
{
    std::unique_ptr<AsyncBdevReader> ptr_;

public:
    static constexpr const char* NAME() { return "AsyncBdevFileReader"; }

    AsyncBdevFileReader() = default;
    AsyncBdevFileReader(AsyncBdevFileReader&&) = default;
    AsyncBdevFileReader(const AsyncBdevFileReader&) = delete;
    AsyncBdevFileReader& operator=(AsyncBdevFileReader&&) = default;
    AsyncBdevFileReader& operator=(const AsyncBdevFileReader&) = delete;

    void open(const std::string &bdevPath,
              uint64_t offsetLb = 0,
              size_t bufferSize = bdev_reader_local::DEFAULT_BUFFER_SIZE,
              size_t maxIoSize = bdev_reader_local::DEFAULT_MAX_IO_SIZE) {
        ptr_.reset(new AsyncBdevReader(bdevPath, offsetLb, bufferSize, maxIoSize));
    }

    /**
     * Set read ahead limit to the current offset + size.
     * @size [byte]
     */
    void readAhead(size_t size) { ptr_->setReadAheadLimit(size); }
    bool seekable() const { return true; }
    size_t readsome(void *data, size_t size) { return ptr_->readsome(data, size); }
    void read(void *data, size_t size) { ptr_->read(data, size); }
    /**
     * Reset the position to read.
     * Read ahead data will be cleared.
     * readAhead will be set to 0.
     * @off [byte]
     * @whence SEEK_SET, SEEK_CUR, or SEEK_END.
     */
    void lseek(off_t off, int whence = SEEK_SET);
};


struct SyncBdevFileReader : cybozu::util::File
{
    using File::File;

    void open(const std::string &bdevPath) {
        File::open(bdevPath, O_RDONLY | O_DIRECT);
    }
    void readAhead(size_t) { /* do nothing. */ }
};

} // namespace walb
