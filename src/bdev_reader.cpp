#include "bdev_reader.hpp"

namespace walb {

size_t RingBufferForSeqRead::getFreeSize() const
{
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

char *RingBufferForSeqRead::prepare(size_t size)
{
    assert(0 < size);
    assert(size <= getAvailableSize());
    char *data = &buf_[aheadOff_];
    proceedOff(aheadOff_, size);
    if (aheadOff_ == readOff_) isFull_ = true;
    return data;
}

size_t RingBufferForSeqRead::consume(void *data, size_t size, bool doCopy)
{
    const size_t s = std::min(size, readableSize_);
    if (doCopy) {
        assert(data);
        ::memcpy(data, &buf_[readOff_], s);
    }
    proceedOff(readOff_, s);
    readableSize_ -= s;
    if (isFull_ && s > 0) isFull_ = false;
    return s;
}


size_t AsyncBdevReader::readsome(void *data, size_t size)
{
    if (size == 0) return 0;
    if (!prepareAvailableData()) return 0; // reached the end.
    size_t s = ringBuf_.read(data, size);
    readAhead(false);
    return s;
}


void AsyncBdevReader::read(void *data, size_t size)
{
    char *ptr = (char *)data;
    while (size > 0) {
        size_t s = readsome(ptr, size);
        if (s == 0) {
            throw cybozu::Exception(NAME()) << "reached the end of the device.";
        }
        ptr += s;
        size -= s;
    }
}


void AsyncBdevReader::seek(uint64_t offsetLb)
{
    while (!ioQ_.empty()) waitForIo();
    ringBuf_.reset();
    devOffset_ = offsetLb * LOGICAL_BLOCK_SIZE;
    setReadAheadLimit(0);
}


bool AsyncBdevReader::prepareAheadIo(bool force)
{
    if (aio_.isQueueFull()) return false;
    const size_t ioSize = decideIoSize();
    if (ioSize == 0) return false;
    if (!force && devOffset_ >= readAheadLimitOffset_) return false;

    char *ptr = ringBuf_.prepare(ioSize);
    const uint32_t aioKey = aio_.prepareRead(devOffset_, ioSize, ptr);
    assert(aioKey > 0);
    devOffset_ += ioSize;
    ioQ_.push({aioKey, ioSize});
    return true;
}


size_t AsyncBdevReader::waitForIo()
{
    assert(!ioQ_.empty());
    const Io io = ioQ_.front();
    ioQ_.pop();
    aio_.waitFor(io.key);
    return io.size;
}


/**
 * @return
 *   true if there is available data.
 *   false if the stream reached to the end of the device.
 */
bool AsyncBdevReader::prepareAvailableData()
{
    if (ringBuf_.getReadableSize() > 0) return true;
    if (ioQ_.empty()) readAhead(true);
    if (ioQ_.empty()) return false;
    ringBuf_.complete(waitForIo());
    return true;
}


size_t AsyncBdevReader::decideIoSize() const
{
    if (ringBuf_.getFreeSize() < maxIoSize_) {
        /* There is not enough buffer size. */
        return 0;
    }
    uint64_t s = maxIoSize_;
    /* Available size in ring buffer. */
    s = std::min<uint64_t>(s, ringBuf_.getAvailableSize());
    /* Block device remaining size. */
    s = std::min(s, devTotal_ - devOffset_);
    /* Here, 0 means the file offset reached the end of the device. */
    assert(s % pbs_ == 0);
    assert(s <= SIZE_MAX);
    return size_t(s);
}


void AsyncBdevFileReader::lseek(off_t off, int whence)
{
    const off_t curOff = ptr_->getCurrentOffset();
    off_t newOff = 0;
    if (whence == SEEK_SET) {
        if (curOff == off) return;
        newOff = off;
    } else if (whence == SEEK_CUR) {
        if (off == 0) return;
        newOff = curOff + off;
    } else {
        throw cybozu::Exception(NAME()) << "no such whence supported" << whence;
    }
    assert(newOff % LOGICAL_BLOCK_SIZE == 0);
    ptr_->seek(newOff / LOGICAL_BLOCK_SIZE);
}


} // namespace walb
