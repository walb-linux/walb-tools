#pragma once
/**
 * @file
 * @brief Linux Aio Utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#define _FILE_OFFSET_BITS 64

#include <vector>
#include <queue>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <string>
#include <exception>
#include <cerrno>
#include <cstdio>
#include <cassert>
#include <memory>

#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <libaio.h>

#include "linux/walb/common.h"
#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"

namespace cybozu {
namespace aio {

/**
 * Asynchronous IO wrapper.
 *
 * (1) call prepareXXX() once or more.
 * (2) call submit() to submit all prepared IOs.
 * (3) call waitFor(), waitOne(), or wait().
 *
 * You can issue up to 'queueSize' IOs concurrently.
 * This is not thread-safe class.
 *
 * Do not use prepareFlush().
 * Currently aio flush is not supported by Linux kernel.
 *
 * Thrown EofError and LibcError in waitFor()/waitOne()/wait(),
 * you can use the Aio instance continuously,
 * however, thrown other error(s),
 * the Aio instance will be no more operational.
 */
class Aio
{
private:
    enum IoType
    {
        IOTYPE_READ = 0,
        IOTYPE_WRITE = 1,
        IOTYPE_FLUSH = 2,
    };

    struct AioData
    {
        uint key;
        IoType type;
        struct iocb iocb;
        off_t oft;
        size_t size;
        char *buf;
        double beginTime;
        double endTime;
        int err;

        void init(uint key, IoType type, off_t oft, size_t size, char *buf) {
            this->key = key;
            this->type = type;
            ::memset(&iocb, 0, sizeof(iocb));
            this->oft = oft;
            this->size = size;
            this->buf = buf;
            beginTime = 0.0;
            endTime = 0.0;
            err = 0;
        }
    };

    const int fd_;
    const size_t queueSize_;
    io_context_t ctx_;

    /*
     * submitQ_ contains prepared but not submitted IOs.
     * pendingIOs_ contains submitted but not completed IOs.
     * completedIOs_ contains completed IOs.
     *   Key: aiodata.key, value: aiodata ptr.
     */
    using AioDataPtr = std::unique_ptr<AioData>;
    std::deque<AioDataPtr> submitQ_;
    using Umap = std::unordered_map<uint, AioDataPtr>;
    Umap pendingIOs_;
    Umap completedIOs_;

    /* temporal use for submit. */
    std::vector<struct iocb *> iocbs_;

    /* temporal use for wait. */
    std::vector<struct io_event> ioEvents_;

    const bool isMeasureTime_;
    bool isReleased_;
    uint key_;

    /*
     * This must be not greater than /proc/sys/fs/aio-max-nr value.
     * Otherwise, io_queue_init() will fail with EAGAIN.
     */
    static constexpr size_t MAX_AIO_REQ_NR() { return 1024; }

public:
    /**
     * @fd Opened file descripter.
     *   You must open the file/device with O_DIRECT
     *   to work it really asynchronously.
     * @queueSize queue size for aio.
     * @isMeasureTime true if you want to measure IO begein/end time.
     */
    Aio(int fd, size_t queueSize)
        : fd_(fd)
        , queueSize_(std::min(MAX_AIO_REQ_NR(), queueSize))
        , submitQ_()
        , pendingIOs_()
        , completedIOs_()
        , iocbs_(queueSize)
        , ioEvents_(queueSize)
        , isMeasureTime_(false)
        , isReleased_(false)
        , key_(1) {
        assert(fd_ >= 0);
        assert(queueSize > 0);
        const int err = ::io_queue_init(queueSize_, &ctx_);
        if (err < 0) {
            throwLibcErrorWithNo("Aio: io_queue_init failed.", -err);
        }
    }
    ~Aio() noexcept try {
        release();
    } catch (...) {
    }
    void release() {
        if (isReleased_) return;
        int err = ::io_queue_release(ctx_);
        if (err < 0) {
            throwLibcErrorWithNo("Aio: io_queue_release failed.", -err);
        }
        isReleased_ = true;
    }
    /**
     * If this returns true, the queue is full.
     * Call submit() and waitXXX() before calling additional prepareXXX().
     */
    bool isQueueFull() const {
        return submitQ_.size() + pendingIOs_.size() >= queueSize_;
    }
    size_t queueSize() const {
        return queueSize_;
    }
    size_t queueUsage() const {
        return submitQ_.size() + pendingIOs_.size();
    }
    bool empty() const {
        return submitQ_.empty() && pendingIOs_.empty() && completedIOs_.empty();
    }
    /**
     * Prepare a read IO.
     * RETURN:
     *   Unique key (non-zero) to identify the IO in success, or 0.
     */
    uint prepareRead(off_t oft, size_t size, char* buf) {
        if (isQueueFull()) return 0;
        const uint key = getKey();
        AioDataPtr iop(new AioData());
        iop->init(key, IOTYPE_READ, oft, size, buf);
        ::io_prep_pread(&iop->iocb, fd_, buf, size, oft);
        iop->iocb.data = reinterpret_cast<void *>(key);
        submitQ_.push_back(std::move(iop));
        return key;
    }
    /**
     * Prepare a write IO.
     */
    uint prepareWrite(off_t oft, size_t size, const char* buf) {
        if (isQueueFull()) return 0;
        const uint key = getKey();
        AioDataPtr iop(new AioData());
        char* buf2 = const_cast<char *>(buf);
        iop->init(key, IOTYPE_WRITE, oft, size, buf2);
        ::io_prep_pwrite(&iop->iocb, fd_, buf2, size, oft);
        iop->iocb.data = reinterpret_cast<void *>(key);
        submitQ_.push_back(std::move(iop));
        return key;
    }
    /**
     * Prepare a flush IO.
     *
     * Currently aio flush is not supported
     * by almost all filesystems and block devices.
     */
    uint prepareFlush() {
        if (isQueueFull()) return 0;
        const uint key = getKey();
        AioDataPtr iop(new AioData());
        iop->init(key, IOTYPE_FLUSH, 0, 0, nullptr);
        ::io_prep_fdsync(&iop->iocb, fd_);
        iop->iocb.data = reinterpret_cast<void *>(key);
        submitQ_.push_back(std::move(iop));
        return key;
    }
    /**
     * Submit all prepared IO(s).
     *
     * EXCEPTION:
     *   LibcError
     */
    void submit() {
        size_t nr = submitQ_.size();
        if (nr == 0) return;

        assert(iocbs_.size() >= nr);
        double beginTime = 0;
        if (isMeasureTime_) beginTime = util::getTime();
        for (size_t i = 0; i < nr; i++) {
            AioDataPtr iop = std::move(submitQ_.front());
            submitQ_.pop_front();
            iocbs_[i] = &iop->iocb;
            iop->beginTime = beginTime;
            const uint key = iop->key;
            assert(pendingIOs_.find(key) == pendingIOs_.end());
            pendingIOs_.emplace(key, std::move(iop));
        }
        assert(submitQ_.empty());

        size_t done = 0;
        while (done < nr) {
            int err = ::io_submit(ctx_, nr - done, &iocbs_[done]);
            if (err < 0) {
                throwLibcErrorWithNo("Aio: io_submit failed.", -err);
            }
            done += err;
        }
    }
    /**
     * Cancel an IO.
     *
     * EXCEPTION:
     *   LibcError
     *   std::runtime_error
     */
    bool cancel(uint key) {
        /* Submit queue. */
        {
            std::deque<AioDataPtr>::iterator it = submitQ_.begin();
            while (it != submitQ_.end()) {
                AioDataPtr& iop = *it;
                if (iop->key == key) {
                    submitQ_.erase(it);
                    return true;
                }
                ++it;
            }
        }

        /* Pending IOs. */
        {
            Umap::iterator it = pendingIOs_.find(key);
            if (it != pendingIOs_.end()) {
                AioDataPtr& iop = it->second;
                if (::io_cancel(ctx_, &iop->iocb, &ioEvents_[0]) == 0) {
                    pendingIOs_.erase(it);
                    return true;
                }
                return false;
            }
        }

        /* Completed IOs. */
        Umap::iterator it = completedIOs_.find(key);
        if (it != completedIOs_.end()) {
            return false;
        }

        throw RT_ERR("Aio: key not found: %u", key);
    }
    /**
     * Wait for an IO.
     *
     * Do not use wait()/waitOne() and waitFor() concurrently.
     *
     * EXCEPTION:
     *   EofError
     *   LibcError
     *   std::runtime_error
     */
    void waitFor(uint key) {
        verifyKeyExistance(key);
        AioDataPtr iop;
        while (!popCompleted(key, iop)) {
            wait_(1);
        }
        verifyNoError(*iop);
    }
    /**
     * Check a given IO has been completed or not.
     * Do not call this function for IOs before submission.
     *
     * RETURN:
     *   true if the IO has been completed.
     * EXCEPTION:
     *   std::runtime_error
     */
    bool isCompleted(uint key) {
        verifyKeyExistance(key);
        wait_(0);
        return completedIOs_.find(key) != completedIOs_.cend();
    }
    /**
     * Wait several IO(s) completed.
     *
     * @nr number of waiting IO(s). nr >= 0.
     * @queue completed key(s) will be inserted to.
     *
     * EXCEPTION:
     *   EofError
     *   LibcError
     *   std::runtime_error
     *
     * If EofError or LibcError ocurred,
     * you can not know which IO(s) failed.
     * Use waitFor() to know it.
     */
    void wait(size_t nr, std::queue<uint>& queue) {
        verifyNr(nr);
        while (completedIOs_.size() < nr) {
            wait_(nr - completedIOs_.size());
        }
        bool isEofError = false;
        bool isLibcError = false;
        while (nr > 0) {
            assert(!completedIOs_.empty());
            AioDataPtr iop;
            popAnyCompleted(iop);
            if (iop->err == 0) {
                isEofError = true;
            } else if (iop->err < 0) {
                isLibcError = true;
            }
            assert(iop->iocb.u.c.nbytes == static_cast<uint>(iop->err));
            queue.push(iop->key);
            nr--;
        }
        if (isLibcError) {
            throwLibcErrorWithNo("Aio: some IOs failed.", EIO);
        }
        if (isEofError) {
            throw util::EofError();
        }
    }
    /**
     * Wait just one IO completed.
     *
     * RETURN:
     *   completed key.
     *
     * EXCEPTION:
     *   EofError
     *   LibcError
     *   std::runtime_error
     */
    uint waitOne() {
        if (completedIOs_.empty()) wait_(1);
        AioDataPtr iop;
        popAnyCompleted(iop);
        verifyNoError(*iop);
        return iop->key;
    }
    void waitOneOrMore(std::queue<uint>& queue) {
        if (completedIOs_.empty()) wait_(1);
        AioDataPtr iop;
        while (popAnyCompleted(iop)) {
            verifyNoError(*iop);
            queue.push(iop->key);
        }
    }
private:
    /**
     * Never return 0.
     */
    uint getKey() {
        uint ret = key_;
        key_++;
        if (key_ == 0) key_++;
        assert(ret != 0);
        return ret;
    }
    /**
     * Wait IOs completion.
     * @minNr minimum number of waiting IOs. minNr <= queueSize_.
     * @return number of completed IOs.
     */
    size_t wait_(size_t minNr) {
        assert(minNr <= queueSize_);
        const int nr = ::io_getevents(ctx_, minNr, queueSize_, &ioEvents_[0], NULL);
        if (nr < 0) {
            throwLibcErrorWithNo("Aio: io_getevents failed.", -nr);
        }
        if ((size_t)nr < minNr) {
            throw RT_ERR("io_getevents returns bad value %d %u.", nr, minNr);
        }
        double endTime = 0;
        if (isMeasureTime_) endTime = util::getTime();
        for (int i = 0; i < nr; i++) {
            const uint key = getKeyFromEvent(ioEvents_[i]);
            Umap::iterator it = pendingIOs_.find(key);
            assert(it != pendingIOs_.end());
            AioDataPtr& iop = it->second;
            assert(iop->key == key);
            iop->endTime = endTime;
            iop->err = ioEvents_[i].res;
            completedIOs_.emplace(key, std::move(iop));
            pendingIOs_.erase(it);
        }
        return nr;
    }
    static uint getKeyFromEvent(struct io_event &event) {
        struct iocb &iocb = *static_cast<struct iocb *>(event.obj);
        return static_cast<uint>(reinterpret_cast<uintptr_t>(iocb.data));
    }
    /**
     * @iop poped data will be set.
     * @return true if poped.
     */
    bool popAnyCompleted(AioDataPtr& iop) {
        if (completedIOs_.empty()) return false;

        Umap::iterator it = completedIOs_.begin();
        iop = std::move(it->second);
        completedIOs_.erase(it);
        return true;
    }
    /**
     * @key IO identifier
     * @iop poped data will be set.
     * @return found AioDataPtr or nullptr.
     */
    bool popCompleted(uint key, AioDataPtr& iop) {
        if (completedIOs_.empty()) return false;
        Umap::iterator it = completedIOs_.find(key);
        if (it == completedIOs_.end()) return false;

        iop = std::move(it->second);
        assert(iop->key == key);
        completedIOs_.erase(it);
        return true;
    }
    void verifyKeyExistance(uint key) const {
        if (completedIOs_.find(key) == completedIOs_.cend() &&
            pendingIOs_.find(key) == pendingIOs_.cend()) {
            throw RT_ERR("Aio: key not found: %u", key);
        }
    }
    void verifyNr(size_t nr) const {
        if (nr > queueSize_) {
            throw RT_ERR("Aio: bad nr. (nr must be <= %zu but %zu)", queueSize_, nr);
        }
    }
    void verifyNoError(const AioData& io) const {
        if (io.err == 0) {
            throw util::EofError();
        }
        if (io.err < 0) {
            throwLibcErrorWithNo("Aio: io failed.", -io.err);
        }
        assert(io.iocb.u.c.nbytes == static_cast<uint>(io.err));
    }
};

/**
 * Zero-clear a block device.
 *
 * @fd writalbe file descriptor of a block device. O_DIRECT is required.
 * @startLb start offset to zero-clear [logical block].
 * @sizeLb size to zero-clear [logical block].
 */
inline void zeroClear(int fd, uint64_t startLb, uint64_t sizeLb)
{
    const size_t lbs = 512;
    const size_t bufSizeB = 64 * 1024; // 64KiB.
    const size_t bufSizeLb = bufSizeB / lbs;
    const size_t maxQueueSize = 64;

    std::shared_ptr<char> buf = cybozu::util::allocateBlocks<char>(lbs, bufSizeB);
    ::memset(buf.get(), 0, bufSizeB);
    Aio aio(fd, maxQueueSize);

    size_t pending = 0;
    uint64_t offLb = startLb;
    while (pending < maxQueueSize && offLb < startLb + sizeLb) {
        const size_t blks = size_t(std::min<uint64_t>(startLb + sizeLb - offLb, bufSizeLb));
        const uint32_t key = aio.prepareWrite(offLb * lbs, blks * lbs, buf.get());
        if (key == 0) throw std::runtime_error("zeroClear: prepare failed.");
        aio.submit();
        offLb += blks;
        pending++;
    }
    while (offLb < startLb + sizeLb) {
        aio.waitOne();
        pending--;
        const size_t blks = size_t(std::min<uint64_t>(startLb + sizeLb - offLb, bufSizeLb));
        const uint32_t key = aio.prepareWrite(offLb * lbs, blks * lbs, buf.get());
        if (key == 0) throw std::runtime_error("zeroClear: prepare failed.");
        aio.submit();
        offLb += blks;
        pending++;
    }
    while (pending > 0) {
        aio.waitOne();
        pending--;
    }
}

} // namespace aio
} // namespace walb
