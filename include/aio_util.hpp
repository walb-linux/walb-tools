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
#include <list>
#include <unordered_map>
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

#include "walb/common.h"
#include "util.hpp"
#include "fileio.hpp"
#include "memory_buffer.hpp"

namespace cybozu {
namespace aio {

enum IoType
{
    IOTYPE_READ = 0,
    IOTYPE_WRITE = 1,
    IOTYPE_FLUSH = 2,
};

/**
 * An aio data.
 */
struct AioData
{
    unsigned int key;
    IoType type;
    struct iocb iocb;
    off_t oft;
    size_t size;
    char *buf;
    double beginTime;
    double endTime;
    bool done;
    int err;
};

/**
 * Pointer to AioData.
 */
using AioDataPtr = std::shared_ptr<AioData>;

/**
 * AioData Allocator.
 */
class AioDataAllocator
{
private:
    unsigned int key_;

public:
    AioDataAllocator()
        : key_(1) {}

    AioDataPtr alloc() {
        AioDataPtr p(new AioData());
        p->key = getKey();
        return p;
    }
private:
    /**
     * Never return 0.
     */
    unsigned int getKey() {
        unsigned int ret = key_;
        if (key_ == static_cast<unsigned int>(-1)) {
            key_ += 2;
        } else {
            key_++;
        }
        return ret;
    }
};

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
    const int fd_;
    const size_t queueSize_;
    io_context_t ctx_;

    AioDataAllocator allocator_;

    using Umap = std::unordered_map<uint, AioDataPtr>;

    /* Prepared but not submitted IOs. */
    std::list<AioDataPtr> submitQ_;

    /*
     * Submitted but not completed IOs.
     * Key: aiodata.key, value: aiodata pointer.
     */
    Umap pendingIOs_;

    /*
     * Completed IOs.
     * Each aiodata->done must be true, and it must not exist in the pendingIOs_.
     */
    Umap completedIOs_;

    /* temporal use for submit. */
    std::vector<struct iocb *> iocbs_;

    /* temporal use for wait. */
    std::vector<struct io_event> ioEvents_;

    const bool isMeasureTime_;
    bool isReleased_;

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
        , queueSize_(queueSize)
        , allocator_()
        , submitQ_()
        , pendingIOs_()
        , completedIOs_()
        , iocbs_(queueSize)
        , ioEvents_(queueSize)
        , isMeasureTime_(false)
        , isReleased_(false) {
        assert(fd_ >= 0);
        assert(queueSize > 0);
        const int err = ::io_queue_init(queueSize_, &ctx_);
        if (err < 0) {
            throw util::LibcError(-err);
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
            throw util::LibcError(-err, "Aio: release failed.");
        }
        isReleased_ = true;
    }
    /**
     * If this returns true, the queue is full.
     * call submit() and waitXXX() before calling additional prepareXXX().
     */
    bool isQueueFull() const {
        return submitQ_.size() + pendingIOs_.size() >= queueSize_;
    }
    /**
     * Prepare a read IO.
     * RETURN:
     *   Unique key (non-zero) to identify the IO in success, or 0.
     */
    uint prepareRead(off_t oft, size_t size, char* buf) {
        if (isQueueFull()) return 0;

        AioDataPtr ptr = allocator_.alloc();
        assert(ptr->key != 0);
        submitQ_.push_back(ptr);
        ptr->type = IOTYPE_READ;
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = buf;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ptr->done = false;
        ptr->err = 0;
        ::io_prep_pread(&ptr->iocb, fd_, buf, size, oft);
        ptr->iocb.data = reinterpret_cast<void *>(ptr->key);
        return ptr->key;
    }

    /**
     * Prepare a write IO.
     */
    unsigned int prepareWrite(off_t oft, size_t size, const char* buf) {
        if (isQueueFull()) return 0;

        AioDataPtr ptr = allocator_.alloc();
        assert(ptr->key != 0);
        submitQ_.push_back(ptr);
        ptr->type = IOTYPE_WRITE;
        ptr->oft = oft;
        ptr->size = size;
        ptr->buf = const_cast<char *>(buf);
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ptr->done = false;
        ptr->err = 0;
        ::io_prep_pwrite(&ptr->iocb, fd_, ptr->buf, size, oft);
        ptr->iocb.data = reinterpret_cast<void *>(ptr->key);
        return ptr->key;
    }

    /**
     * Prepare a flush IO.
     *
     * Currently aio flush is not supported
     * by almost all filesystems and block devices.
     */
    unsigned int prepareFlush() {
        if (isQueueFull()) return 0;

        AioDataPtr ptr = allocator_.alloc();
        assert(ptr->key != 0);
        submitQ_.push_back(ptr);

        ptr->type = IOTYPE_FLUSH;
        ptr->oft = 0;
        ptr->size = 0;
        ptr->buf = nullptr;
        ptr->beginTime = 0.0;
        ptr->endTime = 0.0;
        ptr->done = false;
        ptr->err = 0;
        ::io_prep_fdsync(&ptr->iocb, fd_);
        ptr->iocb.data = reinterpret_cast<void *>(ptr->key);
        return ptr->key;
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
            AioDataPtr ptr = submitQ_.front();
            submitQ_.pop_front();
            iocbs_[i] = &ptr->iocb;
            ptr->beginTime = beginTime;
            assert(pendingIOs_.find(ptr->key) == pendingIOs_.end());
            pendingIOs_.emplace(ptr->key, ptr);
        }
        assert(submitQ_.empty());

        size_t done = 0;
        while (done < nr) {
            int err = ::io_submit(ctx_, nr - done, &iocbs_[done]);
            if (err < 0) {
                throw util::LibcError(-err);
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
            auto it = submitQ_.begin();
            while (it != submitQ_.end()) {
                AioDataPtr p = *it;
                if (p->key == key) {
                    submitQ_.erase(it);
                    return true;
                }
                ++it;
            }
        }

        /* Pending IOs. */
        {
            auto it = pendingIOs_.find(key);
            if (it != pendingIOs_.end()) {
                AioDataPtr p = it->second;
                struct io_event &event = ioEvents_[0];
                if (::io_cancel(ctx_, &p->iocb, &event) == 0) {
                    pendingIOs_.erase(it);
                    return true;
                }
                return false;
            }
        }

        /* Completed IOs. */
        auto it = completedIOs_.find(key);
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
    void waitFor(unsigned int key) {
        verifyKeyExistance(key);
        AioDataPtr p;
        p = popCompleted(key);
        while (!p) {
            wait_(1);
            p = popCompleted(key);
        }
        verifyNoError(*p);
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
        if (completedIOs_.size() < nr) {
            wait_(nr - completedIOs_.size());
        }
        bool isEofError = false;
        bool isLibcError = false;
        while (nr > 0) {
            assert(completedIOs_.empty());
            AioDataPtr p = popAnyCompleted();
            if (p->err == 0) {
                isEofError = true;
            } else if (p->err < 0) {
                isLibcError = true;
            }
            assert(p->iocb.u.c.nbytes == static_cast<uint>(p->err));
            queue.push(p->key);
            nr--;
        }
        if (isLibcError) {
            throw util::LibcError(EIO, "Aio: some IOs failed.");
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
     *
     * You should use waitFor() to know errors.
     */
    uint waitOne() {
        if (completedIOs_.empty()) wait_(1);
        AioDataPtr p = popAnyCompleted();
        verifyNoError(*p);
        return p->key;
    }
private:
    /**
     * Wait IOs completion.
     * @minNr minimum number of waiting IOs. minNr <= queueSize_.
     * @return number of completed IOs.
     */
    size_t wait_(size_t minNr) {
        assert(minNr <= queueSize_);
        const int nr = ::io_getevents(ctx_, minNr, queueSize_, &ioEvents_[0], NULL);
        if (nr < 0) {
            throw util::LibcError(-nr, "io_getevents: ");
        }
        if ((size_t)nr < minNr) {
            throw RT_ERR("io_getevents returns bad value %d %u.", nr, minNr);
        }
        double endTime = 0;
        if (isMeasureTime_) endTime = util::getTime();
        for (int i = 0; i < nr; i++) {
            const uint key = getKeyFromEvent(ioEvents_[i]);
            auto it = pendingIOs_.find(key);
            assert(it != pendingIOs_.end());
            AioDataPtr p = it->second;
            assert(p->key == key);
            assert(!p->done);
            p->done = true;
            p->endTime = endTime;
            p->err = ioEvents_[i].res;
            pendingIOs_.erase(it);
            completedIOs_.emplace(key, p);
        }
        return nr;
    }
    static uint getKeyFromEvent(struct io_event &event) {
        struct iocb &iocb = *static_cast<struct iocb *>(event.obj);
        return static_cast<uint>(reinterpret_cast<uintptr_t>(iocb.data));
    }
    /**
     * completeIOs_.empty() must be false.
     * @return aio pointer.
     */
    AioDataPtr popAnyCompleted() {
        assert(!completedIOs_.empty());
        AioDataPtr p;
        {
            auto it = completedIOs_.begin();
            AioDataPtr p = it->second;
            completedIOs_.erase(it);
            assert(p.get() != nullptr);
        }
        return p;
    }
    /**
     * @return found AioDataPtr or nullptr.
     */
    AioDataPtr popCompleted(uint key) {
        if (completedIOs_.empty()) return nullptr;
        auto it = completedIOs_.find(key);
        if (it == completedIOs_.end()) return nullptr;

        AioDataPtr p = it->second;
        assert(p->key == key);
        completedIOs_.erase(it);
        return p;
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
            throw util::LibcError(-io.err, "Aio: io failed.");
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
        const size_t blks = std::min(startLb + sizeLb - offLb, bufSizeLb);
        const uint32_t key = aio.prepareWrite(offLb * lbs, blks * lbs, buf.get());
        if (key == 0) throw std::runtime_error("zeroClear: prepare failed.");
        aio.submit();
        offLb += blks;
        pending++;
    }
    while (offLb < startLb + sizeLb) {
        aio.waitOne();
        pending--;
        const size_t blks = std::min(startLb + sizeLb - offLb, bufSizeLb);
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
