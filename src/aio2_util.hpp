#pragma once

#include "walb_types.hpp"

#include <utility>
#include <mutex>
#include <unordered_map>

#include <libaio.h>

/**
 * Thread-safe aio manager.
 */
class Aio2
{
    struct AioData {
        uint32_t key;
        int type;
        struct iocb iocb;
        off_t oft;
        size_t size;
        walb::AlignedArray buf;
        int err;

        void init(uint32_t key, int type, off_t oft, walb::AlignedArray&& buf) {
            this->key = key;
            this->type = type;
            ::memset(&iocb, 0, sizeof(iocb));
            this->oft = oft;
            this->size = buf.size();
            this->buf = std::move(buf);
            err = 0;
        }
    };

    int fd_;
    size_t queueSize_;
    io_context_t ctx_;

    using AioDataPtr = std::unique_ptr<AioData>;
    using Umap = std::unordered_map<uint, AioDataPtr>;

    mutable std::mutex mutex_;
    std::vector<AioDataPtr> submitQ_;
    Umap pendingIOs_;
    Umap completedIOs_;

    std::atomic_flag isInitialized_;
    std::atomic_flag isReleased_;
    std::atomic<uint> key_;
    std::atomic<size_t> nrIOs_;

    using AutoLock = std::lock_guard<std::mutex>;

public:
    Aio2()
        : fd_(0)
        , queueSize_(0)
        , mutex_()
        , submitQ_()
        , pendingIOs_()
        , completedIOs_()
        , isInitialized_()
        , isReleased_()
        , key_(0)
        , nrIOs_(0) {
        isInitialized_.clear();
        isReleased_.clear();
    }
    /**
     * You must call this in the thread which will run the destructor.
     */
    void init(int fd, size_t queueSize) {
        if (isInitialized_.test_and_set()) {
            throw cybozu::Exception("Aio: do not call init() more than once");
        }
        assert(fd > 0);
        fd_ = fd;
        queueSize_ = queueSize;
        int err = ::io_queue_init(queueSize_, &ctx_);
        if (err < 0) {
            throw cybozu::Exception("Aio2 init failed") << cybozu::ErrorNo(-err);
        }
    }
    ~Aio2() noexcept try {
        if (isInitialized_.test_and_set()) {
            waitAll();
            release();
        }
    } catch (...) {
    }
    uint32_t prepareRead(off_t oft, size_t size) {
        if (++nrIOs_ > queueSize_) {
            --nrIOs_;
            throw cybozu::Exception("prepareRead: queue is full");
        }
        const uint32_t key = key_++;
        AioDataPtr iop(new AioData());
        iop->init(key, 0, oft, walb::AlignedArray(size));
        ::io_prep_pread(&iop->iocb, fd_, iop->buf.data(), size, oft);
        ::memcpy(&iop->iocb.data, &key, sizeof(key));
        pushToSubmitQ(std::move(iop));
        return key;
    }
    uint32_t prepareWrite(off_t oft, walb::AlignedArray&& buf) {
        if (++nrIOs_ > queueSize_) {
            --nrIOs_;
            throw cybozu::Exception("prepareWrite: queue is full");
        }
        const uint32_t key = key_++;
        AioDataPtr iop(new AioData());
        const size_t size = buf.size();
        iop->init(key, 1, oft, std::move(buf));
        ::io_prep_pwrite(&iop->iocb, fd_, iop->buf.data(), size, oft);
        ::memcpy(&iop->iocb.data, &key, sizeof(key));
        pushToSubmitQ(std::move(iop));
        return key;
    }
    void submit() {
        std::vector<AioDataPtr> submitQ;
        {
            AutoLock lk(mutex_);
            submitQ = std::move(submitQ_);
            submitQ_.clear();
        }
        const size_t nr = submitQ.size();
        std::vector<struct iocb *> iocbs(nr);
        for (size_t i = 0; i < nr; i++) {
            iocbs[i] = &submitQ[i]->iocb;
        }
        {
            AutoLock lk(mutex_);
            for (size_t i = 0; i < nr; i++) {
                AioDataPtr iop = std::move(submitQ[i]);
                const uint32_t key = iop->key;
                pendingIOs_.emplace(key, std::move(iop));
            }
        }
        size_t done = 0;
        while (done < nr) {
            int err = ::io_submit(ctx_, nr - done, &iocbs[done]);
            if (err < 0) {
                throw cybozu::Exception("Aio submit failed") << cybozu::ErrorNo(-err);
            }
            done += err;
        }
    }
    walb::AlignedArray waitFor(uint32_t key) {
        verifyKeyExists(key);
        AioDataPtr iop;
        while (!popCompleted(key, iop)) {
            waitDetail();
        }
        verifyNoError(*iop);
        --nrIOs_;
        return std::move(iop->buf);
    }
    walb::AlignedArray waitAny(uint32_t* keyP = nullptr) {
        AioDataPtr iop;
        while (!popCompletedAny(iop)) {
            waitDetail();
        }
        verifyNoError(*iop);
        --nrIOs_;
        if (keyP) *keyP = iop->key;
        return std::move(iop->buf);
    }
private:
    void verifyKeyExists(uint32_t key) {
        AutoLock lk(mutex_);
        if (completedIOs_.find(key) == completedIOs_.cend() &&
            pendingIOs_.find(key) == pendingIOs_.cend()) {
            throw cybozu::Exception("waitFor: key not found") << key;
        }
    }
    void release() {
        if (isReleased_.test_and_set()) return;
        int err = ::io_queue_release(ctx_);
        if (err < 0) {
            throw cybozu::Exception("Aio: release failed") << cybozu::ErrorNo(-err);
        }
    }
    void pushToSubmitQ(AioDataPtr&& iop) {
        AutoLock lk(mutex_);
        submitQ_.push_back(std::move(iop));
    }
    bool popCompleted(uint32_t key, AioDataPtr& iop) {
        AutoLock lk(mutex_);
        Umap::iterator it = completedIOs_.find(key);
        if (it == completedIOs_.end()) return false;
        iop = std::move(it->second);
        assert(iop->key == key);
        completedIOs_.erase(it);
        return true;
    }
    bool popCompletedAny(AioDataPtr& iop) {
        AutoLock lk(mutex_);
        if (completedIOs_.empty()) return false;
        Umap::iterator it = completedIOs_.begin();
        iop = std::move(it->second);
        completedIOs_.erase(it);
        return true;
    }
    size_t waitDetail(size_t minNr = 1) {
        size_t maxNr = nrIOs_;
        if (maxNr < minNr) maxNr = minNr;
        std::vector<struct io_event> ioEvents(maxNr);
        int nr = ::io_getevents(ctx_, minNr, maxNr, &ioEvents[0], NULL);
        if (nr < 0) {
            throw cybozu::Exception("io_getevents failed") << cybozu::ErrorNo(-nr);
        }
        AutoLock lk(mutex_);
        for (int i = 0; i < nr; i++) {
            const uint32_t key = getKeyFromEvent(ioEvents[i]);
            Umap::iterator it = pendingIOs_.find(key);
            assert(it != pendingIOs_.end());
            AioDataPtr& iop = it->second;
            assert(iop->key == key);
            iop->err = ioEvents[i].res;
            completedIOs_.emplace(key, std::move(iop));
            pendingIOs_.erase(it);
        }
        return nr;
    }
    static uint32_t getKeyFromEvent(struct io_event &event) {
        uint32_t key;
        ::memcpy(&key, &event.obj->data, sizeof(key));
        return key;
    }
    void verifyNoError(const AioData& io) const {
        if (io.err == 0) {
            throw cybozu::util::EofError();
        }
        if (io.err < 0) {
            throw cybozu::Exception("Aio: IO failed") << io.key << cybozu::ErrorNo(-io.err);
        }
        assert(io.iocb.u.c.nbytes == static_cast<uint>(io.err));
    }
    void waitAll() {
        for (;;) {
            size_t size;
            {
                AutoLock lk(mutex_);
                size = pendingIOs_.size();
            }
            if (size == 0) break;
            try {
                waitDetail();
            } catch (...) {
                break;
            }
        }
    }
};
