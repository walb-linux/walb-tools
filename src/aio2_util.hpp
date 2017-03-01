#pragma once

#include "walb_types.hpp"
#include "cybozu/exception.hpp"

#include <memory>
#include <mutex>
#include <unordered_map>
#include <cinttypes>
#include <atomic>

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

        void init(uint32_t key, int type, off_t oft, walb::AlignedArray&& buf);
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
    void init(int fd, size_t queueSize);

    ~Aio2() noexcept try {
        if (isInitialized_.test_and_set()) {
            waitAll();
            release();
        }
    } catch (...) {
    }
    uint32_t prepareRead(off_t oft, size_t size);
    uint32_t prepareWrite(off_t oft, walb::AlignedArray&& buf);
    void submit();
    walb::AlignedArray waitFor(uint32_t key);
    walb::AlignedArray waitAny(uint32_t* keyP = nullptr);
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
    bool popCompleted(uint32_t key, AioDataPtr& iop);
    bool popCompletedAny(AioDataPtr& iop);
    size_t waitDetail(size_t minNr = 1);
    static uint32_t getKeyFromEvent(struct io_event &event) {
        uint32_t key;
        ::memcpy(&key, &event.obj->data, sizeof(key));
        return key;
    }
    void verifyNoError(const AioData& io) const;
    void waitAll();
};
