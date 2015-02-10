#pragma once
/**
 * @file
 * @brief parallel compressor/uncompressor class
 * @author MITSUNARI Shigeo
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <vector>
#include <atomic>
#include <queue>
#include <mutex>
#include <chrono>
#include <thread>
#include <condition_variable>
#include <cybozu/thread.hpp>
#include <cybozu/event.hpp>
#include "walb_diff_base.hpp"
#include "walb_diff_pack.hpp"
#include "compressor.hpp"
#include "checksum.hpp"
#include "walb_logger.hpp"

namespace walb {

namespace compressor {

using Buffer = AlignedArray;

/*
 * convert pack data
 * @param conv [in] PackCompressor or PackUncompressor
 * @param inPackTop [in] top address of pack data
 * @param maxOutSize max output size excluding pack header block.
 * @return buffer of converted pack data
 */
template<class Convertor>
Buffer g_convert(Convertor& conv, const char *inPackTop, size_t maxOutSize)
{
    const DiffPackHeader& inPack = *(const DiffPackHeader*)inPackTop;
    Buffer ret(WALB_DIFF_PACK_SIZE + maxOutSize, false);
    DiffPackHeader& outPack = *(DiffPackHeader*)ret.data();
    outPack.clear();
    const char *in = inPackTop + WALB_DIFF_PACK_SIZE;
    char *out = &ret[WALB_DIFF_PACK_SIZE];

    uint32_t outOffset = 0;
    for (int i = 0, n = inPack.n_records; i < n; i++) {
        const DiffRecord& inRecord = inPack[i];
        DiffRecord& outRecord = outPack[i];

        if (inRecord.isNormal()) {
            conv.convertRecord(out, maxOutSize - outOffset, outRecord, in, inRecord);
        } else {
            outRecord = inRecord;
        }
        outRecord.data_offset = outOffset;
        outOffset += outRecord.data_size;
        assert(outOffset <= maxOutSize);
        out += outRecord.data_size;
        in += inRecord.data_size;
    }
    outPack.n_records = inPack.n_records;
    outPack.total_size = outOffset;
    outPack.updateChecksum();
    ret.resize(WALB_DIFF_PACK_SIZE + outPack.total_size, false);
    return ret;
}

inline uint32_t calcTotalBlockNum(const walb_diff_pack& pack)
{
    uint32_t num = 0;
    for (int i = 0; i < pack.n_records; i++) {
        num += pack.record[i].io_blocks;
    }
    return num;
}

struct PackCompressorBase {
    virtual ~PackCompressorBase() {}
    virtual void convertRecord(char *out, size_t maxOutSize, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord) = 0;
    virtual Buffer convert(const char *inPackTop) = 0;
};

} // compressor

class PackCompressor : public compressor::PackCompressorBase {
    int type_;
    walb::Compressor c_;
public:
    PackCompressor(int type, size_t compressionLevel = 0)
        : type_(type), c_(type, compressionLevel)
    {
    }
    void convertRecord(char *out, size_t maxOutSize, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord)
    {
        outRecord = inRecord;
        const size_t inSize = inRecord.data_size;
        size_t encSize;
        if (c_.run(out, &encSize, maxOutSize, in, inSize) && encSize < inSize) {
            outRecord.compression_type = type_;
            outRecord.data_size = encSize;
        } else {
            // not compress
            outRecord.compression_type = WALB_DIFF_CMPR_NONE;
            ::memcpy(out, in, inSize);
        }
        outRecord.checksum = cybozu::util::calcChecksum(out, outRecord.data_size, 0);
    }
    /*
     * compress pack data
     * @param inPackTop [in] top address of pack data
     * @return buffer of compressed pack data
     */
    compressor::Buffer convert(const char *inPackTop)
    {
        const walb_diff_pack& inPack = *(const walb_diff_pack*)inPackTop;
        const size_t margin = 4096;
        return compressor::g_convert(*this, inPackTop, inPack.total_size + margin);
    }
};

class PackUncompressor : public compressor::PackCompressorBase {
    int type_;
    walb::Uncompressor d_;
public:
    PackUncompressor(int type, size_t para = 0)
        : type_(type), d_(type, para)
    {
    }
    void convertRecord(char *out, size_t maxOutSize, walb_diff_record& outRecord, const char *in, const walb_diff_record& inRecord)
    {
        outRecord = inRecord;
        const size_t inSize = inRecord.data_size;
        if (inRecord.compression_type == WALB_DIFF_CMPR_NONE) {
            if (inSize > maxOutSize) throw cybozu::Exception("PackUncompressor:convertRecord:small maxOutSize") << inSize << maxOutSize;
            ::memcpy(out, in, inSize);
            return;
        } else if (inRecord.compression_type != type_) {
            throw cybozu::Exception("PackUncompressor:convertRecord:type mismatch") << inRecord.compression_type << type_;
        }
        size_t decSize = d_.run(out, maxOutSize, in, inSize);
        outRecord.compression_type = WALB_DIFF_CMPR_NONE;
        outRecord.data_size = decSize;
        assert(decSize == outRecord.io_blocks * 512);
        outRecord.checksum = cybozu::util::calcChecksum(out, outRecord.data_size, 0);
    }
    /*
     * uncompress pack data
     * @param inPackTop [in] top address of pack data
     * @return buffer of compressed pack data
     */
    compressor::Buffer convert(const char *inPackTop)
    {
        const walb_diff_pack& inPack = *(const walb_diff_pack*)inPackTop;
        const size_t uncompressedSize = compressor::calcTotalBlockNum(inPack) * 512;
        return compressor::g_convert(*this, inPackTop, uncompressedSize);
    }
};

namespace compressor_local {

typedef std::pair<compressor::Buffer, std::exception_ptr> MaybeBuffer;

class Queue {
    const std::atomic<bool> *pq_;
    size_t maxQueSize_;
    std::queue<MaybeBuffer> q_;
    mutable std::mutex m_;
    std::condition_variable avail_;
    std::condition_variable notFull_;

    bool isAvailable(const MaybeBuffer &buf) const {
        return !buf.first.empty() || buf.second;
    }
public:
    explicit Queue(const std::atomic<bool> *pq, size_t maxQueSize) : pq_(pq), maxQueSize_(maxQueSize) {}
    /*
        allocate reserved buffer where will be stored laster and return it
        @note lock if queue is fill
    */
    MaybeBuffer *push()
    {
        std::unique_lock<std::mutex> lk(m_);
        notFull_.wait(lk, [this] { return q_.size() < maxQueSize_; });
        q_.push(MaybeBuffer());
        return &q_.back();
    }
    /*
        notify to pop() that buffer is released
    */
    void notify()
    {
        std::unique_lock<std::mutex> lk(m_);
        avail_.notify_one();
    }
    compressor::Buffer pop()
    {
        MaybeBuffer ret;
        {
            std::unique_lock<std::mutex> lk(m_);
            avail_.wait(lk, [this] {
                return (!q_.empty() && isAvailable(q_.front())) || (*pq_ && q_.empty());
            });
            if (*pq_ && q_.empty()) return compressor::Buffer();
            ret = std::move(q_.front());
            q_.pop();
            notFull_.notify_one();
        }
        if (ret.second) std::rethrow_exception(ret.second);
        return std::move(ret.first);
    }
    bool empty() const
    {
        std::lock_guard<std::mutex> lk(m_);
        return q_.empty();
    }
};

template<class Conv, class UnConv>
struct EngineT : cybozu::ThreadBase {
private:
    compressor::PackCompressorBase *e_;
    std::atomic<bool> using_;
    Queue *que_;
    cybozu::Event startEv_;
    cybozu::Event *endEv_;
    compressor::Buffer inBuf_;
    MaybeBuffer *outBuf_;
    EngineT(const EngineT&) = delete;
    void operator=(const EngineT&) = delete;
public:
    EngineT()
        : e_(nullptr)
        , using_(false)
        , que_(nullptr)
        , endEv_(nullptr)
        , outBuf_(nullptr)
    {
    }
    ~EngineT()
    {
        delete e_;
    }
    void init(bool doCompress, int type, size_t para, Queue *que, cybozu::Event *endEv)
    {
        assert(e_ == nullptr);
        if (doCompress) {
            e_ = new Conv(type, para);
        } else {
            e_ = new UnConv(type, para);
        }
        que_ = que;
        endEv_ = endEv;
        beginThread();
    }
    void threadEntry()
        try
    {
        assert(e_);
        for (;;) {
            startEv_.wait();
            /*
             * case 1 (process task): set inBuf_ and outBuf_ and wakeup().
             * case 2 (quit): just call wakeup() without setting inBuf_ and outBuf_.
             */
            if (inBuf_.empty()) {
                que_->notify();
                break;
            }
            assert(outBuf_);
            try {
                outBuf_->first = e_->convert(inBuf_.data());
            } catch (...) {
                outBuf_->second = std::current_exception();
            }
            inBuf_.clear();
            outBuf_ = nullptr;
            using_ = false;
            endEv_->wakeup();
            que_->notify();
        }
    } catch (std::exception& e) {
        printf("compress_local::Engine::threadEntry %s\n", e.what());
    }
    void wakeup()
    {
        startEv_.wakeup();
    }
    bool tryToRun(MaybeBuffer *outBuf, compressor::Buffer&& inBuf)
    {
        if (using_.exchange(true)) return false;
        inBuf_ = std::move(inBuf);
        outBuf_ = outBuf;
        startEv_.wakeup();
        return true;
    }
    bool isUsing() const { return using_; }
};

template<class Conv = PackCompressor, class UnConv = PackUncompressor>
class ConverterQueueT {
    typedef EngineT<Conv, UnConv> Engine;
    std::atomic<bool> quit_;
    std::atomic<bool> joined_;
    Queue que_;
    cybozu::Event endEv_;
    std::vector<Engine> enginePool_;
    void runEngine(MaybeBuffer *outBuf, compressor::Buffer&& inBuf)
    {
        for (;;) {
            for (Engine& e : enginePool_) {
                if (e.tryToRun(outBuf, std::move(inBuf))) return;
            }
            endEv_.wait();
        }
    }
    bool isFreeEngine() const
    {
        for (const Engine& e : enginePool_) {
            if (e.isUsing()) return false;
        }
        return true;
    }
    void Sleep1msec() const
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
public:
    ConverterQueueT(size_t maxQueueNum, size_t threadNum, bool doCompress, int type, size_t para = 0)
        : quit_(false)
        , joined_(false)
        , que_(&quit_, maxQueueNum)
        , enginePool_(threadNum)
    {
        for (Engine& e : enginePool_) {
            e.init(doCompress, type, para, &que_, &endEv_);
        }
    }
    ~ConverterQueueT()
        try
    {
        join();
    } catch (std::exception& e) {
        printf("ConverterQueue:dstr:%s\n", e.what());
        exit(1);
    } catch (...) {
        printf("ConverterQueue:dstr:unknown exception\n");
        exit(1);
    }
    /*
     * join all thread
     */
    void join()
    {
        if (joined_.exchange(true)) return;
        quit();
        while (!isFreeEngine()) {
            Sleep1msec();
        }
        while (!que_.empty()) {
            Sleep1msec();
        }
        for (Engine& e : enginePool_) {
            e.wakeup();
            e.joinThread();
        }
    }
    /*
     * start to quit all thread
     */
    void quit() {
        quit_ = true;
        que_.notify();
    }
    /*
     * push buffer and return true
     * return false if quit_
     * @param inBuf [in] not empty
     */
    bool push(compressor::Buffer&& inBuf)
    {
        if (inBuf.empty()) throw cybozu::Exception("walb:ConverterQueueT:push:inBuf is empty");
        if (quit_) return false;
        MaybeBuffer *outBuf = que_.push();
        runEngine(outBuf, std::move(inBuf)); // no throw
        return true;
    }
    /*
     * return empty buffer if quit_ and queue is empty
     * otherwise return buffer after blocking until data comes
     */
    compressor::Buffer pop()
    {
        return que_.pop();
    }
    /*
     * You must call this to notify the thread calling push()
     * because it may not detect that quit variable becomes true when queue is full.
     */
    void popAll() noexcept
    {
        for (;;) {
            try {
                if (pop().empty()) break;
            } catch (...) {
            }
        }
    }
};

/**
 * push() caller must be single-thread.
 * pop() caller must be single-thread.
 * Any thread can call quit() and join().
 */
template<class Conv = PackCompressor, class UnConv = PackUncompressor>
class ConverterQueueT2
{
    using LockGuard = std::lock_guard<std::mutex>;
    using UniqueLock = std::unique_lock<std::mutex>;

    struct Task {
        compressor::Buffer inBuf;
        compressor::Buffer outBuf;
        std::exception_ptr ep;

        bool isAvailable() const {
            return !outBuf.empty() || ep;
        }
    };

    struct Engine : cybozu::ThreadBase {
        std::mutex* m_;
        const bool* quit_;
        std::queue<Task*>* readyQ_;
        std::condition_variable *ready_, *avail_;
        std::unique_ptr<compressor::PackCompressorBase> e_;

        static constexpr const char* NAME() { return "ConverterQueue::Engine"; }
        void init(bool doCompress, int type, size_t para,
                  std::mutex* m, const bool* quit, std::queue<Task*>* readyQ,
                  std::condition_variable* ready, std::condition_variable* avail) {
            m_ = m;
            quit_ = quit;
            readyQ_ = readyQ;
            ready_ = ready;
            avail_ = avail;
            if (doCompress) {
                e_.reset(new Conv(type, para));
            } else {
                e_.reset(new UnConv(type, para));
            }
            beginThread();
        }
        void threadEntry() try {
            Task *task;
            for (;;) {
                {
                    UniqueLock lk(*m_);
                    ready_->wait(lk, [this]() { return !readyQ_->empty() || (*quit_ && readyQ_->empty()); });
                    if (*quit_ && readyQ_->empty()) break;
                    task = readyQ_->front();
                    readyQ_->pop();
                }
                try {
                    task->outBuf = e_->convert(task->inBuf.data());
                } catch (...) {
                    task->ep = std::current_exception();
                }
                {
                    LockGuard lk(*m_);
                    avail_->notify_one();
                }
            }
        } catch (std::exception& e) {
            LOGs.error() << NAME() << e.what();
            ::exit(1);
        } catch (...) {
            LOGs.error() << NAME() << "unknown error";
            ::exit(1);
        }
    };

    size_t maxQueueSize_;

    mutable std::mutex m_;
    bool quit_;
    std::queue<Task> taskQ_;
    std::queue<Task*> readyQ_;
    std::condition_variable full_, ready_, avail_;

    std::vector<Engine> enginePool_;
    std::atomic<bool> joined_;

public:
    static constexpr const char* NAME() { return "ConverterQueue"; }
    ConverterQueueT2(size_t maxQueueNum, size_t threadNum, bool doCompress, int type, size_t para = 0)
        : maxQueueSize_(maxQueueNum)
        , m_()
        , quit_(false)
        , taskQ_()
        , readyQ_()
        , full_()
        , ready_()
        , avail_()
        , enginePool_(threadNum)
        , joined_(false) {

        for (Engine& e : enginePool_) {
            e.init(doCompress, type, para, &m_, &quit_, &readyQ_, &ready_, &avail_);
        }
    }
    ~ConverterQueueT2() noexcept {
        join();
    }
    bool push(compressor::Buffer&& inBuf) {
        if (inBuf.empty()) throw cybozu::Exception(__func__) << "inBuf is empty";
        UniqueLock lk(m_);
        full_.wait(lk, [this]() { return taskQ_.size() < maxQueueSize_ || quit_; });
        if (quit_) return false;
        taskQ_.emplace();
        taskQ_.back().inBuf = std::move(inBuf);
        readyQ_.push(&taskQ_.back());
        ready_.notify_one();
        return true;
    }
    compressor::Buffer pop() {
        Task task;
        {
            UniqueLock lk(m_);
            avail_.wait(lk, [this]() {
                return (!taskQ_.empty() && taskQ_.front().isAvailable()) || (quit_ && taskQ_.empty());
            });
            if (quit_ && taskQ_.empty()) return compressor::Buffer();
            task = std::move(taskQ_.front());
            taskQ_.pop();
            full_.notify_one();
        }
        if (task.ep) std::rethrow_exception(task.ep);
        assert(!task.outBuf.empty());
        return std::move(task.outBuf);
    }
    void popAll() noexcept {
        for (;;) {
            try {
                if (pop().empty()) break;
            } catch (...) {
            }
        }
    }
    void quit() {
        LockGuard lk(m_);
        quit_ = true;
        full_.notify_all();
        ready_.notify_all();
        avail_.notify_all();
    }
    void join() noexcept try {
        if (joined_.exchange(true)) return;
        quit();
        for (Engine& e : enginePool_) {
            e.joinThread();
        }
    } catch (std::exception& e) {
        LOGs.error() << NAME() << e.what();
        ::exit(1);
    } catch (...) {
        LOGs.error() << NAME() << "unknown error.";
        ::exit(1);
    }
};

} // compressor_local

typedef compressor_local::ConverterQueueT2<> ConverterQueue;

} //namespace walb
