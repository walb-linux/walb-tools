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

/**
 * push() caller must be single-thread.
 * pop() caller must be single-thread.
 * Any thread can call quit() and join().
 */
template<class Conv = PackCompressor, class UnConv = PackUncompressor>
class ConverterQueueT
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
    ConverterQueueT(size_t maxQueueNum, size_t threadNum, bool doCompress, int type, size_t para = 0)
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
    ~ConverterQueueT() noexcept {
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

typedef compressor_local::ConverterQueueT<> ConverterQueue;

} //namespace walb
