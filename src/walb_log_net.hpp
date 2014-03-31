#pragma once
/**
 * @file
 * @brief Walb log network utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <cstring>

#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "walb_log_compressor.hpp"
#include "walb_logger.hpp"

namespace walb {
namespace log {

constexpr size_t Q_SIZE = 16;

CompressedData convertToCompressedData(const BlockData &blockD, bool doCompress)
{
    const unsigned int pbs = blockD.pbs();
    const size_t n = blockD.nBlocks();
    assert(0 < n);
    std::vector<char> d(n * pbs);
    for (size_t i = 0; i < n; i++) {
        ::memcpy(&d[i * pbs], blockD.get(i), pbs);
    }
    CompressedData cd;
    cd.moveFrom(0, n * pbs, std::move(d));
    return doCompress ? cd.compress() : cd;
}

template <typename BlockDataT>
BlockDataT convertToBlockData(const CompressedData &/*cd*/, unsigned int /*pbs*/)
{
    throw std::runtime_error("convertToBlockData: not supported type.");
}

template <>
BlockDataVec convertToBlockData(const CompressedData &cd, unsigned int pbs)
{
    const size_t origSize = cd.originalSize();
    assert(origSize % pbs == 0);
    std::vector<uint8_t> v(origSize);
    if (cd.isCompressed()) {
        cd.uncompressTo(&v[0]);
    } else {
        ::memcpy(&v[0], cd.rawData(), origSize);
    }
    BlockDataVec blockD(pbs);
    blockD.moveFrom(std::move(v));
    return blockD;
}

template <>
BlockDataShared convertToBlockData(const CompressedData &cd, unsigned int pbs)
{
    BlockDataVec blockD0 = convertToBlockData<BlockDataVec>(cd, pbs);
    BlockDataShared blockD1(pbs);
    const size_t n = blockD0.nBlocks();
    blockD1.resize(n);
    for (size_t i = 0; i < n; i++) {
        ::memcpy(blockD1.get(i), blockD0.get(i), pbs);
    }
    return blockD1;
}

/**
 * Walb log sender via TCP/IP connection.
 * This will send packets only, never receive packets.
 *
 * Usage:
 *   (1) call setParams() to set parameters.
 *   (2) call start() to start worker threads.
 *   (3) call pushHeader() and corresponding pushIo() multiple times.
 *   (4) repeat (3).
 *   (5) call sync() for normal finish, or fail().
 */
class Sender
{
private:
    cybozu::Socket &sock_;
    Logger &logger_;
    uint32_t pbs_;
    uint32_t salt_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isFailed_;

    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData>;

    class SendWorker : public cybozu::thread::Runnable
    {
    private:
        BoundedQ &inQ_;
        packet::Packet packet_;
        Logger &logger_;
    public:
        SendWorker(BoundedQ &inQ, cybozu::Socket &sock, Logger &logger)
            : inQ_(inQ), packet_(sock), logger_(logger) {}
        void operator()() noexcept override try {
            packet::StreamControl ctrl(packet_.sock());
            log::CompressedData cd;
            while (inQ_.pop(cd)) {
                ctrl.next();
                cd.send(packet_);
            }
            ctrl.end();
            done();
        } catch (std::exception &e) {
            handleError(e.what());
        } catch (...) {
            handleError("unknown error");
        }
    private:
        void handleError(const char *msg) noexcept {
            try {
                packet::StreamControl(packet_.sock()).error();
            } catch (...) {}
            logger_.error() << "SendWorker" << msg;
            inQ_.fail();
            throwErrorLater();
        }
    };

    cybozu::thread::ThreadRunner<CompressWorker> compressor_;
    cybozu::thread::ThreadRunner<SendWorker> sender_;
    size_t recIdx_;

    BoundedQ q0_; /* input to compressor_ */
    BoundedQ q1_; /* compressor_ to sender_. */

public:
    Sender(cybozu::Socket &sock, Logger &logger)
        : sock_(sock), logger_(logger)
        , isEnd_(false), isFailed_(false)
        , q0_(Q_SIZE), q1_(Q_SIZE) {
    }
    ~Sender() noexcept {
        if (!isEnd_ && !isFailed_) fail();
    }
    void setParams(uint32_t pbs, uint32_t salt) {
        pbs_ = pbs;
        salt_ = salt;
    }
    void start() {
        compressor_.set(std::make_shared<CompressWorker>(q0_, q1_));
        sender_.set(std::make_shared<SendWorker>(q1_, sock_, logger_));
        compressor_.start();
        sender_.start();
    }
    /**
     * You must call pushHeader(h) and n times of pushIo(),
     * where n is h.nRecords().
     */
    void pushHeader(const PackHeader &header) {
        assert(header.pbs() == pbs_);
        assert(header.salt() == salt_);
#ifdef DEBUG
        assert(header.isValid());
#endif
        CompressedData cd;
        cd.copyFrom(0, pbs_, header.rawData());
        assert(0 < cd.originalSize());
        q0_.push(std::move(cd));
        recIdx_ = 0;
    }
    /**
     * You must call this for discard/padding record also.
     */
    void pushIo(const PackHeader &header, size_t recIdx, const BlockData &blockD) {
        assert(header.pbs() == pbs_);
        assert(header.salt() == salt_);
        assert(recIdx_ == recIdx);
        assert(recIdx < header.nRecords());
        const RecordWrap rec(&header, recIdx);
#ifdef DEBUG
        assert(isValidRecordAndBlockData(rec, blockD));
#endif
        if (rec.hasDataForChecksum()) {
            CompressedData cd = convertToCompressedData(blockD, false);
            assert(0 < cd.originalSize());
            q0_.push(std::move(cd));
        }
        recIdx_++;
    }
    /**
     * Notify the end of input.
     */
    void sync() {
        q0_.sync();
        isEnd_ = true;
        joinWorkers();
    }
    /**
     * Notify an error.
     */
    void fail() noexcept {
        isFailed_ = true;
        q0_.fail();
        q1_.fail();
        joinWorkers();
    }
private:
    /**
     * Join workers to finish.
     * You can this multiple times because ThreadRunner::join() supports it.
     */
    void joinWorkers() noexcept {
        std::function<void()> f0 = [this]() { compressor_.join(); };
        std::function<void()> f1 = [this]() { sender_.join(); };
        for (auto &f : {f0, f1}) {
            try {
                f();
            } catch (std::exception &e) {
                logger_.error() << "walb::log::Sender" << e.what();
            } catch (...) {
                logger_.error() << "walb::log::Sender:unknown error";
            }
        }
    }
};

/**
 * Walb log receiver via TCP/IP connection.
 *
 * Usage:
 *   (1) call setParams() to set parameters.
 *   (2) call start() to start worker threads.
 *   (3) call popHeader() and corresponding popIo() multiple times.
 *   (4) repeat (3) while popHeader() returns true.
 *   popHeader() will throw an error if something is wrong.
 */
class Receiver
{
private:
    cybozu::Socket &sock_;
    Logger &logger_;
    uint32_t pbs_;
    uint32_t salt_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isFailed_;

    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData>;

    class RecvWorker : public cybozu::thread::Runnable
    {
    private:
        BoundedQ &outQ_;
        packet::Packet packet_;
        Logger &logger_;
    public:
        RecvWorker(BoundedQ &outQ, cybozu::Socket &sock, Logger &logger)
            : outQ_(outQ), packet_(sock), logger_(logger) {}
        void operator()() noexcept override try {
            packet::StreamControl ctrl(packet_.sock());
            log::CompressedData cd;
            while (ctrl.isNext()) {
                cd.recv(packet_);
                outQ_.push(std::move(cd));
                ctrl.reset();
            }
            if (ctrl.isError()) {
                throw std::runtime_error("Client sent an error.");
            }
            assert(ctrl.isEnd());
            outQ_.sync();
            done();
        } catch (std::exception &e) {
            handleError(e.what());
        } catch (...) {
            handleError("unknown error");
        }
    private:
        void handleError(const char *msg) noexcept {
            logger_.error() << "RecvWorker" << msg;
            outQ_.fail();
            throwErrorLater();
        }
    };

    cybozu::thread::ThreadRunner<RecvWorker> receiver_;
    cybozu::thread::ThreadRunner<UncompressWorker> uncompressor_;
    size_t recIdx_;

    BoundedQ q0_; /* receiver_ to uncompressor_ */
    BoundedQ q1_; /* uncompressor_ to output. */

public:
    Receiver(cybozu::Socket &sock, Logger &logger)
        : sock_(sock), logger_(logger)
        , isEnd_(false), isFailed_(false)
        , q0_(Q_SIZE), q1_(Q_SIZE) {
    }
    ~Receiver() noexcept {
        if (!isEnd_ && !isFailed_) fail();
    }
    void setParams(uint32_t pbs, uint32_t salt) {
        pbs_ = pbs;
        salt_ = salt;
    }
    void start() {
        receiver_.set(std::make_shared<RecvWorker>(q0_, sock_, logger_));
        uncompressor_.set(std::make_shared<UncompressWorker>(q0_, q1_));
        receiver_.start();
        uncompressor_.start();
    }
    /**
     * You must call popHeader(h) and its corresponding popIo() n times,
     * where n is h.n_records.
     *
     * RETURN:
     *   false if the input stream has reached the end.
     */
    bool popHeader(struct walb_logpack_header &header) {
        CompressedData cd;
        if (!q1_.pop(cd)) {
            isEnd_ = true;
            joinWorkers();
            return false;
        }
        assert(!cd.isCompressed());
        PackHeaderWrap h(reinterpret_cast<uint8_t *>(&header), pbs_, salt_);
        cd.copyTo(h.rawData(), pbs_);
        if (!h.isValid()) throw std::runtime_error("Invalid pack header.");
        assert(!h.isEnd());
        recIdx_ = 0;
        return true;
    }
    /**
     * Get IO data.
     * You must call this for discard/padding record also.
     */
    template <typename BlockDataT>
    void popIo(const walb_logpack_header &header, size_t recIdx, BlockDataT &blockD) {
        assert(recIdx_ == recIdx);
        const PackHeaderWrap h((uint8_t*)&header, pbs_, salt_); // QQQ
        assert(recIdx < h.nRecords());
        const RecordWrap rec(&h, recIdx);
        if (rec.hasDataForChecksum()) {
            CompressedData cd;
            if (!q1_.pop(cd)) throw std::runtime_error("Pop IO data failed.");
            blockD = convertToBlockData<BlockDataT>(cd, pbs_);
        } else {
            blockD.setPbs(pbs_);
            blockD.resize(0);
        }
        if (!isValidRecordAndBlockData(rec, blockD)) {
            throw cybozu::Exception("popIo:Popped IO is invalid.");
        }
        recIdx_++;
    }
    /**
     * Notify an error.
     */
    void fail() noexcept {
        isFailed_ = true;
        q0_.fail();
        q1_.fail();
        joinWorkers();
    }
private:
    /**
     * Join workers to finish.
     * You can this multiple times because ThreadRunner::join() supports it.
     */
    void joinWorkers() noexcept {
        std::function<void()> f0 = [this]() { receiver_.join(); };
        std::function<void()> f1 = [this]() { uncompressor_.join(); };
        for (auto &f : {f0, f1}) {
            try {
                f();
            } catch (std::exception &e) {
                logger_.error("walb::log::Receiver: %s.", e.what());
            } catch (...) {
                logger_.error("walb::log::Receiver: unknown error.");
            }
        }
    }
};

}} //namespace walb::log
