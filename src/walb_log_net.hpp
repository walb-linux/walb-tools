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
#include "compressed_data.hpp"
#include "walb_logger.hpp"

namespace walb {
namespace log {

constexpr size_t Q_SIZE = 16;

CompressedData convertToCompressedData(const BlockDataShared &blockD, bool doCompress)
{
    const unsigned int pbs = blockD.pbs();
    const size_t n = blockD.nBlocks();
    assert(0 < n);
    std::vector<char> d(n * pbs);
    for (size_t i = 0; i < n; i++) {
        ::memcpy(&d[i * pbs], blockD.get(i), pbs);
    }
    CompressedData cd;
    cd.setUncompressed(std::move(d));
    return doCompress ? cd.compress() : cd;
}

inline void convertToBlockDataVec(BlockDataVec& blockD, const CompressedData &cd, unsigned int pbs)
{
    const size_t origSize = cd.originalSize();
    assert(origSize % pbs == 0);
    std::vector<uint8_t> v;
    cd.getUncompressed(v);
    blockD.setPbs(pbs);
    blockD.moveFrom(std::move(v));
}

inline void convertToBlockDataShared(BlockDataShared& blockDS, const CompressedData &cd, unsigned int pbs)
{
    BlockDataVec blockDV;
    convertToBlockDataVec(blockDV, cd, pbs);
    blockDS.setPbs(pbs);
    const size_t n = blockDV.nBlocks();
    blockDS.resize(n);
    for (size_t i = 0; i < n; i++) {
        ::memcpy(blockDS.get(i), blockDV.get(i), pbs);
    }
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

    class SendWorker
    {
    private:
        BoundedQ &inQ_;
        packet::Packet packet_;
        Logger &logger_;
    public:
        SendWorker(BoundedQ &inQ, cybozu::Socket &sock, Logger &logger)
            : inQ_(inQ), packet_(sock), logger_(logger) {}
        void operator()() try {
            packet::StreamControl ctrl(packet_.sock());
            CompressedData cd;
            while (inQ_.pop(cd)) {
                ctrl.next();
                cd.send(packet_);
            }
            ctrl.end();
        } catch (std::exception &e) {
            handleError(e.what());
            throw;
        } catch (...) {
            handleError("unknown error");
            throw;
        }
    private:
        void handleError(const char *msg) noexcept {
            try {
                packet::StreamControl(packet_.sock()).error();
            } catch (...) {}
            logger_.error() << "SendWorker" << msg;
            inQ_.fail();
        }
    };

    cybozu::thread::ThreadRunner compressor_;
    cybozu::thread::ThreadRunner sender_;
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
        compressor_.set(CompressWorker(q0_, q1_));
        sender_.set(SendWorker(q1_, sock_, logger_));
        compressor_.start();
        sender_.start();
    }
    /**
     * You must call pushHeader(h) and n times of pushIo(),
     * where n is h.nRecords().
     */
    void pushHeader(const LogPackHeader &header) {
        assert(header.pbs() == pbs_);
        assert(header.salt() == salt_);
#ifdef DEBUG
        assert(header.isValid());
#endif
        CompressedData cd;
        cd.setUncompressed(header.rawData(), pbs_);
        q0_.push(std::move(cd));
        recIdx_ = 0;
    }
    /**
     * You must call this for discard/padding record also.
     */
    void pushIo(const LogPackHeader &header, size_t recIdx, const BlockDataShared &blockD) {
        assert(header.pbs() == pbs_);
        assert(header.salt() == salt_);
        assert(recIdx_ == recIdx);
        assert(recIdx < header.nRecords());
        const LogRecord &rec = header.record(recIdx);
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

    class RecvWorker
    {
    private:
        BoundedQ &outQ_;
        packet::Packet packet_;
        Logger &logger_;
    public:
        RecvWorker(BoundedQ &outQ, cybozu::Socket &sock, Logger &logger)
            : outQ_(outQ), packet_(sock), logger_(logger) {}
        void operator()() try {
            packet::StreamControl ctrl(packet_.sock());
            CompressedData cd;
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
        } catch (std::exception &e) {
            handleError(e.what());
            throw;
        } catch (...) {
            handleError("unknown error");
            throw;
        }
    private:
        void handleError(const char *msg) noexcept {
            logger_.error() << "RecvWorker" << msg;
            outQ_.fail();
        }
    };

    cybozu::thread::ThreadRunner receiver_;
    cybozu::thread::ThreadRunner uncompressor_;
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
        receiver_.set(RecvWorker(q0_, sock_, logger_));
        uncompressor_.set(UncompressWorker(q0_, q1_));
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
        LogPackHeader h(&header, pbs_, salt_);
        if (cd.rawSize() != pbs_) {
            throw cybozu::Exception(__func__) << "invalid pack header size" << cd.rawSize() << pbs_;
        }
        h.copyFrom(cd.rawData(), pbs_);
        if (!h.isValid()) throw std::runtime_error("Invalid pack header.");
        assert(!h.isEnd());
        recIdx_ = 0;
        return true;
    }
    /**
     * Get IO data.
     * You must call this for discard/padding record also.
     */
    void popIo(const walb_logpack_header &header, size_t recIdx, BlockDataShared &blockD) {
        assert(recIdx_ == recIdx);
        const LogPackHeader h(&header, pbs_, salt_); // QQQ
        assert(recIdx < h.nRecords());
        const LogRecord &rec = h.record(recIdx);
        if (rec.hasDataForChecksum()) {
            CompressedData cd;
            if (!q1_.pop(cd)) throw std::runtime_error("Pop IO data failed.");
            convertToBlockDataShared(blockD, cd, pbs_);
        } else {
            blockD.setPbs(pbs_);
            blockD.resize(0);
        }
        if (!isValidRecordAndBlockData(rec, blockD, h.salt())) {
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
