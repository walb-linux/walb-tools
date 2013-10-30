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

CompressedData convertToCompressedData(const BlockData &blockD, bool doCompress)
{
    const unsigned int pbs = blockD.pbs();
    const size_t n = blockD.nBlocks();
    std::vector<char> d(n * pbs);
    for (size_t i = 0; i < n; i++) {
        ::memcpy(&d[i * pbs], blockD.get(i), pbs);
    }
    CompressedData cd;
    cd.moveFrom(0, n * pbs, std::move(d));
    return doCompress ? cd.compress() : cd;
}

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

/**
 * Walb log sender via TCP/IP connection.
 * This will send packets only, never receive packets.
 *
 * Usage:
 *   (1) call setParams() to set parameters.
 *   (2) call start() to start worker threads.
 *   (3) call pushHeader() and corresponding pushIo() multiple times.
 *   (4) repeat (3).
 *   (5) call sync() for normal finish, or error().
 */
class Sender
{
private:
    cybozu::Socket &sock_;
    Logger &logger_;
    uint32_t pbs_;
    uint32_t salt_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isError_;

    cybozu::thread::ThreadRunner compressor_;
    cybozu::thread::ThreadRunner sender_;
    size_t recIdx_;

    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData, true>;
    BoundedQ q0_; /* input to compressor_ */
    BoundedQ q1_; /* compressor_ to sender_. */

    class SendWorker : public cybozu::thread::Runnable
    {
    private:
        BoundedQ &inQ_;
        packet::Packet packet_;
        Logger &logger_;
        const std::atomic<bool> &isError_;
    public:
        SendWorker(BoundedQ &inQ, cybozu::Socket &sock, Logger &logger,
                   const std::atomic<bool> &isError)
            : inQ_(inQ), packet_(sock), logger_(logger)
            , isError_(isError) {}
        void operator()() noexcept override try {
            packet::StreamControl ctrl(packet_.sock());
            log::CompressedData cd;
            while (!inQ_.pop(cd)) {
                ctrl.next();
                cd.send(packet_);
            }
            if (isError_) ctrl.error(); else ctrl.end();
            done();
        } catch (...) {
            throwErrorLater();
            inQ_.error();
        }
    };
public:
    Sender(cybozu::Socket &sock, Logger &logger)
        : sock_(sock), logger_(logger)
        , isEnd_(false), isError_(false) {
    }
    ~Sender() noexcept {
        if (!isEnd_ && !isError_) isError_ = true;
        joinWorkers();
    }
    void setParams(uint32_t pbs, uint32_t salt) {
        pbs_ = pbs;
        salt_ = salt;
    }
    void start() {
        compressor_.set(std::make_shared<CompressWorker>(q0_, q1_));
        sender_.set(std::make_shared<SendWorker>(q1_, sock_, logger_, isError_));
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
        header.isValid();
#endif
        CompressedData cd;
        cd.copyFrom(0, pbs_, header.rawData());
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
        const RecordWrapConst rec(&header, recIdx);
#ifdef DEBUG
        const PackIoWrapConst packIo(&rec, &blockD);
        assert(packIo.isValid());
#endif
        if (rec.hasData()) {
            CompressedData cd = convertToCompressedData(blockD, false);
            q0_.push(std::move(cd));
        }
        recIdx_++;
    }
    /**
     * Send the end header block.
     */
    void sync() {
        q0_.push(generateEndHeaderBlock());
        q0_.sync();
        isEnd_ = true;
        joinWorkers();
    }
    /**
     * Notify an error and finalie the protocol.
     */
    void error() {
        isError_ = true;
        q0_.error();
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
                logger_.error("walb::log::Sender: %s.", e.what());
            } catch (...) {
                logger_.error("walb::log::Sender: unknown error.");
            }
        }
    }
    /**
     * Generate end header block.
     */
    CompressedData generateEndHeaderBlock() const {
        std::shared_ptr<uint8_t> b = cybozu::util::allocateBlocks<uint8_t>(pbs_, pbs_);
        log::PackHeaderRaw header(b, pbs_, salt_);
        header.setEnd();
        header.updateChecksum();
        log::CompressedData cd;
        cd.copyFrom(0, pbs_, b.get());
        return cd;
    }
};

/**
 * Walb log receiver via TCP/IP connection.
 *
 * Usage:
 *   (1) call popHeader() and corresponding popIo() multiple times.
 *   (2) repeat (1) while popHeader() returns true.
 *   popHeader() will throw an error if something is wrong.
 */
class Receiver
{
    /* now editing */
};

}} //namespace walb::log
