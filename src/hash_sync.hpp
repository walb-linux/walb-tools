#pragma once
#include "thread_util.hpp"
#include "protocol.hpp"
#include "murmurhash3.hpp"
#include "walb_diff_pack.hpp"
#include "walb_logger.hpp"

namespace walb {

constexpr size_t Q_SIZE = 16;

namespace hash_sync_local {

struct Io
{
    uint64_t ioAddress; // [logical block]
    uint16_t ioBlocks; // [logical block]
    std::vector<char> data;
};

} // namespace hash_sync_local

/**
 * Walb hash-sync client helper.
 * This will receive hash arrays, send diff packs.
 *
 * Usage:
 *   (1) call start() to start worker threads.
 *   (2) call popHash(). If the returned value is true, goto (3) else (4).
 *   (3) call pushIo() if the corresponding hash is different. goto (2).
 *   (4) call sync() for successful end.
 *
 *   You can call fail() when an error ocurrs and stop all threads.
 */
class HashSyncClient
{
private:
    using HashQ = cybozu::thread::BoundedQueue<cybozu::murmurhash3::Hash>;
    using IoQ = cybozu::thread::BoundedQueue<hash_sync_local::Io>;
    using PackQ = cybozu::thread::BoundedQueue<std::vector<char> >;

    class SendPackWorker
    {
    private:
        PackQ &inQ_;
        packet::Packet packet_;
        Logger &logger_;
        static constexpr char CLS[] = "SendPackWorker";
    public:
        SendPackWorker(PackQ &inQ, cybozu::Socket &sock, Logger &logger)
            : inQ_(inQ), packet_(sock), logger_(logger) {
        }
        void operator()() try {
            packet::StreamControl ctrl(packet_.sock());
            std::vector<char> packAsVec;
            while (inQ_.pop(packAsVec)) {
                ctrl.next();
                packet_.write(&packAsVec[0], packAsVec.size());
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
            logger_.error() << CLS << msg;
            inQ_.fail();
        }
    };
    class CreatePackWorker
    {
    private:
        IoQ &inQ_;
        PackQ &outQ_;
        Logger &logger_;
        static constexpr char CLS[] = "CreatePackWorker";
    public:
        CreatePackWorker(IoQ &inQ, PackQ &outQ, Logger &logger)
            : inQ_(inQ), outQ_(outQ), logger_(logger) {
        }
        void operator()() try {
            hash_sync_local::Io io;
            while (inQ_.pop(io)) {
                // Compress and add to packer.
                // Packer is full, generate and push the pack.
                // QQQ
            }
            // Packer is not empty, generate and push the pack.
            // QQQ
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
            logger_.error() << CLS << msg;
            inQ_.fail();
            outQ_.fail();
        }
    };
    class ReceiveHashWorker
    {
    private:
        HashQ &outQ_;
        packet::Packet packet_;
        Logger &logger_;
        static constexpr char CLS[] = "ReceiveHashWorker";
    public:
        ReceiveHashWorker(HashQ &outQ, cybozu::Socket &sock, Logger &logger)
            : outQ_(outQ), packet_(sock), logger_(logger) {
        }
        void operator()() try {
            packet::StreamControl ctrl(packet_.sock());
            cybozu::murmurhash3::Hash hash;
            while (ctrl.isNext()) {
                packet_.read(hash);
                outQ_.push(hash);
                ctrl.reset();
            }
            if (ctrl.isError()) {
                throw cybozu::Exception(CLS) << "server sent an error";
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
            logger_.error() << CLS << msg;
            outQ_.fail();
        }
    };

    cybozu::Socket &sock_;
    Logger &logger_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isFailed_;

    HashQ hashQ_;
    IoQ ioQ_;
    PackQ packQ_;

    cybozu::thread::ThreadRunner receiveHashThread_;
    cybozu::thread::ThreadRunner createPackThread_;
    cybozu::thread::ThreadRunner sendPackThread_;

    static constexpr char CLS[] = "HashSyncClient";

public:
    HashSyncClient(cybozu::Socket &sock, Logger &logger)
        : sock_(sock), logger_(logger)
        , isEnd_(false), isFailed_(false)
        , hashQ_(Q_SIZE), ioQ_(Q_SIZE), packQ_(Q_SIZE) {
    }
    ~HashSyncClient() noexcept {
        if (!isEnd_ && !isFailed_) fail();
    }
    void start() {
        receiveHashThread_.set(ReceiveHashWorker(hashQ_, sock_, logger_));
        createPackThread_.set(CreatePackWorker(ioQ_, packQ_, logger_));
        sendPackThread_.set(SendPackWorker(packQ_, sock_, logger_));
        receiveHashThread_.start();
        createPackThread_.start();
        sendPackThread_.start();
    }
    bool popHash(cybozu::murmurhash3::Hash &hash) {
        if (!hashQ_.pop(hash)) {
            return false;
        }
        return true;
    }
    void pushIo(uint64_t ioAddress, uint16_t ioBlocks, std::vector<char> &&data) {
        ioQ_.push(hash_sync_local::Io{ioAddress, ioBlocks, std::move(data)});
    }
    void sync() {
        ioQ_.sync();
        isEnd_ = true;
        joinWorkers();
    }
    void fail() {
        isFailed_ = true;
        hashQ_.fail();
        ioQ_.fail();
        packQ_.fail();
        joinWorkers();
    }
private:
    void joinWorkers() noexcept {
        putErrorLogIfNecessary(receiveHashThread_.joinNoThrow(), logger_, CLS);
        putErrorLogIfNecessary(createPackThread_.joinNoThrow(), logger_, CLS);
        putErrorLogIfNecessary(sendPackThread_.joinNoThrow(), logger_, CLS);
    }
};

/**
 * Walb hash-sync server helper.
 * This will send hash arrays, receive diff packs.
 * You need more two threads A and B:
 *   A will call pushHash() repeatedly.
 *   B will call popPack() repeatedly.
 *
 * Usage:
 *   (1) call start() to start worker threads.
 *   (2) prpeare two threads A and B.
 *   (3) A call pushHash() repeatedly. finally call sync().
 *   (4) B call popPack() repeatedly until false is returned.
 *
 *   You can call fail() to stop all threads when an error ocurrs.
 */
class HashSyncServer
{
private:
    // QQQ

public:
    void pushHash(const cybozu::murmurhash3::Hash &hash) {
        cybozu::disable_warning_unused_variable(hash);
        // QQQ
    }
    void sync() {
        // QQQ
    }
    bool popPack(std::vector<char> &v) {
        cybozu::disable_warning_unused_variable(v);
        return false;
        // QQQ
    }
    void fail() {
        // QQQ
    }
};

} // namespace walb
