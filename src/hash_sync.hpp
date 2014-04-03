#pragma once
#include "thread_util.hpp"
#include "protocol.hpp"
#include "murmurhash3.hpp"
#include "walb_diff_pack.hpp"
#include "walb_diff_compressor.hpp"
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

class SendHashWorker
{
private:
    static constexpr char CLS[] = "SendHashWorker";
    using HashQ = cybozu::thread::BoundedQueue<cybozu::murmurhash3::Hash>;

    HashQ &inQ_;
    packet::Packet packet_;
    Logger &logger_;

public:
    SendHashWorker(HashQ &inQ, cybozu::Socket &sock, Logger &logger)
        : inQ_(inQ), packet_(sock), logger_(logger) {
    }
    void operator()() try {
        packet::StreamControl ctrl(packet_.sock());
        cybozu::murmurhash3::Hash hash;
        while (inQ_.pop(hash)) {
            ctrl.next();
            packet_.write(hash);
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

class ReceiveHashWorker
{
private:
    static constexpr char CLS[] = "ReceiveHashWorker";
    using HashQ = cybozu::thread::BoundedQueue<cybozu::murmurhash3::Hash>;

    HashQ &outQ_;
    packet::Packet packet_;
    Logger &logger_;

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

class SendPackWorker
{
private:
    static constexpr char CLS[] = "SendPackWorker";
    using PackQ = cybozu::thread::BoundedQueue<std::vector<char> >;

    PackQ &inQ_;
    packet::Packet packet_;
    Logger &logger_;

public:
    SendPackWorker(PackQ &inQ, cybozu::Socket &sock, Logger &logger)
        : inQ_(inQ), packet_(sock), logger_(logger) {
    }
    void operator()() try {
        packet::StreamControl ctrl(packet_.sock());
        std::vector<char> packAsVec;
        while (inQ_.pop(packAsVec)) {
            ctrl.next();
            packet_.write(packAsVec);
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

class ReceivePackWorker
{
private:
    static constexpr char CLS[] = "ReceivePackWorker";
    using PackQ = cybozu::thread::BoundedQueue<std::vector<char> >;

    PackQ &outQ_;
    packet::Packet packet_;
    Logger &logger_;

public:
    ReceivePackWorker(PackQ &outQ, cybozu::Socket &sock, Logger &logger)
        : outQ_(outQ), packet_(sock), logger_(logger) {
    }
    void operator()() try {
        packet::StreamControl ctrl(packet_.sock());
        std::vector<char> packAsVec;
        while (ctrl.isNext()) {
            packet_.read(packAsVec);
            outQ_.push(std::move(packAsVec));
            ctrl.reset();
        }
        if (ctrl.isError()) {
            throw cybozu::Exception(CLS) << "client sent an error";
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

/**
 * Full scan a block device.
 * Read blocks will be pushed to a thread-safe queue: outQ.
 * Users can pop IO data from the queue.
 * To notify an error to the worker, call outQ.fail().
 */
class ReadBdevWorker
{
private:
    using IoQ = cybozu::thread::BoundedQueue<hash_sync_local::Io>;

    cybozu::util::BlockDevice &bd_;
    const uint64_t sizeLb_;
    const uint16_t bulkLb_;
    IoQ &outQ_;
    Logger &logger_;
    static constexpr char CLS[] = "BdevReader";

public:
    ReadBdevWorker(cybozu::util::BlockDevice &bd, uint64_t sizeLb, uint16_t bulkLb, IoQ &outQ, Logger &logger)
        : bd_(bd), sizeLb_(sizeLb), bulkLb_(bulkLb)
        , outQ_(outQ), logger_(logger) {
    }
    void operator()() try {
        uint64_t addr = 0;
        uint64_t remainingLb = sizeLb_;
        std::vector<char> buf;
        while (remainingLb > 0) {
            const uint16_t lb = std::min<uint64_t>(remainingLb, bulkLb_);
            const uint64_t off = addr * LOGICAL_BLOCK_SIZE;
            const size_t size = lb * LOGICAL_BLOCK_SIZE;
            buf.resize(size);
            bd_.read(off, size, &buf[0]);
            outQ_.push(hash_sync_local::Io{addr, lb, std::move(buf)});
            remainingLb -= lb;
            addr += lb;
        }
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

    class CreatePackWorker
    {
    private:
        static constexpr char CLS[] = "CreatePackWorker";

        IoQ &inQ_;
        ConverterQueue &outQ_;
        Logger &logger_;

    public:
        CreatePackWorker(IoQ &inQ, ConverterQueue &outQ, Logger &logger)
            : inQ_(inQ), outQ_(outQ), logger_(logger) {
        }
        void operator()() try {
            hash_sync_local::Io io;
            diff::Packer packer;
            packer.setMaxNumRecords(Q_SIZE);
            while (inQ_.pop(io)) {
                if (packer.add(io.ioAddress, io.ioBlocks, &io.data[0])) {
                    continue;
                }
                pushPack(packer);
                packer.add(io.ioAddress, io.ioBlocks, &io.data[0]);
            }
            if (!packer.empty()) pushPack(packer);
            outQ_.quit();
        } catch (std::exception &e) {
            handleError(e.what());
            throw;
        } catch (...) {
            handleError("unknown error");
            throw;
        }
    private:
        void pushPack(diff::Packer &packer) {
            if (!packer.isValid()) {
                throw cybozu::Exception(CLS) << "invalid pack";
            }
            outQ_.push(packer.getPackAsVector());
        }
        void handleError(const char *msg) noexcept {
            logger_.error() << CLS << msg;
            inQ_.fail();
            outQ_.quit();
        }
    };
    class PassWorker
    {
    private:
        static constexpr char CLS[] = "PassWorker";

        ConverterQueue &inQ_;
        PackQ &outQ_;
        Logger &logger_;

    public:
        PassWorker(ConverterQueue &inQ, PackQ &outQ, Logger &logger)
            : inQ_(inQ), outQ_(outQ), logger_(logger) {
        }
        void operator()() try {
            std::vector<char> pack = inQ_.pop();
            if (!pack.empty()) {
                outQ_.push(std::move(pack));
                pack = inQ_.pop();
            }
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
            inQ_.quit();
            outQ_.fail();
        }
    };

    cybozu::Socket &sock_;
    Logger &logger_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isFailed_;

    HashQ hashQ_;
    IoQ ioQ_;
    std::unique_ptr<ConverterQueue> convQueueP_; // diff pack converter.
    PackQ packQ_;

    cybozu::thread::ThreadRunner receiveHashThread_;
    cybozu::thread::ThreadRunner createPackThread_;
    cybozu::thread::ThreadRunner passThread_;
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
        convQueueP_.reset(new ConverterQueue(Q_SIZE, 1, true, ::WALB_DIFF_CMPR_SNAPPY));
        receiveHashThread_.set(ReceiveHashWorker(hashQ_, sock_, logger_));
        createPackThread_.set(CreatePackWorker(ioQ_, *convQueueP_, logger_));
        passThread_.set(PassWorker(*convQueueP_, packQ_, logger_));
        sendPackThread_.set(SendPackWorker(packQ_, sock_, logger_));
        receiveHashThread_.start();
        createPackThread_.start();
        passThread_.start();
        sendPackThread_.start();
    }
    bool popHash(cybozu::murmurhash3::Hash &hash) {
        if (!hashQ_.pop(hash)) return false;
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
        if (convQueueP_) convQueueP_->quit();
        packQ_.fail();
        joinWorkers();
    }
private:
    void joinWorkers() noexcept {
        putErrorLogIfNecessary(receiveHashThread_.joinNoThrow(), logger_, CLS);
        putErrorLogIfNecessary(createPackThread_.joinNoThrow(), logger_, CLS);
        putErrorLogIfNecessary(passThread_.joinNoThrow(), logger_, CLS);
        putErrorLogIfNecessary(sendPackThread_.joinNoThrow(), logger_, CLS);
        if (convQueueP_) {
            convQueueP_->join();
            convQueueP_.reset();
        }
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
    static constexpr char CLS[] = "HashSyncServer";
    using HashQ = cybozu::thread::BoundedQueue<cybozu::murmurhash3::Hash>;
    using PackQ = cybozu::thread::BoundedQueue<std::vector<char> >;

    cybozu::Socket &sock_;
    Logger &logger_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isFailed_;

    HashQ hashQ_; /* Users push hashes to the queue. */
    PackQ packQ_; /* Users pop packs from the queue. */

    cybozu::thread::ThreadRunner sendHashThread_;
    cybozu::thread::ThreadRunner receivePackThread_;

public:
    HashSyncServer(cybozu::Socket &sock, Logger &logger)
        : sock_(sock), logger_(logger)
        , isEnd_(false), isFailed_(false)
        , hashQ_(Q_SIZE), packQ_(Q_SIZE) {
    }
    ~HashSyncServer() noexcept {
        if (!isEnd_ && !isFailed_) fail();
    }
    void start() {
        sendHashThread_.set(SendHashWorker(hashQ_, sock_, logger_));
        receivePackThread_.set(ReceivePackWorker(packQ_, sock_, logger_));
        sendHashThread_.start();
        receivePackThread_.start();
    }
    void pushHash(const cybozu::murmurhash3::Hash &hash) {
        hashQ_.push(hash);
    }
    void sync() {
        hashQ_.sync();
    }
    bool popPack(std::vector<char> &v) {
        if (!packQ_.pop(v)) {
            isEnd_ = true;
            joinWorkers();
            return false;
        }
        return true;
    }
    void fail() {
        isFailed_ = true;
        hashQ_.fail();
        packQ_.fail();
        joinWorkers();
    }
private:
    void joinWorkers() noexcept {
        putErrorLogIfNecessary(sendHashThread_.joinNoThrow(), logger_, CLS);
        putErrorLogIfNecessary(receivePackThread_.joinNoThrow(), logger_, CLS);
    }
};

/**
 *
 * Usage:
 *   (1) call setParams()
 *   (2) call pop() until it returns false.
 *
 *   You can call fail() to stop threads.
 */
class BdevHashArrayGenerator
{
private:
    using IoQ = cybozu::thread::BoundedQueue<hash_sync_local::Io>;

    Logger &logger_;
    std::atomic<bool> isEnd_;
    std::atomic<bool> isFailed_;
    IoQ ioQ_;

    cybozu::util::BlockDevice bd_;
    uint64_t sizeLb_;
    uint16_t bulkLb_;
    uint32_t hashSeed_;

    cybozu::thread::ThreadRunner readBdevThread_;

    static constexpr char CLS[] = "BdevHashArrayGenerator";

public:
    explicit BdevHashArrayGenerator(Logger &logger)
        : logger_(logger)
        , isEnd_(false), isFailed_(false)
        , ioQ_(Q_SIZE) {
    }
    ~BdevHashArrayGenerator() noexcept {
        if (!isEnd_ && !isFailed_) fail();
    }
    void setParams(const std::string &bdevPath, uint64_t sizeLb, uint16_t bulkLb, uint32_t hashSeed) {
        cybozu::util::BlockDevice bd(bdevPath, O_RDONLY);
        const uint64_t sizeLb2 = bd.getDeviceSize() / LOGICAL_BLOCK_SIZE;
        if (sizeLb2 != sizeLb) {
            throw cybozu::Exception(CLS) << "wrong sizeLb" << sizeLb2 << sizeLb;
        }
        if (bulkLb == 0) {
            throw cybozu::Exception(CLS) << "bulkLb is 0";
        }
        bd_ = std::move(bd);
        sizeLb_ = sizeLb;
        bulkLb_ = bulkLb;
        hashSeed_ = hashSeed;
    }
    void start() {
        readBdevThread_.set(ReadBdevWorker(bd_, sizeLb_, bulkLb_, ioQ_, logger_));
        readBdevThread_.start();
    }
    bool pop(cybozu::murmurhash3::Hash &hash, uint64_t &ioAddress, uint16_t &ioBlocks, std::vector<char> &data) {
        hash_sync_local::Io io;
        if (!ioQ_.pop(io)) {
            isEnd_ = true;
            joinWorkers();
            return false;
        }
        ioAddress = io.ioAddress;
        ioBlocks = io.ioBlocks;
        data = std::move(io.data);
        cybozu::murmurhash3::Hasher hasher(hashSeed_);
        hash = hasher(&data[0], data.size());
        return true;
    }
    void fail() noexcept {
        isFailed_ = true;
        ioQ_.fail();
        joinWorkers();
    }
private:
    void joinWorkers() noexcept {
        putErrorLogIfNecessary(readBdevThread_.joinNoThrow(), logger_, CLS);
    }
};

} // namespace walb
