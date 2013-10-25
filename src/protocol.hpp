#pragma once
/**
 * @file
 * @brief Protocol set.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <map>
#include <string>
#include <memory>
#include "cybozu/format.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/time.hpp"
#include "cybozu/atoi.hpp"
#include "cybozu/itoa.hpp"
#include "packet.hpp"
#include "util.hpp"
#include "sys_logger.hpp"
#include "serializer.hpp"
#include "fileio.hpp"
#include "walb_diff_virt.hpp"
#include "server_data.hpp"
#include "proxy_data.hpp"
#include "walb_diff_pack.hpp"
#include "walb_diff_compressor.hpp"
#include "thread_util.hpp"
#include "murmurhash3.hpp"
#include "walb_log_compressor.hpp"
#include "walb_log_file.hpp"
#include "uuid.hpp"
#include "walb_diff_converter.hpp"

namespace walb {

/**
 * Logger wrapper for protocols.
 */
class Logger
{
private:
    std::string selfId_;
    std::string remoteId_;
public:
    Logger(const std::string &selfId, const std::string &remoteId)
        : selfId_(selfId), remoteId_(remoteId) {}

    void write(cybozu::LogPriority pri, const char *msg) const noexcept {
        cybozu::PutLog(pri, "[%s][%s] %s", selfId_.c_str(), remoteId_.c_str(), msg);
    }
    void write(cybozu::LogPriority pri, const std::string &msg) const noexcept {
        write(pri, msg.c_str());
    }
    void writeV(cybozu::LogPriority pri, const char *format, va_list args) const noexcept {
        try {
            std::string msg;
            cybozu::vformat(msg, format, args);
            write(pri, msg);
        } catch (...) {
            write(pri, "Logger::write() error.");
        }
    }
    void writeF(cybozu::LogPriority pri, const char *format, ...) const noexcept {
        try {
            va_list args;
            va_start(args, format);
            writeV(pri, format, args);
            va_end(args);
        } catch (...) {
            write(pri, "Logger::write() error.");
        }
    }

    void debug(UNUSED const std::string &msg) const noexcept {
#ifdef DEBUG
        write(cybozu::LogDebug, msg);
#endif
    }
    void info(const std::string &msg) const noexcept { write(cybozu::LogInfo, msg); }
    void warn(const std::string &msg) const noexcept { write(cybozu::LogWarning, msg); }
    void error(const std::string &msg) const noexcept { write(cybozu::LogError, msg); }

    void debug(UNUSED const char *format, ...) const noexcept {
#ifdef DEBUG
        va_list args;
        va_start(args, format);
        writeV(cybozu::LogDebug, format, args);
        va_end(args);
#endif
    }
    void info(const char *format, ...) const noexcept {
        va_list args;
        va_start(args, format);
        writeV(cybozu::LogInfo, format, args);
        va_end(args);
    }
    void warn(const char *format, ...) const noexcept {
        va_list args;
        va_start(args, format);
        writeV(cybozu::LogWarning, format, args);
        va_end(args);
    }
    void error(const char *format, ...) const noexcept {
        va_list args;
        va_start(args, format);
        writeV(cybozu::LogError, format, args);
        va_end(args);
    }
};

/**
 * Protocol interface.
 */
class Protocol
{
protected:
    const std::string name_; /* protocol name */
public:
    Protocol(const std::string &name) : name_(name) {}
    const std::string &name() const { return name_; }
    virtual void runAsClient(cybozu::Socket &, Logger &,
                             const std::vector<std::string> &) = 0;
    virtual void runAsServer(cybozu::Socket &, Logger &,
                             const std::vector<std::string> &) = 0;
};

/**
 * Simple echo client.
 */
class EchoProtocol : public Protocol
{
public:
    using Protocol :: Protocol;

    void runAsClient(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        if (params.empty()) throw std::runtime_error("params empty.");
        packet::Packet packet(sock);
        uint32_t size = params.size();
        packet.write(size);
        logger.info("size: %" PRIu32 "", size);
        for (const std::string &s0 : params) {
            std::string s1;
            packet.write(s0);
            packet.read(s1);
            logger.info("s0: %s s1: %s", s0.c_str(), s1.c_str());
            if (s0 != s1) {
                throw std::runtime_error("echo-backed string differs from the original.");
            }
        }
    }
    void runAsServer(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &) override {
        packet::Packet packet(sock);
        uint32_t size;
        packet.read(size);
        logger.info("size: %" PRIu32 "", size);
        for (uint32_t i = 0; i < size; i++) {
            std::string s0;
            packet.read(s0);
            packet.write(s0);
            logger.info("echoback: %s", s0.c_str());
        }
    }
};

/**
 * Utility class for protocols.
 */
class ProtocolData
{
protected:
    cybozu::Socket &sock_;
    Logger &logger_;
    const std::vector<std::string> &params_;
public:
    ProtocolData(cybozu::Socket &sock, Logger &logger, const std::vector<std::string> &params)
        : sock_(sock), logger_(logger), params_(params) {}
    virtual ~ProtocolData() noexcept = default;

    void logAndThrow(const char *fmt, ...) const {
        va_list args;
        va_start(args, fmt);
        std::string msg = cybozu::util::formatStringV(fmt, args);
        va_end(args);
        logger_.error(msg);
        throw std::runtime_error(msg);
    }
};

/**
 * Dirty full sync.
 *
 * TODO: exclusive access control.
 */
class DirtyFullSyncProtocol : public Protocol
{
private:
    class Data : public ProtocolData
    {
    protected:
        std::string name_;
        uint64_t sizeLb_;
        uint16_t bulkLb_;
        uint64_t gid_;
    public:
        using ProtocolData::ProtocolData;
        void checkParams() const {
            if (name_.empty()) logAndThrow("name param empty.");
            if (sizeLb_ == 0) logAndThrow("sizeLb param is zero.");
            if (bulkLb_ == 0) logAndThrow("bulkLb param is zero.");
            if (gid_ == uint64_t(-1)) logAndThrow("gid param must not be uint64_t(-1).");
        }
        void sendParams() {
            packet::Packet packet(sock_);
            packet.write(name_);
            packet.write(sizeLb_);
            packet.write(bulkLb_);
            packet.write(gid_);
        }
        void recvParams() {
            packet::Packet packet(sock_);
            packet.read(name_);
            packet.read(sizeLb_);
            packet.read(bulkLb_);
            packet.read(gid_);
        }
    };
    class Client : public Data
    {
    private:
        std::string path_;
    public:
        using Data::Data;
        void run() {
            loadParams();
            sendParams();
            readAndSend();
        }
    private:
        void loadParams() {
            if (params_.size() != 5) logAndThrow("Five parameters required.");
            path_ = params_[0];
            name_ = params_[1];
            uint64_t size = cybozu::util::fromUnitIntString(params_[2]);
            sizeLb_ = size / LOGICAL_BLOCK_SIZE;
            uint64_t bulk = cybozu::util::fromUnitIntString(params_[3]);
            if ((1U << 16) * LOGICAL_BLOCK_SIZE <= bulk) logAndThrow("bulk size too large. < %u\n", (1U << 16) * LOGICAL_BLOCK_SIZE);
            bulkLb_ = bulk / LOGICAL_BLOCK_SIZE;
            gid_ = cybozu::atoi(params_[4]);
        }
        void readAndSend() {
            packet::Packet packet(sock_);
            std::vector<char> buf(bulkLb_ * LOGICAL_BLOCK_SIZE);
            cybozu::util::BlockDevice bd(path_, O_RDONLY);

            uint64_t remainingLb = sizeLb_;
            while (0 < remainingLb) {
                uint16_t lb = bulkLb_;
                if (remainingLb < bulkLb_) lb = remainingLb;
                size_t size = lb * LOGICAL_BLOCK_SIZE;
                bd.read(&buf[0], size);
                packet.write(lb);
                packet.write(&buf[0], size);
                remainingLb -= lb;
            }
        }
    };
    class Server : public Data
    {
    private:
        cybozu::FilePath baseDir_;
    public:
        using Data::Data;
        void run() {
            loadParams();
            recvParams();
            logger_.info("dirty-full-sync %s %" PRIu64 " %u %" PRIu64 ""
                         , name_.c_str(), sizeLb_, bulkLb_, gid_);
            recvAndWrite();
        }
    private:
        void loadParams() {
            if (params_.size() != 1) logAndThrow("One parameter required.");
            baseDir_ = cybozu::FilePath(params_[0]);
            if (!baseDir_.stat().isDirectory()) {
                logAndThrow("Base directory %s does not exist.", baseDir_.cStr());
            }
        }
        void recvAndWrite() {
            packet::Packet packet(sock_);
            ServerData sd(baseDir_.str(), name_);
            sd.reset(gid_);
            sd.createLv(sizeLb_);
            std::string lvPath = sd.getLv().path().str();
            cybozu::util::BlockDevice bd(lvPath, O_RDWR);
            std::vector<char> buf(bulkLb_ * LOGICAL_BLOCK_SIZE);

            uint16_t c = 0;
            uint64_t remainingLb = sizeLb_;
            while (0 < remainingLb) {
                uint16_t lb = bulkLb_;
                if (remainingLb < bulkLb_) lb = remainingLb;
                size_t size = lb * LOGICAL_BLOCK_SIZE;
                uint16_t lb0 = 0;
                packet.read(lb0);
                if (lb0 != lb) logAndThrow("received lb %u is invalid. must be %u", lb0, lb);
                packet.read(&buf[0], size);
                bd.write(&buf[0], size);
                remainingLb -= lb;
                c++;
            }
            logger_.info("received %" PRIu64 " packets.", c);
            bd.fdatasync();
            logger_.info("dirty-full-sync %s done.", name_.c_str());
        }
    };

public:
    using Protocol :: Protocol;

    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: full path of lv.
     *   [1] :: string: lv identifier.
     *   [2] :: uint64_t: lv size [byte].
     *   [3] :: uint32_t: bulk size [byte]. less than uint16_t * LOGICAL_BLOCK_SIZE;
     */
    void runAsClient(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        Client client(sock, logger, params);
        client.run();
    }
    /**
     *
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void runAsServer(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        Server server(sock, logger, params);
        server.run();
    }
};

#if 1
/**
 * Hash sync
 */
class DirtyHashSyncProtocol : public Protocol
{
    class Data : public ProtocolData
    {
    protected:
        std::string name_;
        uint64_t sizeLb_;
        uint16_t bulkLb_;
        uint32_t seed_; /* murmurhash3 seed */
        MetaSnap snap_;
    public:
        using ProtocolData::ProtocolData;
        void checkParams() const {
            if (name_.empty()) logAndThrow("name param empty.");
            if (sizeLb_ == 0) logAndThrow("sizeLb param is zero.");
            if (bulkLb_ == 0) logAndThrow("bulkLb param is zero.");
        }
    };
    class Client : public Data
    {
    private:
        std::string path_;
    public:
        using Data::Data;
        void run() {
            loadParams();
            negotiate();
            readAndSendDiffData();
            sendMetaDiff();
        }
    private:
        /**
         * path
         * name
         * lv size [bytes]
         * bulk size [bytes]
         * seed
         */
        void loadParams() {
            if (params_.size() != 5) logAndThrow("Five parameters required.");
            path_ = params_[0];
            name_ = params_[1];
            uint64_t size = cybozu::util::fromUnitIntString(params_[2]);
            sizeLb_ = size / LOGICAL_BLOCK_SIZE;
            uint64_t bulk = cybozu::util::fromUnitIntString(params_[3]);
            if ((1U << 16) * LOGICAL_BLOCK_SIZE <= bulk) {
                logAndThrow("bulk size too large. < %u\n", (1U << 16) * LOGICAL_BLOCK_SIZE);
            }
            bulkLb_ = bulk / LOGICAL_BLOCK_SIZE;
            seed_ = cybozu::atoi(params_[4]);
        }
        void negotiate() {
            packet::Packet packet(sock_);
            packet.write(name_);
            packet.write(sizeLb_);
            packet.write(bulkLb_);
            packet.write(seed_);
            packet::Answer ans(sock_);
            int err; std::string msg;
            if (!ans.recv(&err, &msg)) {
                logAndThrow("negotiation failed: %d %s", err, msg.c_str());
            }
            packet.read(snap_);
        }
        /**
         * TODO: send diffs as snappied wdiff format.
         */
        void readAndSendDiffData() {
            packet::Packet packet(sock_);
            cybozu::thread::BoundedQueue<cybozu::murmurhash3::Hash> hashQ(32); /* TODO: do not hardcode. */
            std::vector<char> buf(bulkLb_ * LOGICAL_BLOCK_SIZE);
            cybozu::util::BlockDevice bd(path_, O_RDONLY);
            cybozu::murmurhash3::Hasher hasher(seed_);
            ConverterQueue convQ(4, 2, true, ::WALB_DIFF_CMPR_SNAPPY); /* TODO: do not hardcode. */
            diff::Packer packer;
            packer.setMaxPackSize(2U << 20); /* 2MiB. TODO: do not hardcode. */
            packer.setMaxNumRecords(10); /* TODO: do not hardcode. */
            std::atomic<bool> isError(false);

            /* Read hash data */
            auto receiveHash = [this, &packet, &hashQ](uint64_t num) {
                try {
                    for (uint64_t i = 0; i < num; i++) {
                        cybozu::murmurhash3::Hash h;
                        packet.read(h.rawData(), h.rawSize());
                        hashQ.push(h);
                    }
                    hashQ.sync();
                } catch (...) {
                    logger_.error("receiveHash failed.");
                    hashQ.error();
                }
                logger_.info("receiveHash end");
            };

            size_t nPack = 0;
            auto pushPackToQueue = [this, &nPack, &packer, &convQ]() {
                std::unique_ptr<char[]> up = packer.getPackAsUniquePtr();
                diff::MemoryPack mpack(up.get());
                logger_.info("try to push pack %zu %p %zu", nPack, up.get(), mpack.size());
                if (!convQ.push(up)) {
                    throw std::runtime_error("convQ push failed.");
                }
                logger_.info("push pack %zu", nPack++);
                assert(packer.empty());
            };

            /* Send updated bulk. */
            auto addToPack = [this, &hashQ, &packer, &buf, &bd, &hasher, &convQ, &pushPackToQueue](
                uint64_t &offLb, uint64_t &remainingLb) {

                uint16_t lb = bulkLb_;
                if (remainingLb < bulkLb_) lb = remainingLb;
                size_t size = lb * LOGICAL_BLOCK_SIZE;
                bd.read(&buf[0], size);
                cybozu::murmurhash3::Hash h0, h1;
                h0 = hasher(&buf[0], size);
                h1 = hashQ.pop();
                if (h0 != h1) {
                    //logger_.info("Hash differ %s %s", h0.str().c_str(), h1.str().c_str());
                    if (!packer.canAddLb(lb)) pushPackToQueue();
                    UNUSED bool r = packer.add(offLb, lb, &buf[0]);
                    assert(r);
                    //packer.print(); /* debug */
                    assert(packer.isValid(false)); /* debug */
                } else {
                    //logger_.info("Hash same %s %s", h0.str().c_str(), h1.str().c_str());
                }
                offLb += lb;
                remainingLb -= lb;
            };

            /* Read block device and create pack. */
            auto createPack = [this, &isError, &hashQ, &addToPack, &packer, &convQ, &pushPackToQueue](
                UNUSED uint64_t num) {

                try {
                    uint64_t offLb = 0;
                    uint64_t remainingLb = sizeLb_;
                    uint64_t c = 0;
                    while (0 < remainingLb) {
                        addToPack(offLb, remainingLb);
                        c++;
                    }
                    if (!packer.empty()) pushPackToQueue();
                    assert(num == c);
                    assert(offLb == sizeLb_);
                    assert(hashQ.isEnd());
                } catch (...) {
                    logger_.error("createPack failed.");
                    isError = true;
                    hashQ.error();
                }
                convQ.quit();
                convQ.join();
                logger_.info("createPack end");
            };

            /* Send compressed pack. */
            auto sendPack = [this, &packet, &convQ, &isError]() {
                try {
                    packet::StreamControl ctrl(packet.sock());
                    size_t c = 0;
                    logger_.info("try to pop from convQ"); /* debug */
                    std::unique_ptr<char[]> up = convQ.pop();
                    logger_.info("popped from convQ: %p", up.get()); /* debug */
                    while (up) {
                        logger_.info("try to send pack %zu", c);
                        ctrl.next();
                        diff::MemoryPack mpack(up.get());
                        uint32_t packSize = mpack.size();
                        packet.write(packSize);
                        packet.write(mpack.rawPtr(), packSize);
                        logger_.info("send pack %zu", c++);
                        up = convQ.pop();
                    }
                    if (isError) ctrl.error(); else ctrl.end();
                    logger_.info("sendPack: end (%d)", isError.load());
                } catch (std::exception &e) {
                    logger_.error("sendPack failed: %s", e.what());
                    convQ.quit();
                    while (convQ.pop());
                    isError = true;
                } catch (...) {
                    logger_.error("sendPack failed.");
                    convQ.quit();
                    while (convQ.pop());
                    isError = true;
                }
                assert(convQ.pop() == nullptr);
                logger_.info("sendPack end");
            };

            /* Invoke threads. */
            uint64_t num = (sizeLb_ - 1) / bulkLb_ + 1;
            std::thread th0(receiveHash, num);
            std::thread th1(createPack, num);
            std::thread th2(sendPack);
            th0.join();
            th1.join();
            th2.join();

            if (isError) logAndThrow("readAndSendDiffData failed.");
        }
        void sendMetaDiff() {
            packet::Packet packet(sock_);
            MetaDiff diff(snap_.gid0(), snap_.gid1(), snap_.gid1() + 1, snap_.gid1() + 2);
            diff.setTimestamp(::time(0));
            packet.write(diff);
        }
    };
    class Server : public Data
    {
    private:
        cybozu::FilePath baseDir_;
    public:
        using Data::Data;
        void run() {
            loadParams();
            negotiate();
            logger_.info("dirty-hash-sync %s %" PRIu64 " %u %u (%" PRIu64 " %" PRIu64")"
                         , name_.c_str(), sizeLb_, bulkLb_, seed_, snap_.gid0(), snap_.gid1());
            recvAndWriteDiffData();
        }
    private:
        void loadParams() {
            if (params_.size() != 1) logAndThrow("One parameter required.");
            baseDir_ = cybozu::FilePath(params_[0]);
            if (!baseDir_.stat().isDirectory()) {
                logAndThrow("Base directory %s does not exist.", baseDir_.cStr());
            }
        }
        void negotiate() {
            packet::Packet packet(sock_);
            packet.read(name_);
            packet.read(sizeLb_);
            packet.read(bulkLb_);
            packet.read(seed_);
            packet::Answer ans(sock_);
            ServerData sd(baseDir_.str(), name_);
            if (!sd.lvExists()) {
                std::string msg = cybozu::util::formatString(
                    "Lv does not exist for %s. Try full-sync."
                    , name_.c_str());
                ans.ng(1, msg);
                logAndThrow(msg.c_str());
            }
            ans.ok();
            MetaSnap snap = sd.getLatestSnapshot();
            packet.write(snap);
        }
        void recvAndWriteDiffData() {
            packet::Packet packet(sock_);
            ServerData sd(baseDir_.str(), name_);
            std::string lvPath = sd.getLv().path().str();
            cybozu::util::BlockDevice bd(lvPath, O_RDWR);
            cybozu::murmurhash3::Hasher hasher(seed_);
            std::atomic<bool> isError(false);

            logger_.info("hoge1");

            /* Prepare virtual full lv */
            std::vector<MetaDiff> diffs = sd.diffsToApply(snap_.gid0());
            std::vector<std::string> diffPaths;
            for (MetaDiff &diff : diffs) {
                cybozu::FilePath fp = sd.getDiffPath(diff);
                diffPaths.push_back(fp.str());
            }
            diff::VirtualFullScanner virtLv(bd.getFd(), diffPaths);
            std::vector<char> buf0(bulkLb_ * LOGICAL_BLOCK_SIZE);

            logger_.info("hoge2");

            auto readBulkAndSendHash = [this, &virtLv, &packet, &buf0, &hasher](
                uint64_t &offLb, uint64_t &remainingLb) {

                uint16_t lb = bulkLb_;
                if (remainingLb < bulkLb_) lb = remainingLb;
                size_t size = lb * LOGICAL_BLOCK_SIZE;
                virtLv.read(&buf0[0], size);
                cybozu::murmurhash3::Hash h0 = hasher(&buf0[0], size);
                packet.write(h0.rawData(), h0.rawSize());
                offLb += lb;
                remainingLb -= lb;
            };

            auto hashSender = [this, &readBulkAndSendHash, &isError]() {
                try {
                    uint64_t offLb = 0;
                    uint64_t remainingLb = sizeLb_;
                    uint64_t c = 0;
                    while (0 < remainingLb) {
#if 0
                        logger_.info("c %" PRIu64 " offLb %" PRIu64 "", c, offLb); /* debug */
#endif
                        readBulkAndSendHash(offLb, remainingLb);
                        c++;
                    }
                    logger_.info("remainingLb 0"); /* debug */
                    assert(offLb = sizeLb_);
                } catch (std::exception &e) {
                    logger_.error("hashSender error: %s", e.what());
                    isError = true;
                } catch (...) {
                    logger_.error("bulkReceiver unknown error");
                    isError = true;
                }
                logger_.info("hashSender end");
            };

            diff::FileHeaderRaw wdiffH;
            wdiffH.init();
            wdiffH.setMaxIoBlocksIfNecessary(bulkLb_);

            cybozu::TmpFile tmpFile(sd.getDiffDir().str());
            diff::Writer writer(tmpFile.fd());
            writer.writeHeader(wdiffH);
            cybozu::util::FdWriter fdw(tmpFile.fd());

            logger_.info("hoge3");

            auto bulkReceiver = [this, &packet, &writer, &fdw, &isError]() {
                try {
                    std::vector<char> buf;
                    packet::StreamControl ctrl(sock_);
                    size_t c = 0;
                    while (ctrl.isNext()) {
                        uint32_t packSize;
                        packet.read(packSize);
                        buf.resize(packSize);
                        packet.read(&buf[0], packSize);
                        logger_.info("received pack %zu", c++);
                        diff::MemoryPack mpack(&buf[0]);
                        if (mpack.size() != packSize) {
                            throw std::runtime_error("pack size invalid.");
                        }
                        if (!mpack.isValid()) {
                            throw std::runtime_error("received pack invalid.");
                        }
                        fdw.write(mpack.rawPtr(), packSize);
                        ctrl.reset();
                    }
                    if (ctrl.isError()) {
                        logger_.error("CtrlMsg error");
                        isError = true;
                    } else {
                        logger_.info("CtrlMsg end");
                    }
                    logger_.info("bulkRecriver end.");
                } catch (std::exception &e) {
                    logger_.error("bulkReceiver error: %s", e.what());
                    isError = true;
                } catch (...) {
                    logger_.error("bulkReceiver unknown error");
                    isError = true;
                }
                logger_.info("bulkReceiver end");
            };

            std::thread th0(hashSender);
            logger_.info("hoge4");
            std::thread th1(bulkReceiver);
            logger_.info("hoge5");
            th0.join();
            th1.join();
            logger_.info("joined: %d", isError.load());

            std::this_thread::sleep_for(std::chrono::milliseconds(500)); /* debug */
            if (isError) logAndThrow("recvAndWriteDiffData failed.");

            writer.close();
            MetaDiff diff;
            packet.read(diff); /* get diff information from the client. */
            cybozu::FilePath fp = sd.getDiffPath(diff);
            assert(fp.isFull());
            tmpFile.save(fp.str());
            sd.add(diff);

            logger_.info("dirty-hash-sync %s done.", name_.c_str());
        }
    };

public:
    using Protocol :: Protocol;

    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: full path of lv.
     *   [1] :: string: lv identifier.
     *   [2] :: uint64_t: lv size [byte].
     *   [3] :: uint32_t: bulk size [byte]. less than uint16_t * LOGICAL_BLOCK_SIZE;
     */
    void runAsClient(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        Client client(sock, logger, params);
        client.run();
    }
    /**
     *
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void runAsServer(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        Server server(sock, logger, params);
        server.run();
    }
    /* now editing */
};
#endif

#if 1
/**
 * Wlog-send protocol.
 *
 * Client: walb-worker.
 * Server: walb-proxy.
 */
class LogSendProtocol : public Protocol
{
private:
    using Block = std::shared_ptr<uint8_t>;
    using BoundedQ = cybozu::thread::BoundedQueue<log::CompressedData, true>;
    static constexpr size_t Q_SIZE = 10; /* TODO: do not hard cord. */

    class Data : public ProtocolData
    {
    protected:
        std::string name_;
        /* Server will check hash-sync occurrence with the uuid. */
        cybozu::Uuid uuid_;
        MetaDiff diff_;
        uint32_t pbs_; /* physical block size */
        uint32_t salt_; /* checksum salt. */
        uint64_t sizePb_; /* Number of physical blocks (lsid range) 0 is allowed. */
    public:
        using ProtocolData::ProtocolData;
        void checkParams() const {
            if (name_.empty()) logAndThrow("name param empty.");
            if (!diff_.isValid()) logAndThrow("Invalid meta diff.");
            if (!::is_valid_pbs(pbs_)) logAndThrow("Invalid pbs.");
        }
        std::string str() const {
            return cybozu::util::formatString(
                "name %s uuid %s diff (%s) pbs %" PRIu32 " salt %08x %" PRIu64 ""
                , name_.c_str(), uuid_.str().c_str(), diff_.str().c_str()
                , pbs_, salt_, sizePb_);
        }
    };
    /**
     * Client must call
     * (1) setParams().
     * (2) prepare().
     * (3) push() multiple times.
     * (4) sync() or error().
     */
    class Client : public Data
    {
    private:
        BoundedQ q0_; /* uncompressed data. */
        BoundedQ q1_; /* compressed data. */
        cybozu::thread::ThreadRunner compressor_;
        cybozu::thread::ThreadRunner sender_;
        std::atomic<bool> isError_;

        class Sender : public cybozu::thread::Runnable
        {
        private:
            BoundedQ &inQ_;
            packet::Packet packet_;
            Logger &logger_;
            const std::atomic<bool> &isError_;
        public:
            Sender(BoundedQ &inQ, cybozu::Socket &sock, Logger &logger,
                   const std::atomic<bool> &isError)
                : inQ_(inQ), packet_(sock), logger_(logger)
                , isError_(isError) {}
            void operator()() noexcept override try {
                packet::StreamControl ctrl(packet_.sock());
                while (!inQ_.isEnd()) {
                    log::CompressedData cd = inQ_.pop();
                    ctrl.next();
                    cd.send(packet_);
                }
                if (isError_) ctrl.error(); else ctrl.end();
            } catch (...) {
                throwErrorLater();
                inQ_.error();
            }
        };
    public:
        Client(cybozu::Socket &sock, Logger &logger, const std::vector<std::string> &params)
            : Data(sock, logger, params), q0_(Q_SIZE), q1_(Q_SIZE) {}
        void run() {
#if 0
            /* Open wldev device. */

            /* Get pbs, salt from wldev. */

            /* Decide lsid range. */


            /* Get wlog information. */


            setParams();
            prepare();
            while (true) {
                /* Get logpack and push */
                push();
            }
            sync();
            //error();

            /* now editing */
#endif
        }
        void setParams(const std::string &name, const uint8_t *uuid, const MetaDiff &diff,
                       uint32_t pbs, uint32_t salt, uint64_t sizePb) {
            name_ = name;
            ::memcpy(uuid_.rawData(), uuid, uuid_.rawSize());
            diff_ = diff;
            pbs_ = pbs;
            salt_ = salt;
            sizePb_ = sizePb;
            checkParams();
        }
        /**
         * Prepare worker threads.
         */
        void prepare() {
            isError_ = false;
            negotiate();
            compressor_.set(std::make_shared<log::CompressWorker>(q0_, q1_));
            sender_.set(std::make_shared<Sender>(q1_, sock_, logger_, isError_));
            compressor_.start();
            sender_.start();
        }
        /**
         * Push a log pack.
         * Do not send end logpack header by yourself. Use sync() instead.
         * TODO: use better interface.
         */
        void push(const log::PackHeaderWrap &header, std::queue<Block> &&blocks) {
            assert(header.totalIoSize() == blocks.size());
            /* Header */
            log::CompressedData cd;
            cd.copyFrom(0, header.pbs(), header.rawData());
            q0_.push(std::move(cd));

            /* IO data */
            size_t total = 0;
            for (size_t i = 0; i < header.nRecords(); i++) {
                total += pushIo(header, i, blocks);
            }
            assert(total == header.totalIoSize());
        }
        /**
         * Notify the input has reached the end and finalize the protocol.
         *
         */
        void sync() {
            q0_.push(generateEndHeaderBlock());
            q0_.sync();
            joinWorkers();
            packet::Ack ack(sock_);
            ack.recv();
        }
        /**
         * Notify an error and finalize the protocol.
         */
        void error() {
            isError_ = true;
            q0_.error();
            joinWorkers();
        }
    private:
        /**
         * RETURN:
         *   Number of consumed physical blocks.
         */
        size_t pushIo(const log::PackHeader &header, size_t idx,
                    std::queue<Block> &blocks) {

            const log::RecordWrapConst rec(&header, idx);
            if (!rec.hasData()) return 0;

            size_t s = header.pbs() * rec.ioSizePb();
            std::vector<char> v;
            v.resize(s);
            size_t n = rec.ioSizePb();
            size_t off = 0;
            while (0 < n && !blocks.empty()) {
                Block b = std::move(blocks.front());
                blocks.pop();
                ::memcpy(&v[off], b.get(), header.pbs());
                off += header.pbs();
                n--;
            }
            assert(n == 0);
            assert(off == s);
            log::CompressedData cd;
            cd.moveFrom(0, s, std::move(v));
            q0_.push(std::move(cd));
            return rec.ioSizePb();
        }
        log::CompressedData generateEndHeaderBlock() const {
            Block b = cybozu::util::allocateBlocks<uint8_t>(pbs_, pbs_);
            log::PackHeaderRaw header(b, pbs_, salt_);
            header.setEnd();
            header.updateChecksum();
            log::CompressedData cd;
            cd.copyFrom(0, pbs_, b.get());
            return cd;
        }
        void joinWorkers() {
            std::exception_ptr ep0, ep1;
            ep0 = compressor_.joinNoThrow();
            ep1 = sender_.joinNoThrow();
            if (ep0) std::rethrow_exception(ep0);
            if (ep1) std::rethrow_exception(ep1);
        }
        void negotiate() {
            packet::Packet packet(sock_);
            packet.write(name_);
            packet.write(uuid_);
            packet.write(diff_);
            packet.write(pbs_);
            packet.write(salt_);
            packet.write(sizePb_);
            packet::Answer ans(sock_);
            int err; std::string msg;
            if (!ans.recv(&err, &msg)) {
                logAndThrow("negotiation failed: %d %s", err, msg.c_str());
            }
        }
    };
    class Server : public Data
    {
    private:
        cybozu::FilePath baseDir_;
    public:
        using Data::Data;
        void run() {
            loadParams();
            negotiate();
            logger_.info(
                "wlog-send %s %s ((%" PRIu64 ", %" PRIu64 "), (%" PRIu64", %" PRIu64 ")) "
                "%" PRIu32 " %" PRIu32 " %" PRIu64 " "
                , name_.c_str(), uuid_.str().c_str()
                , diff_.snap0().gid0(), diff_.snap0().gid1()
                , diff_.snap1().gid0(), diff_.snap1().gid1()
                , pbs_, salt_, sizePb_);
            recvAndWriteDiffData();
        }
    private:
        void loadParams() {
            if (params_.size() != 1) logAndThrow("One parameter required.");
            baseDir_ = cybozu::FilePath(params_[0]);
            if (!baseDir_.stat().isDirectory()) {
                logAndThrow("Base directory %s does not exist.", baseDir_.cStr());
            }
        }
        void negotiate() {
            packet::Packet packet(sock_);
            packet.read(name_);
            packet.read(uuid_);
            packet.read(diff_);
            packet.read(pbs_);
            packet.read(salt_);
            packet.read(sizePb_);
            checkParams();
            packet::Answer ans(sock_);
            ProxyData pd(baseDir_.str(), name_);
            if (pd.getServerNameList().empty()) {
                std::string msg("There is no server registered.");
                ans.ng(1, msg);
                logAndThrow(msg.c_str());
            }
            ans.ok();
        }
        void recvAndWriteDiffData() {
            packet::Packet packet(sock_);

            /* TODO: proxy data instance must be shared among threads. */
            ProxyData pd(baseDir_.str(), name_);
            cybozu::TmpFile tmpFile(pd.getDiffDirToAdd().str());
            const uint16_t maxIoBlocks = 64 * 1024 / LOGICAL_BLOCK_SIZE; /* TODO: */
            diff::MemoryData wdiffM(maxIoBlocks);
            wdiffM.header().setUuid(uuid_.rawData());

            while (readLogpackAndAdd(packet, wdiffM)) {}
            wdiffM.writeTo(tmpFile.fd());
            tmpFile.save(pd.getDiffPathToAdd(diff_).str());
            pd.add(diff_);

            packet::Ack ack(sock_);
            ack.send();

            /* TODO: notify wdiff-sender threads. */
        }
    private:
        /**
         * Read a logpack, convert to diff data and
         * add all the IOs in the pack into the memory data.
         *
         * padding data will be skipped.
         *
         * RETURN:
         *   false if it read the end header block,
         *   true otherwise.
         */
        bool readLogpackAndAdd(packet::Packet &packet, diff::MemoryData &wdiffM) {
            log::CompressedData cdHead, cdIo;
            packet::StreamControl ctrl(packet.sock());
            if (!ctrl.isNext()) logAndThrow("Client said error.");
            ctrl.reset();
            cdHead.recv(packet);
            if (cdHead.isCompressed()) cdHead = cdHead.uncompress();
            const uint8_t *raw = reinterpret_cast<const uint8_t *>(cdHead.rawData());
            const log::PackHeaderWrapConst packH(raw, pbs_, salt_);
            if (!packH.isValid()) {
                logAndThrow("invalid logpack header: lsid %" PRIu64 "", packH.logpackLsid());
            }
            if (packH.isEnd()) {
                if (!ctrl.isEnd()) logAndThrow("Client did not say end.");
                return false;
            }

            for (size_t i = 0; i < packH.nRecords(); i++) {
                const log::RecordWrapConst lrec(&packH, i);
                if (!lrec.hasData()) continue;
                /* Read IO data. */
                if (!ctrl.isNext()) logAndThrow("Client said error.");
                ctrl.reset();
                cdIo.recv(packet);
                if (cdIo.isCompressed()) cdIo = cdIo.uncompress();
                if (cdIo.rawSize() != lrec.ioSizePb() * pbs_) {
                    logAndThrow("invalid size of log IO. expected %zu received %zu."
                                , lrec.ioSizePb() * pbs_, cdIo.rawSize());
                }
                /* Convert Log to diff. */
                const log::BlockDataWrapT<const char> blockData(
                    pbs_, cdIo.rawData(), lrec.ioSizePb());
                const log::PackIoWrapConst packIo(&lrec, &blockData);
                diff::RecordRaw drec;
                diff::IoData dio;
                if (convertLogToDiff(packIo, drec, dio)) {
                    /* Add to memory data. */
                    wdiffM.add(drec, std::move(dio));
                }
            }
            return true;
        }
    };

public:
    using Protocol :: Protocol;

    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: lv identifier.
     *
     *   now editing
     */
    void runAsClient(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        Client client(sock, logger, params);
        client.run();
    }
    /**
     *
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void runAsServer(cybozu::Socket &sock, Logger &logger,
                     const std::vector<std::string> &params) override {
        Server server(sock, logger, params);
        server.run();
    }
};
#endif

/**
 * Protocol factory.
 */
class ProtocolFactory
{
private:
    using Map = std::map<std::string, std::unique_ptr<Protocol> >;
    Map map_;

public:
    static ProtocolFactory &getInstance() {
        static ProtocolFactory factory;
        return factory;
    }
    Protocol *find(const std::string &name) {
        Map::iterator it = map_.find(name);
        if (it == map_.end()) return nullptr;
        return it->second.get();
    }

private:
#define DECLARE_PROTOCOL(name, cls)                                     \
    map_.insert(std::make_pair(#name, std::unique_ptr<cls>(new cls(#name))))

    ProtocolFactory() : map_() {
        DECLARE_PROTOCOL(echo, EchoProtocol);
        DECLARE_PROTOCOL(dirty-full-sync, DirtyFullSyncProtocol);
        DECLARE_PROTOCOL(dirty-hash-sync, DirtyHashSyncProtocol);
        DECLARE_PROTOCOL(wlog-send, LogSendProtocol);
        //DECLARE_PROTOCOL(wdiff-send, DiffSendProtocol);
        /* now editing */
    }
#undef DECLARE_PROTOCOL
};

/**
 * Run a protocol as a client.
 */
static inline void runProtocolAsClient(
    cybozu::Socket &sock, const std::string &clientId, const std::string &protocolName,
    const std::vector<std::string> &params)
{
    packet::Packet packet(sock);
    packet.write(clientId);
    packet.write(protocolName);
    packet::Version ver(sock);
    ver.send();
    std::string serverId;
    packet.read(serverId);

    Logger logger(clientId, serverId);

    packet::Answer ans(sock);
    int err;
    std::string msg;
    if (!ans.recv(&err, &msg)) {
        logger.warn("received NG: err %d msg %s", err, msg.c_str());
        return;
    }

    Protocol *protocol = ProtocolFactory::getInstance().find(protocolName);
    if (!protocol) {
        throw std::runtime_error("receive OK but protocol not found.");
    }
    /* Client can throw an error. */
    protocol->runAsClient(sock, logger, params);
}

/**
 * Run a protocol as a server.
 *
 * TODO: arguments for server data.
 */
static inline void runProtocolAsServer(
    cybozu::Socket &sock, const std::string &serverId, const std::string &baseDirStr) noexcept
{
    ::printf("runProtocolAsServer start\n"); /* debug */
    packet::Packet packet(sock);
    std::string clientId;
    packet.read(clientId);
    ::printf("clientId: %s\n", clientId.c_str()); /* debug */
    std::string protocolName;
    packet.read(protocolName);
    ::printf("protocolName: %s\n", protocolName.c_str()); /* debug */
    packet::Version ver(sock);
    bool isVersionSame = ver.recv();
    ::printf("isVersionSame: %d\n", isVersionSame); /* debug */
    packet.write(serverId);

    Logger logger(serverId, clientId);
    Protocol *protocol = ProtocolFactory::getInstance().find(protocolName);

    packet::Answer ans(sock);
    if (!protocol) {
        std::string msg = cybozu::util::formatString(
            "There is not such protocol %s.", protocolName.c_str());
        logger.info(msg);
        ans.ng(1, msg);
        return;
    }
    if (!isVersionSame) {
        std::string msg = cybozu::util::formatString(
            "Version differ: server %" PRIu32 "", packet::VERSION);
        logger.info(msg);
        ans.ng(1, msg);
        return;
    }
    ans.ok();

    logger.info("initial negotiation succeeded: %s", protocolName.c_str());
    try {
        protocol->runAsServer(sock, logger, { baseDirStr });
    } catch (std::exception &e) {
        logger.error("[%s] runProtocolAsServer failed: %s.", e.what());
    } catch (...) {
        logger.error("[%s] runProtocolAsServer failed: unknown error.");
    }
}

} //namespace walb
