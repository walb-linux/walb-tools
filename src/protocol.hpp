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
#include "walb_diff_pack.hpp"
#include "walb_diff_compressor.hpp"
#include "thread_util.hpp"
#include "murmurhash3.hpp"

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
            walb::ServerData sd(baseDir_.str(), name_);
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
        enum class CtrlMsg : uint8_t {
            Continue = 0, End = 1, Error = 2,
        };
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
            walb::ConverterQueue convQ(4, 2, true, ::WALB_DIFF_CMPR_SNAPPY); /* TODO: do not hardcode. */
            walb::diff::Packer packer;
            packer.setMaxPackSize(2U << 20); /* 2MiB. TODO: do not hardcode. */
            packer.setMaxNumRecords(10); /* TODO: do not hardcode. */
            std::atomic<bool> isError(false);

            /* Read hash data */
            auto receiveHash = [this, &packet, &hashQ](uint64_t num) {
                try {
                    for (uint64_t i = 0; i < num; i++) {
                        cybozu::murmurhash3::Hash h;
                        packet.read(h.ptr(), h.size());
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
                logger_.info("try to push pack %zu", nPack);
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
                    uint8_t ctrl = static_cast<uint8_t>(CtrlMsg::Continue);
                    size_t c = 0;
                    logger_.info("try to pop from convQ"); /* debug */
                    std::unique_ptr<char[]> up = convQ.pop();
                    logger_.info("popped from convQ"); /* debug */
                    while (up) {
                        packet.write(ctrl);
                        diff::MemoryPack mpack(up.get());
                        uint32_t packSize = mpack.size();
                        packet.write(packSize);
                        packet.write(mpack.rawPtr(), packSize);
                        logger_.info("send pack %zu", c++);
                        up = convQ.pop();
                    }
                    ctrl = static_cast<uint8_t>(isError ? CtrlMsg::Error : CtrlMsg::End);
                    packet.write(ctrl);
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
            walb::ServerData sd(baseDir_.str(), name_);
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
            walb::ServerData sd(baseDir_.str(), name_);
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
            walb::diff::VirtualFullScanner virtLv(bd.getFd(), diffPaths);
            std::vector<char> buf0(bulkLb_ * LOGICAL_BLOCK_SIZE);

            logger_.info("hoge2");

            auto readBulkAndSendHash = [this, &virtLv, &packet, &buf0, &hasher](
                uint64_t &offLb, uint64_t &remainingLb) {

                uint16_t lb = bulkLb_;
                if (remainingLb < bulkLb_) lb = remainingLb;
                size_t size = lb * LOGICAL_BLOCK_SIZE;
                virtLv.read(&buf0[0], size);
                cybozu::murmurhash3::Hash h0 = hasher(&buf0[0], size);
                packet.write(h0.ptr(), h0.size());
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
                    auto readCtrl = [&](CtrlMsg &ctrl) {
                        uint8_t v;
                        packet.read(v);
                        ctrl = static_cast<CtrlMsg>(v);
                    };

                    CtrlMsg ctrl;
                    std::vector<char> buf;
                    readCtrl(ctrl);
                    size_t c = 0;
                    while (ctrl == CtrlMsg::Continue) {
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
                        readCtrl(ctrl);
                    }
                    if (ctrl == CtrlMsg::Error) {
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
    cybozu::Socket &sock, const std::string &serverId, const std::string &baseDirStr)
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

    logger.info("initial negociation succeeded: %s", protocolName.c_str());
    try {
        protocol->runAsServer(sock, logger, { baseDirStr });
    } catch (std::exception &e) {
        /* Server must not throw an error. */
        logger.error("[%s] runProtocolAsServer failed: %s.", e.what());
    }
}

} //namespace walb
