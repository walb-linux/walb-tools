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
#include "walb_logger.hpp"
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
#include "server_util.hpp"
#include "walb_log_net.hpp"
#include "memory_buffer.hpp"
#include "init_vol.hpp"

namespace walb {
namespace protocol {

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

    virtual void run(cybozu::Socket &, Logger &, const std::atomic<bool>&,
                     const std::vector<std::string> &) {}
};

namespace echo {

/**
 * Simple echo protocol.
 */
class Client : public Protocol
{
public:
    using Protocol :: Protocol;
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &,
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
};

class Server : public Protocol
{
public:
    using Protocol :: Protocol;
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &,
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

} //namespace echo

/**
 * Utility class for protocols.
 */
class ProtocolData
{
protected:
    const std::string &protocolName_;
    cybozu::Socket &sock_;
    Logger &logger_;
    const std::atomic<bool> &forceQuit_;
    const std::vector<std::string> &params_;
public:
    ProtocolData(const std::string &protocolName,
                 cybozu::Socket &sock, Logger &logger,
                 const std::atomic<bool> &forceQuit,
                 const std::vector<std::string> &params)
        : protocolName_(protocolName)
        , sock_(sock), logger_(logger)
        , forceQuit_(forceQuit), params_(params) {}
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
 * Dirty full-sync.
 *
 * Client: walb-client.
 * Server: walb-server.
 */
namespace dirty_full_sync {

class SharedData : public ProtocolData
{
protected:
    std::string name_;
    uint64_t sizeLb_;
    uint16_t bulkLb_;
    uint64_t gid_;
public:
    using ProtocolData :: ProtocolData;
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

class ClientRunner : public SharedData
{
private:
    std::string path_;
public:
    using SharedData :: SharedData;
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
            uint16_t lb = std::min<uint64_t>(bulkLb_, remainingLb);
            size_t size = lb * LOGICAL_BLOCK_SIZE;
            bd.read(&buf[0], size);
            packet.write(lb);
            packet.write(&buf[0], size);
            remainingLb -= lb;
        }
    }
};

class ServerRunner : public SharedData
{
private:
    cybozu::FilePath baseDir_;
public:
    using SharedData :: SharedData;
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

        uint64_t c = 0;
        uint64_t remainingLb = sizeLb_;
        while (0 < remainingLb) {
            uint16_t lb = std::min<uint64_t>(bulkLb_, remainingLb);
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

/**
 * Dirty full sync.
 *
 * TODO: exclusive access control.
 */
class Client : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: full path of lv.
     *   [1] :: string: lv identifier.
     *   [2] :: uint64_t: lv size [byte].
     *   [3] :: uint32_t: bulk size [byte]. less than uint16_t * LOGICAL_BLOCK_SIZE;
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        ClientRunner c(name_, sock, logger, forceQuit, params);
        c.run();
    }
};

class Server : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        ServerRunner s(name_, sock, logger, forceQuit, params);
        s.run();
    }
};

} // namespace dirty_full_sync

/**
 * Dirty hash-sync.
 *
 * Client: walb-client.
 * Server: walb-server.
 */
namespace dirty_hash_sync {

class SharedData : public ProtocolData
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

class ClientRunner : public SharedData
{
private:
    std::string path_;
public:
    using SharedData::SharedData;
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
                hashQ.fail();
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

            uint16_t lb = std::min<uint64_t>(bulkLb_, remainingLb);
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
            } catch (...) {
                logger_.error("createPack failed.");
                isError = true;
                hashQ.fail();
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

class ServerRunner : public SharedData
{
private:
    cybozu::FilePath baseDir_;
public:
    using SharedData::SharedData;
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

            uint16_t lb = std::min<uint64_t>(bulkLb_, remainingLb);
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

class Client : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: full path of lv.
     *   [1] :: string: lv identifier.
     *   [2] :: uint64_t: lv size [byte].
     *   [3] :: uint32_t: bulk size [byte]. less than uint16_t * LOGICAL_BLOCK_SIZE;
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        ClientRunner c(name_, sock, logger, forceQuit, params);
        c.run();
    }
};

class Server : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     *
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        ServerRunner s(name_, sock, logger, forceQuit, params);
        s.run();
    }
};

} // namespace dirty_hash_sync

/**
 * Wlog-send protocol.
 *
 * Client: walb-worker.
 * Server: walb-proxy.
 */
namespace wlog_send {

using Block = std::shared_ptr<uint8_t>;
using BoundedQ = cybozu::thread::BoundedQueue<log::CompressedData, true>;
static constexpr size_t Q_SIZE = 10; /* TODO: do not hard cord. */

class SharedData : public ProtocolData
{
protected:
    std::string name_;
    /* Server will check hash-sync occurrence with the uuid. */
    cybozu::Uuid uuid_;
    MetaDiff diff_;
    uint32_t pbs_; /* physical block size */
    uint32_t salt_; /* checksum salt. */
    uint64_t bgnLsid_; /* bgnLsid <= endLsid must be satisfied. */
    uint64_t endLsid_; /* -1 means unknown. */
public:
    using ProtocolData::ProtocolData;
    void checkParams() const {
        if (name_.empty()) logAndThrow("name param empty.");
        if (!diff_.isValid()) logAndThrow("Invalid meta diff.");
        if (!::is_valid_pbs(pbs_)) logAndThrow("Invalid pbs.");
        if (endLsid_ < bgnLsid_) logAndThrow("Invalid lsids.");
    }
    std::string str() const {
        return cybozu::util::formatString(
            "name %s uuid %s diff (%s) pbs %" PRIu32 " salt %08x lsid (%" PRIu64 ", %" PRIu64 ")"
            , name_.c_str(), uuid_.str().c_str(), diff_.str().c_str()
            , pbs_, salt_, bgnLsid_, endLsid_);
    }
};

/**
 * Client must call
 * (1) setParams().
 * (2) prepare().
 * (3) push() multiple times.
 * (4) sync() or error().
 */
class ClientRunner : public SharedData
{
private:
    log::Sender sender_;

public:
    ClientRunner(const std::string &protocolName,
                 cybozu::Socket &sock, Logger &logger, const std::atomic<bool> &forceQuit,
                 const std::vector<std::string> &params)
        : SharedData(protocolName, sock, logger, forceQuit, params)
        , sender_(sock, logger) {}
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
                   uint32_t pbs, uint32_t salt, uint64_t bgnLsid, uint64_t endLsid) {
        name_ = name;
        ::memcpy(uuid_.rawData(), uuid, uuid_.rawSize());
        diff_ = diff;
        pbs_ = pbs;
        salt_ = salt;
        bgnLsid_ = bgnLsid;
        endLsid_ = endLsid;
        checkParams();
    }
    /**
     * Prepare worker threads.
     */
    void prepare() {
        negotiate();
        sender_.setParams(pbs_, salt_);
        sender_.start();
    }
    /**
     * Push a log pack.
     * Do not send end logpack header by yourself. Use sync() instead.
     */
    void pushHeader(const log::PackHeader &header) {
        sender_.pushHeader(header);
    }
    void pushIo(const log::PackHeader &header, size_t recIdx, const log::BlockData &blockD) {
        sender_.pushIo(header, recIdx, blockD);
    }
    /**
     * Notify the input has reached the end and finalize the protocol.
     */
    void sync() {
        sender_.sync();
        packet::Ack ack(sock_);
        ack.recv();
    }
    /**
     * Notify an error and finalize the protocol.
     */
    void fail() noexcept {
        sender_.fail();
    }
private:
    void negotiate() {
        packet::Packet packet(sock_);
        packet.write(name_);
        packet.write(uuid_);
        packet.write(diff_);
        packet.write(pbs_);
        packet.write(salt_);
        packet.write(bgnLsid_);
        packet.write(endLsid_);
        packet::Answer ans(sock_);
        int err; std::string msg;
        if (!ans.recv(&err, &msg)) {
            logAndThrow("negotiation failed: %d %s", err, msg.c_str());
        }
    }
};

class ServerRunner : public SharedData
{
private:
    cybozu::FilePath baseDir_;
    log::Receiver receiver_;

public:
    ServerRunner(const std::string &protocolName,
                 cybozu::Socket &sock, Logger &logger,
                 const std::atomic<bool> &forceQuit,
                 const std::vector<std::string> &params)
        : SharedData(protocolName, sock, logger, forceQuit, params),
          receiver_(sock, logger) {}
    void run() {
        loadParams();
        negotiate();
        logger_.info(
            "wlog-send %s %s ((%" PRIu64 ", %" PRIu64 "), (%" PRIu64", %" PRIu64 ")) "
            "%" PRIu32 " %" PRIu32 " (%" PRIu64 ", %" PRIu64 ") "
            , name_.c_str(), uuid_.str().c_str()
            , diff_.snap0().gid0(), diff_.snap0().gid1()
            , diff_.snap1().gid0(), diff_.snap1().gid1()
            , pbs_, salt_, bgnLsid_, endLsid_);
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
        packet.read(bgnLsid_);
        packet.read(endLsid_);
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

        receiver_.setParams(pbs_, salt_);
        receiver_.start();

        while (readLogpackAndAdd(wdiffM)) {}
        wdiffM.writeTo(tmpFile.fd());
        tmpFile.save(pd.getDiffPathToAdd(diff_).str());
        pd.add(diff_);

        packet::Ack ack(sock_);
        ack.send();

        /* TODO: notify or invoke a wdiff-sender thread inside the proxy daemon. */
    }
private:
    /**
     * Read a logpack, convert to diff data and
     * add all the IOs in the pack into the memory data.
     *
     * RETURN:
     *   true if a pack is successfully merged into the memory data.
     *   false if the input stream reached the end.
     */
    bool readLogpackAndAdd(diff::MemoryData &wdiffM) {
        /* Pack header block */
        auto blk = cybozu::util::allocateBlocks<uint8_t>(pbs_, pbs_);
        log::PackHeaderRaw packH(blk, pbs_, salt_);
        if (!receiver_.popHeader(packH.header())) return false;

        /* Pack IO data. */
        log::BlockDataVec blockD;
        for (size_t i = 0; i < packH.nRecords(); i++) {
            receiver_.popIo(packH.header(), i, blockD);
            const log::RecordWrapConst lrec(&packH, i);
            const log::PackIoWrapConst packIo(&lrec, &blockD);
            diff::RecordRaw drec;
            diff::IoData dio;
            if (convertLogToDiff(packIo, drec, dio)) {
                wdiffM.add(drec, std::move(dio));
            }
        }
        return true;
    }
};

class Client : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: lv identifier.
     *
     *   now editing
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        ClientRunner c(name_, sock, logger, forceQuit, params);
        c.run();
    }
};

class Server : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     *
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        ServerRunner s(name_, sock, logger, forceQuit, params);
        s.run();
    }
};

} // namespace wlog_send

namespace status {

#if 0
class Client : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        /* now editing */
    }
};
#endif

#if 0
class WorkerAsServer : public Protocol
{
public:
    using Protocol :: Protocol;
    /**
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        /* now editing */
    }
};
#endif

#if 0
class ProxyAsServer : public Protocol
{
    using Protocol :: Protocol;
    /**
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        /* now editing */
    }
};
#endif

#if 0
class ServerAsServer : public Protocol
{
    using Protocol :: Protocol;
    /**
     */
    void run(cybozu::Socket &sock, Logger &logger,
             const std::atomic<bool> &forceQuit,
             const std::vector<std::string> &params) override {
        /* now editing */
    }
};
#endif

} // namespace status

enum class ProtocolName
{
    ECHO,
    STATUS,
    DIRTY_FULL_SYNC,
    DIRTY_HASH_SYNC,
    WLOG_SEND,
    WDIFF_SEND,
    GRACEFUL_SHUTDOWN,
    FORCE_SHUTDOWN,
    INIT_VOL,
};

const std::map<ProtocolName, std::string> PROTOCOL_TYPE_MAP =
{
    {ProtocolName::ECHO, "echo"},
    {ProtocolName::GRACEFUL_SHUTDOWN, "graceful-shutdown"},
    {ProtocolName::FORCE_SHUTDOWN, "force-shutdown"},
    {ProtocolName::STATUS, "status"},
    {ProtocolName::DIRTY_FULL_SYNC, "dirty-full-sync"},
    {ProtocolName::DIRTY_HASH_SYNC, "dirty-diff-sync"},
    {ProtocolName::WLOG_SEND, "wlog-send"},
    {ProtocolName::WDIFF_SEND, "wdiff-send"},
    {ProtocolName::INIT_VOL, "init-vol"},
};

/**
 * Protocol factory.
 */
class ProtocolFactory
{
private:
    using Map = std::map<std::string, std::unique_ptr<Protocol> >;
    Map clientMap_;
    Map serverMap_;

public:
    static ProtocolFactory &getInstance() {
        static ProtocolFactory factory;
        return factory;
    }
    Protocol *findClient(const std::string &name) { return find<1>(name); }
    Protocol *findServer(const std::string &name) { return find<0>(name); }
    template <class Cls>
    void registerClient(ProtocolName name) {
        registerProtocol<1, Cls>(PROTOCOL_TYPE_MAP.at(name));
    }
    template <class Cls>
    void registerServer(ProtocolName name) {
        registerProtocol<0, Cls>(PROTOCOL_TYPE_MAP.at(name));
    }
private:
    template <bool isClient>
    Map &getMap() {
        return isClient ? clientMap_ : serverMap_;
    }
    template <bool isClient>
    Protocol *find(const std::string &name) {
        Map &m = getMap<isClient>();
        Map::iterator it = m.find(name);
        if (it == m.end()) return nullptr;
        return it->second.get();
    }
    template<bool isClient, class Cls>
    void registerProtocol(const std::string &name) {
        Map &m = getMap<isClient>();
        m.emplace(name, std::unique_ptr<Cls>(new Cls(name)));
    }
    ProtocolFactory() {
        /* You must call registerProtocol() as your role. */
    }
};

static inline void registerProtocolsAsClient()
{
    ProtocolFactory &factory = ProtocolFactory::getInstance();
    factory.registerClient<echo::Client>(ProtocolName::ECHO);
    //factory.registerClient<status::Client>(ProtocolName::STATUS);
    factory.registerClient<dirty_full_sync::Client>(ProtocolName::DIRTY_FULL_SYNC);
    factory.registerClient<dirty_hash_sync::Client>(ProtocolName::DIRTY_HASH_SYNC);
    /* now editing */
}

static inline void registerProtcolsAsWorker()
{
    ProtocolFactory &factory = ProtocolFactory::getInstance();
    factory.registerServer<echo::Server>(ProtocolName::ECHO);
    //factory.registerServer<status::WorkerAsServer>(ProtocolName::STATUS);
    factory.registerClient<wlog_send::Client>(ProtocolName::WLOG_SEND);
    /* now editing */
}

static inline void registerProtcolsAsProxy()
{
    ProtocolFactory &factory = ProtocolFactory::getInstance();
    factory.registerServer<echo::Server>(ProtocolName::ECHO);
    //factory.registerServer<status::ProxyAsServer>(ProtocolName::STATUS);
    factory.registerServer<wlog_send::Server>(ProtocolName::WLOG_SEND);
    //factory.registerClient<wdiff_send::Client>(ProtocolName::WDIFF_SEND);
    /* now editing */
}

static inline void registerProtcolsAsServer()
{
    ProtocolFactory &factory = ProtocolFactory::getInstance();
    factory.registerServer<echo::Client>(ProtocolName::ECHO);
    //factory.registerServer<status::ServerAsServer>(ProtocolName::STATUS);
    factory.registerServer<dirty_full_sync::Server>(ProtocolName::DIRTY_FULL_SYNC);
    factory.registerServer<dirty_hash_sync::Server>(ProtocolName::DIRTY_HASH_SYNC);
    //factory.registerServer<wdiff_send::Server>(ProtocolName::WDIFF_SEND);
    /* now editing */
}

/**
 * RETURN:
 *   Server ID.
 */
static inline std::string run1stNegotiateAsClient(
    cybozu::Socket &sock,
    const std::string &clientId, const std::string &protocolName)
{
    packet::Packet packet(sock);
    packet.write(clientId);
    packet.write(protocolName);
    packet::Version ver(sock);
    ver.send();
    std::string serverId;
    packet.read(serverId);

    ProtocolLogger logger(clientId, serverId);
    packet::Answer ans(sock);
    int err;
    std::string msg;
    if (!ans.recv(&err, &msg)) {
        std::string s = cybozu::util::formatString(
            "received NG: err %d msg %s", err, msg.c_str());
        logger.error(s);
        throw std::runtime_error(s);
    }
    return serverId;
}

static inline void clientDispatch(const std::string& protocolName, cybozu::Socket& sock, ProtocolLogger& logger,
    const std::atomic<bool> &forceQuit, const std::vector<std::string> &params)
{
    if (protocolName == "init-vol") {
        clientInitVol(sock, logger, forceQuit, params);
        return;
    }
    throw cybozu::Exception("dispatch:receive OK but protocol not found.") << protocolName;
}

/**
 * Run a protocol as a client.
 */
static inline void runProtocolAsClient(
    cybozu::Socket &sock, const std::string &clientId,
    const std::atomic<bool> &forceQuit,
    const std::string &protocolName, const std::vector<std::string> &params)
{
    std::string serverId = run1stNegotiateAsClient(sock, clientId, protocolName);
    ProtocolLogger logger(clientId, serverId);

    clientDispatch(protocolName, sock, logger, forceQuit, params);
}

/**
 * @clientId will be set.
 * @protocol will be set.
 *
 * This function will process shutdown protocols.
 * For other protocols, this function will do only the common negotiation.
 *
 * RETURN:
 *   true if the protocol has finished or failed that is there is nothing to do.
 *   otherwise false.
 */
static inline bool run1stNegotiateAsServer(
    cybozu::Socket &sock, const std::string &serverId,
    std::string &clientId, Protocol **protocolPtr,
    std::atomic<cybozu::server::ControlFlag> &ctrlFlag)
{
    packet::Packet packet(sock);

    LOGi_("run1stNegotiateAsServer start\n");
    packet.read(clientId);
    LOGi_("clientId: %s\n", clientId.c_str());
    std::string protocolName;
    packet.read(protocolName);
    LOGi_("protocolName: %s\n", protocolName.c_str());
    packet::Version ver(sock);
    bool isVersionSame = ver.recv();
    LOGi_("isVersionSame: %d\n", isVersionSame);
    packet.write(serverId);

    ProtocolLogger logger(serverId, clientId);
    packet::Answer ans(sock);

    /* Server shutdown commands. */
    if (protocolName == PROTOCOL_TYPE_MAP.at(ProtocolName::GRACEFUL_SHUTDOWN)) {
        ctrlFlag = cybozu::server::ControlFlag::GRACEFUL_SHUTDOWN;
        logger.info("graceful shutdown.");
        ans.ok();
        return true;
    } else if (protocolName == PROTOCOL_TYPE_MAP.at(ProtocolName::FORCE_SHUTDOWN)) {
        ctrlFlag = cybozu::server::ControlFlag::FORCE_SHUTDOWN;
        logger.info("force shutdown.");
        ans.ok();
        return true;
    }

    /* Find protocol. */
    *protocolPtr = ProtocolFactory::getInstance().findServer(protocolName);
    if (!*protocolPtr) {
        std::string msg = cybozu::util::formatString(
            "There is not such protocol: %s", protocolName.c_str());
        logger.warn(msg);
        ans.ng(1, msg);
        return true;
    }
    if (!isVersionSame) {
        std::string msg = cybozu::util::formatString(
            "Version differ: client %" PRIu32 " server %" PRIu32 ""
            , ver.get(), packet::VERSION);
        logger.warn(msg);
        ans.ng(1, msg);
        return true;
    }
    ans.ok();
    logger.info("initial negotiation succeeded: %s", protocolName.c_str());
    return false;
}

/**
 * Run a protocol as a server.
 *
 * TODO: arguments for server data.
 */
static inline void runProtocolAsServer(
    cybozu::Socket &sock, const std::string &serverId, const std::string &baseDirStr,
    const std::atomic<bool> &forceQuit,
    std::atomic<cybozu::server::ControlFlag> &ctrlFlag) noexcept
{
    std::string clientId, protocolName;
    Protocol *protocol;
    if (run1stNegotiateAsServer(sock, serverId, clientId, &protocol, ctrlFlag)) {
        /* The protocol has finished or failed. */
        return;
    }
    ProtocolLogger logger(serverId, clientId);
    try {
        protocol->run(sock, logger, forceQuit, { baseDirStr });
    } catch (std::exception &e) {
        logger.error("runlAsServer failed: %s", e.what());
    } catch (...) {
        logger.error("runAsServer failed: unknown error.");
    }
}

}} //namespace walb::protocol
