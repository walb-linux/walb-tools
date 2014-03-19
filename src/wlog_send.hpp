#pragma once
#include "protocol.hpp"
#include "protocol_data.hpp"
#include "uuid.hpp"
#include "tmp_file.hpp"
#include "file_path.hpp"
#include "walb_log_compressor.hpp"
#include "walb_log_file.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_converter.hpp"
#include "proxy_data.hpp"

namespace walb {

/**
 * Wlog-send protocol.
 *
 * Client: walb-worker.
 * Server: walb-proxy.
 */
namespace wlog_send {

static const std::string PROTOCOL_NAME("wlog-send");

using Block = std::shared_ptr<uint8_t>;
using BoundedQ = cybozu::thread::BoundedQueue<log::CompressedData>;
static constexpr size_t Q_SIZE = 10; /* TODO: do not hard cord. */

class SharedData : public walb::protocol::ProtocolData
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
    void verifyParams() const {
        if (name_.empty()) logAndThrow("name param empty.");
        diff_.verify();
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
    ClientRunner(cybozu::Socket &sock, Logger &logger, const std::atomic<bool> &forceQuit,
                 const std::vector<std::string> &params)
        : SharedData(PROTOCOL_NAME, sock, logger, forceQuit, params)
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
        verifyParams();
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
    ServerRunner(cybozu::Socket &sock, Logger &logger,
                 const std::atomic<bool> &forceQuit,
                 const std::vector<std::string> &params)
        : SharedData("wlog-send", sock, logger, forceQuit, params),
          receiver_(sock, logger) {}
    void run() {
        loadParams();
        negotiate();
        logger_.info(
            "wlog-send %s %s ((%" PRIu64 ", %" PRIu64 "), (%" PRIu64", %" PRIu64 ")) "
            "%" PRIu32 " %" PRIu32 " (%" PRIu64 ", %" PRIu64 ") "
            , name_.c_str(), uuid_.str().c_str()
            , diff_.snapB.gidB, diff_.snapB.gidE
            , diff_.snapE.gidB, diff_.snapE.gidE
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
        verifyParams();
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
            DiffRecord drec;
            diff::IoData dio;
            if (convertLogToDiff(lrec, blockD, drec, dio)) {
                wdiffM.add(drec, std::move(dio));
            }
        }
        return true;
    }
};

} // namespace wlog_send

/**
 * WlogSend protocol client.
 */
inline void clientWlogSend(
    cybozu::Socket& sock, ProtocolLogger& logger,
    const std::atomic<bool> &forceQuit, const std::vector<std::string> &params)
{
    wlog_send::ClientRunner c(sock, logger, forceQuit, params);
    c.run();
}

/**
 * WlogSend protocol server.
 */
inline void serverWlogSend(cybozu::Socket &sock,
    ProtocolLogger& logger,
    const std::string &baseDirStr,
    const std::atomic<bool> &forceQuit,
    std::atomic<walb::server::ProcessStatus> &/*procStat*/)
{
    wlog_send::ServerRunner s(sock, logger, forceQuit, { baseDirStr });
    s.run();
}

} // namespace walb
