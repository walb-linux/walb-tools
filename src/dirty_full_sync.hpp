#pragma once
#include "protocol_data.hpp"

namespace walb {

/**
 * Dirty full-sync.
 *
 * Client: walb-client.
 * Server: walb-server.
 */
namespace dirty_full_sync {

class SharedData : public walb::protocol::ProtocolData
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

} // namespace walb::dirty_full_sync

/**
 * @params using cybozu::loadFromStr() to convert.
 *   [0] :: string: full path of lv.
 *   [1] :: string: lv identifier.
 *   [2] :: uint64_t: lv size [byte].
 *   [3] :: uint32_t: bulk size [byte]. less than uint16_t * LOGICAL_BLOCK_SIZE;
 */
static inline void clientDirtyFullSync(
    cybozu::Socket &sock, ProtocolLogger &logger,
    const std::atomic<bool> &forceQuit,
    const std::vector<std::string> &params)
{
    dirty_full_sync::ClientRunner c("dirty-full-sync", sock, logger, forceQuit, params);
    c.run();
}

/**
 */
static inline void serverDirtyFullSync(
    cybozu::Socket &sock, ProtocolLogger &logger,
    const std::string &baseDirStr,
    const std::atomic<bool> &forceQuit,
    std::atomic<walb::server::ProcessStatus> &/*procStat*/)
{
    dirty_full_sync::ServerRunner s("dirty-full-sync", sock, logger, forceQuit, { baseDirStr });
    s.run();
}

} // namespace walb
