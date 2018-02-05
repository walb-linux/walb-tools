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
#include <unordered_map>
#include "cybozu/socket.hpp"
#include "packet.hpp"
#include "util.hpp"
#include "walb_logger.hpp"
#include "walb_util.hpp"
#include "process.hpp"
#include "command_param_parser.hpp"

namespace walb {

/**
 * Message string.
 */
const char *const msgOk = "ok"; // used for synchronous protocol.
const char *const msgAccept = "accept"; // used for asynchronous protocol.

const char *const msgTooNewDiff = "too-new-diff";
const char *const msgTooOldDiff = "too-old-diff";
const char *const msgDifferentUuid = "different-uuid";
const char *const msgStopped = "stopped";
const char *const msgWdiffRecv = "wdiff-recv";
const char *const msgSyncing = "syncing";
const char *const msgArchiveNotFound = "archive-not-found";
const char *const msgSmallerLvSize = "smaller-lv-size";

/**
 * Host type.
 */
const char *const controllerHT = "controller";
const char *const storageHT = "storage";
const char *const proxyHT = "proxy";
const char *const archiveHT = "archive";

/**
 * Command name.
 */
const char *const statusCN = "status";
const char *const initVolCN = "init-vol";
const char *const clearVolCN = "clear-vol";
const char *const resetVolCN = "reset-vol";
const char *const startCN = "start";
const char *const stopCN = "stop";
const char *const fullBkpCN = "full-bkp";
const char *const hashBkpCN = "hash-bkp";
const char *const snapshotCN = "snapshot";
const char *const archiveInfoCN = "archive-info";
const char *const restoreCN = "restore";
const char *const delRestoredCN = "del-restored";
const char *const delColdCN = "del-cold";
const char *const replicateCN = "replicate";
const char *const applyCN = "apply";
const char *const mergeCN = "merge";
const char *const resizeCN = "resize";
const char *const shutdownCN = "shutdown";
const char *const kickCN = "kick";
const char *const blockHashCN = "bhash";
const char *const virtualFullScanCN = "virt-full-scan";
const char *const dbgReloadMetadataCN = "dbg-reload-metadata";
const char *const dbgSetUuidCN = "dbg-set-uuid";
const char *const dbgSetStateCN = "dbg-set-state";
const char *const dbgSetBaseCN = "dbg-set-base";
const char *const getCN = "get";
const char *const execCN = "exec";
const char *const disableSnapshotCN = "disable-snapshot";
const char *const enableSnapshotCN = "enable-snapshot";
const char *const dbgDumpLogpackHeaderCN = "dbg-dump-logpack-header";
const char *const setFullScanBpsCN = "set-full-scan-bps";
const char *const gcDiffCN = "gc-diff";
const char *const debugCN = "debug";
const char *const sleepCN = "sleep";

/**
 * Target name of 'get' command.
 */
const char *const isOverflowTN = "is-overflow";
const char *const logUsageTN = "log-usage";
const char *const isWdiffSendErrorTN = "is-wdiff-send-error";
const char *const numActionTN = "num-action";
const char *const stateTN = "state";
const char *const hostTypeTN = "host-type";
const char *const volTN = "vol";
const char *const pidTN = "pid";
const char *const diffTN = "diff";
const char *const proxyDiffTN = "proxy-diff";
const char *const applicableDiffTN = "applicable-diff";
const char *const totalDiffSizeTN = "total-diff-size";
const char *const numDiffTN = "num-diff";
const char *const existsDiffTN = "exists-diff";
const char *const existsBaseImageTN = "exists-base-image";
const char *const restoredTN = "restored";
const char *const coldTN = "cold";
const char *const restorableTN = "restorable";
const char *const uuidTN = "uuid";
const char *const archiveUuidTN = "archive-uuid";
const char *const baseTN = "base";
const char *const baseAllTN = "base-all";
const char *const volSizeTN = "vol-size";
const char *const progressTN = "progress";
const char *const volumeGroupTN = "volume-group";
const char *const thinpoolTN = "thinpool";
const char *const allActionsTN = "all-actions";
const char *const getMetaSnapTN = "meta-snap";
const char *const getMetaStateTN = "meta-state";
const char *const getLatestSnapTN = "latest-snap";
const char *const getTsDeltaTN = "ts-delta";
const char *const getHandlerStatTN = "handler-stat";

/**
 * Internal protocol name.
 */
const char *const dirtyFullSyncPN = "dirty-full-sync";
const char *const dirtyHashSyncPN = "dirty-hash-sync";
const char *const wlogTransferPN = "wlog-transfer";
const char *const wdiffTransferPN = "wdiff-transfer";
const char *const replSyncPN = "repl-sync";
const char *const gatherLatestSnapPN = "gather-latest-snap";


cybozu::SocketAddr parseSocketAddr(const std::string &addrPort);
std::vector<cybozu::SocketAddr> parseMultiSocketAddr(const std::string &multiAddrPort);


namespace protocol {


/**
 * RETURN:
 *   Server ID.
 */
std::string run1stNegotiateAsClient(
    cybozu::Socket &sock,
    const std::string &clientId, const std::string &protocolName);

/**
 * Parameters for commands as a client.
 */
struct ClientParams
{
    cybozu::Socket &sock;
    ProtocolLogger &logger;
    const StrVec &params;

    ClientParams(
        cybozu::Socket &sock0,
        ProtocolLogger &logger0,
        const StrVec &params0)
        : sock(sock0)
        , logger(logger0)
        , params(params0) {
    }
};

/**
 * Client handler type.
 */
using ClientHandler = void (*)(ClientParams &);

/**
 * @sock socket for the connection.
 * @protocolName will be set.
 * @clientId will be set.
 *
 * This function will do only the common negotiation.
 */
void run1stNegotiateAsServer(
    cybozu::Socket &sock, const std::string &serverId,
    std::string &protocolName, std::string &clientId);

/**
 * Parameters for commands as a server.
 */
struct ServerParams
{
    cybozu::Socket &sock;
    const std::string& clientId;
    walb::ProcessStatus &ps;

    ServerParams(
        cybozu::Socket &sock,
        const std::string &clientId,
        walb::ProcessStatus &ps)
        : sock(sock)
        , clientId(clientId)
        , ps(ps) {
    }
};

void shutdownClient(ClientParams &p);
void shutdownServer(ServerParams &p);

void sleepClient(ClientParams &p);
void sleepServer(ClientParams &p);


/**
 * Statistics of Request Handlers.
 */
struct HandlerStat
{
    using Imap = std::unordered_map<std::string, uint64_t>; // key: clientId. value: count.
    using CountMap = std::unordered_map<std::string, Imap>; // key: protocolName.

    struct Latency {
        double sum;
        uint64_t count;
    };
    using LatencyMap = std::unordered_map<std::string, Latency>; // key: protocolName.

    using GetCountMap = std::unordered_map<std::string, Imap>; // key: targetName of get command

    CountMap countMap;
    LatencyMap latencyMap;
    GetCountMap getCountMap;
};

class HandlerStatMgr
{
    mutable std::recursive_mutex mu_;
    using Autolock = std::unique_lock<std::recursive_mutex>;

    HandlerStat stat_;

    // key: unique id. value: timestamp.
    using TsMap = std::unordered_map<uint64_t, cybozu::Timespec>;
    TsMap tsMap_; // in order to calcurate protocol latency.

    uint64_t getId() const {
        static std::atomic<uint64_t> v_(0);
        return v_.fetch_add(1);
    }
public:
    class Transaction {
        HandlerStatMgr *self_;
        bool isEnd_;
        const uint64_t id_;
        std::string protocolName_;
        std::string clientId_;
    public:
        Transaction(HandlerStatMgr *self, const std::string& protocolName,
                    const std::string& clientId)
            : self_(self), isEnd_(false), id_(self->getId())
            , protocolName_(protocolName), clientId_(clientId) {
            assert(self);
            cybozu::Timespec ts = cybozu::getNowAsTimespec();
            Autolock lk(self_->mu_);
            self_->stat_.countMap[protocolName][clientId]++;
            self->tsMap_[id_] = ts;
        }
        void succeed() {
            cybozu::Timespec ts = cybozu::getNowAsTimespec();
            Autolock lk(self_->mu_);
            HandlerStat::Latency& latency = self_->stat_.latencyMap[protocolName_];
            latency.sum += (ts - self_->tsMap_[id_]).getAsDouble();
            latency.count++;
            self_->tsMap_.erase(id_);
            isEnd_ = true;
        }
        ~Transaction() noexcept {
            if (isEnd_) return;
            Autolock lk(self_->mu_);
            self_->tsMap_.erase(id_);
        }
    };

    Transaction start(const std::string& protocolName, const std::string& clientId) {
        return Transaction(this, protocolName, clientId);
    }
    void recordGetCommand(const std::string& targetName, const std::string& clientId) {
        Autolock lk(mu_);
        stat_.getCountMap[targetName][clientId]++;
    }
    HandlerStat getStatByMove() {
        Autolock lk(mu_);
        return std::move(stat_);
    }
};

StrVec prettyPrintHandlerStat(const HandlerStat& stat);


/**
 * Server handler type.
 */
using ServerHandler = void (*)(ServerParams &);
typedef std::map<std::string, ServerHandler> Str2ServerHandler;

ServerHandler findServerHandler(
    const Str2ServerHandler &handlers, const std::string &protocolName);

/**
 * Server dispatcher.
 */
class RequestWorker
{
    cybozu::Socket sock;
    std::string nodeId;
    ProcessStatus &ps;
    HandlerStatMgr &handlerStatMgr;
public:
    const protocol::Str2ServerHandler& handlers;
    RequestWorker(cybozu::Socket &&sock, const std::string &nodeId,
                  ProcessStatus &ps, const protocol::Str2ServerHandler& handlers,
                  HandlerStatMgr& handlerStatMgr)
        : sock(std::move(sock))
        , nodeId(nodeId)
        , ps(ps)
        , handlerStatMgr(handlerStatMgr)
        , handlers(handlers) {}
    void operator()() noexcept;
};

/**
 * If numToSend == 0, it will not check the vector size.
 */
void sendStrVec(
    cybozu::Socket &sock,
    const StrVec &v, size_t numToSend, const char *msg, const char *confirmMsg = nullptr);

/**
 * If numToRecv == 0, it will not check the vector size.
 */
StrVec recvStrVec(cybozu::Socket &sock, size_t numToRecv, const char *msg);


enum ValueType {
    SizeType,
    StringType,
    StringVecType,
};

struct GetCommandInfo
{
    ValueType valueType;
    void (*verify)(const StrVec &);
    std::string helpMsg;
};

using GetCommandInfoMap = std::map<std::string, GetCommandInfo>;

const GetCommandInfo &getGetCommandInfo(const std::string &name, const GetCommandInfoMap &infoM, const char *msg);
void recvValueAndPut(cybozu::Socket &sock, ValueType valType, const char *msg);


struct GetCommandParams
{
    const StrVec &params;
    packet::Packet &pkt;
    Logger &logger;
    bool &sendErr;
};

using GetCommandHandler = void (*)(GetCommandParams&);
using GetCommandHandlerMap = std::map<std::string, GetCommandHandler>;

void runGetCommandServer(ServerParams &p, const std::string &nodeId, const GetCommandHandlerMap &hMap,
                         HandlerStatMgr &handlerStatMgr);


template <typename T>
inline void sendValueAndFin(packet::Packet &pkt, bool &sendErr, const T &t)
{
    pkt.write(msgOk);
    sendErr = false;
    pkt.write(t);
    packet::Ack(pkt.sock()).sendFin();
}

template <typename T>
inline void sendValueAndFin(GetCommandParams &p, const T &t)
{
    sendValueAndFin(p.pkt, p.sendErr, t);
}

template <typename VolStateGetter>
inline void runGetStateServer(GetCommandParams &p, VolStateGetter getter)
{
    const std::string volId = parseVolIdParam(p.params, 1);
    const std::string state = getter(volId).sm.get();
    sendValueAndFin(p, state);
}

std::string runGetHostTypeClient(cybozu::Socket &sock, const std::string &nodeId);
void runExecServer(ServerParams &p, const std::string &nodeId, bool allowExec);

}} // namespace walb::protocol
