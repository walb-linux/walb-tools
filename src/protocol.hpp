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
#include "cybozu/socket.hpp"
#include "packet.hpp"
#include "util.hpp"
#include "walb_logger.hpp"
#include "walb_util.hpp"
#include "server_util.hpp"

namespace walb {

/**
 * Message string.
 */
const char *const msgOk = "ok"; // used for synchronous protocol.
const char *const msgAccept = "accept"; // used for asynchronous protocol.

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
const char *const getStateCN = "get-state";
const char *const statusCN = "status";
const char *const listDiffCN = "list-diff";
const char *const initVolCN = "init-vol";
const char *const clearVolCN = "clear-vol";
const char *const listVolCN = "list-vol";
const char *const resetVolCN = "reset-vol";
const char *const startCN = "start";
const char *const stopCN = "stop";
const char *const fullBkpCN = "full-bkp";
const char *const hashBkpCN = "hash-bkp";
const char *const snapshotCN = "snapshot";
const char *const archiveInfoCN = "archive-info";
const char *const restoreCN = "restore";
const char *const delRestoredCN = "del-restored";
const char *const listRestoredCN = "list-restored";
const char *const listRestorableCN = "list-restorable";
const char *const replicateCN = "replicate";
const char *const applyCN = "apply";
const char *const mergeCN = "merge";
const char *const resizeCN = "resize";
const char *const hostTypeCN = "host-type";
const char *const shutdownCN = "shutdown";
const char *const isOverflowCN = "is-overflow";
const char *const dbgReloadMetadataCN = "dbg-reload-metadata";

/**
 * Internal protocol name.
 */
const char *const dirtyFullSyncPN = "dirty-full-sync";
const char *const dirtyHashSyncPN = "dirty-hash-sync";
const char *const wlogTransferPN = "wlog-transfer";
const char *const wdiffTransferPN = "wdiff-transfer";
const char *const replSyncPN = "repl-sync";


inline cybozu::SocketAddr parseSocketAddr(const std::string &addrPort)
{
    const StrVec v = cybozu::Split(addrPort, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception("parseSocketAddr:parse error") << addrPort;
    }
    return cybozu::SocketAddr(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}

inline std::vector<cybozu::SocketAddr> parseMultiSocketAddr(const std::string &multiAddrPort)
{
    std::vector<cybozu::SocketAddr> ret;
    const StrVec v = cybozu::Split(multiAddrPort, ',');
    for (const std::string &addrPort : v) {
        ret.emplace_back(parseSocketAddr(addrPort));
    }
    return ret;
}

enum class StopType
{
    Graceful, Empty, Force,
};

struct StopOpt
{
    StopType type;

    StopOpt() : type(StopType::Graceful) {
    }
    std::string str() const {
        switch (type) {
        case StopType::Graceful: return "graceful";
        case StopType::Empty: return "empty";
        case StopType::Force: return "force";
        }
        throw cybozu::Exception(__func__) << "bug" << int(type);
    }
    void parse(const std::string &s) {
        type = StopType::Graceful;
        if (s.empty()) return;
        if (s[0] == 'f') {
            type = StopType::Force;
        } else if (s[0] == 'e') {
            type = StopType::Empty;
        }
    }
    bool isForce() const { return type == StopType::Force; }
    bool isGraceful() const { return type == StopType::Graceful; }
    bool isEmpty() const { return type == StopType::Empty; }
    friend inline std::ostream &operator<<(std::ostream &os, const StopOpt &opt) {
        os << opt.str();
        return os;
    }
};

inline std::pair<std::string, StopOpt> parseStopParams(const StrVec &v, const char *msg)
{
    if (v.empty()) throw cybozu::Exception(msg) << "empty";
    if (v[0].empty()) throw cybozu::Exception(msg) << "empty volId";
    StopOpt opt;
    if (v.size() >= 2) opt.parse(v[1]);
    return {v[0], opt};
}

namespace protocol {

/**
 * RETURN:
 *   Server ID.
 */
inline std::string run1stNegotiateAsClient(
    cybozu::Socket &sock,
    const std::string &clientId, const std::string &protocolName)
{
    packet::Packet pkt(sock);
    pkt.write(clientId);
    pkt.write(protocolName);
    packet::Version ver(sock);
    ver.send();
    std::string serverId;
    pkt.read(serverId);

    ProtocolLogger logger(clientId, serverId);
    std::string msg;
    pkt.read(msg);
    if (msg != msgOk) throw cybozu::Exception(__func__) << msg;
    return serverId;
}

/**
 * Parameters for commands as a client.
 */
struct ClientParams
{
    cybozu::Socket &sock;
    ProtocolLogger &logger;
    const std::vector<std::string> &params;

    ClientParams(
        cybozu::Socket &sock0,
        ProtocolLogger &logger0,
        const std::vector<std::string> &params0)
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
inline void run1stNegotiateAsServer(
    cybozu::Socket &sock, const std::string &serverId,
    std::string &protocolName, std::string &clientId)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(sock);

    pkt.read(clientId);
    pkt.read(protocolName);
    packet::Version ver(sock);
    const bool isVersionSame = ver.recv();
    pkt.write(serverId);
    LOGs.debug() << FUNC << clientId << protocolName << ver.get();

    if (!isVersionSame) {
        throw cybozu::Exception(FUNC) << "version differ c/s" << ver.get() << packet::VERSION;
    }
    ProtocolLogger logger(serverId, clientId);
    logger.debug() << "initial negotiation succeeded" << protocolName;
}

/**
 * Parameters for commands as a server.
 */
struct ServerParams
{
    cybozu::Socket &sock;
	const std::string& clientId;
    std::atomic<walb::server::ProcessStatus> &procStat;

    ServerParams(
        cybozu::Socket &sock,
        const std::string &clientId,
        std::atomic<walb::server::ProcessStatus> &procStat)
        : sock(sock)
		, clientId(clientId)
        , procStat(procStat) {
    }
};

inline void shutdownClient(ClientParams &p)
{
    bool isForce = false;
    if (!p.params.empty()) {
        const std::string &s = p.params[0];
        isForce = !s.empty() && s[0] == 'f';
    }
    packet::Packet pkt(p.sock);
    pkt.write(isForce);
    std::string res;
    pkt.read(res);
    if (res != msgAccept) {
        throw cybozu::Exception(__func__) << res;
    }
}

inline void shutdownServer(ServerParams &p)
{
    bool isForce;
    packet::Packet pkt(p.sock);
    pkt.read(isForce);
    p.procStat = (isForce
                  ? walb::server::ProcessStatus::FORCE_SHUTDOWN
                  : walb::server::ProcessStatus::GRACEFUL_SHUTDOWN);
    LOGs.info() << "shutdown" << (isForce ? "force" : "graceful") << p.clientId;
    pkt.write(msgAccept);
}

/**
 * Server handler type.
 */
using ServerHandler = void (*)(ServerParams &);

inline ServerHandler findServerHandler(
    const std::map<std::string, ServerHandler> &handlers, const std::string protocolName)
{
    if (protocolName == shutdownCN) {
        return shutdownServer;
    }
    std::map<std::string, ServerHandler>::const_iterator it = handlers.find(protocolName);
    if (it == handlers.cend()) {
        throw cybozu::Exception(__func__) << "bad protocol" << protocolName;
    }
    return it->second;
}

inline void clientDispatch(
    const std::string& protocolName, cybozu::Socket& sock, ProtocolLogger& logger,
    const std::vector<std::string> &params,
    const std::map<std::string, ClientHandler> &handlers)
{
    ClientParams clientParams(sock, logger, params);
    if (protocolName == shutdownCN) {
        shutdownClient(clientParams);
        return;
    }
    std::map<std::string, ClientHandler>::const_iterator it = handlers.find(protocolName);
    if (it == handlers.cend()) {
        throw cybozu::Exception("clientDispatch:bad protocoName") << protocolName;
    }
    ClientHandler h = it->second;
    h(clientParams);
}

/**
 * Server dispatcher.
 */
inline void serverDispatch(
    cybozu::Socket &sock, const std::string &nodeId,
    std::atomic<walb::server::ProcessStatus> &procStat,
    const std::map<std::string, ServerHandler> &handlers) noexcept try
{
    std::string clientId, protocolName;
    packet::Packet pkt(sock);
    bool sendErr = true;
    try {
        run1stNegotiateAsServer(sock, nodeId, protocolName, clientId);
        ServerHandler handler = findServerHandler(handlers, protocolName);
        ServerParams serverParams(sock, clientId, procStat);
        pkt.write(msgOk);
        sendErr = false;
        handler(serverParams);
    } catch (std::exception &e) {
        LOGs.error() << e.what();
        if (sendErr) pkt.write(e.what());
    } catch (...) {
        cybozu::Exception e(__func__);
        e << "other error";
        LOGs.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
} catch (std::exception &e) {
    LOGs.error() << e.what();
} catch (...) {
    LOGs.error() << "other error";
}

/**
 * If numToSend == 0, it will not check the vector size.
 */
inline void sendStrVec(
    cybozu::Socket &sock,
    const std::vector<std::string> &v, size_t numToSend, const char *msg, const char *confirmMsg = nullptr)
{
    if (numToSend != 0 && v.size() != numToSend) {
        throw cybozu::Exception(msg) << "bad size" << numToSend << v.size();
    }
    packet::Packet packet(sock);
    for (size_t i = 0; i < v.size(); i++) {
        if (v[i].empty()) {
            throw cybozu::Exception(msg) << "empty string" << i;
        }
    }
    packet.write(v);

	if (confirmMsg) {
        packet::Packet pkt(sock);
        std::string res;
        pkt.read(res);
        if (res != confirmMsg) {
            throw cybozu::Exception(msg) << res;
        }
	}
}

/**
 * If numToRecv == 0, it will not check the vector size.
 */
inline std::vector<std::string> recvStrVec(
    cybozu::Socket &sock, size_t numToRecv, const char *msg)
{
    packet::Packet packet(sock);
    std::vector<std::string> v;
    packet.read(v);
    if (numToRecv != 0 && v.size() != numToRecv) {
        throw cybozu::Exception(msg) << "bad size" << numToRecv << v.size();
    }
    for (size_t i = 0; i < v.size(); i++) {
        if (v[i].empty()) {
            throw cybozu::Exception(msg) << "empty string" << i;
        }
    }
    return v;
}

inline std::string runHostTypeClient(cybozu::Socket &sock)
{
    packet::Packet pkt(sock);
    std::string type;
    pkt.read(type);
    packet::Ack(sock).recv();
    return type;
}

inline void runHostTypeServer(ServerParams &p, const std::string &hostType)
{
    packet::Packet(p.sock).write(hostType);
    packet::Ack(p.sock).send();
}

template <class VolStateGetter>
inline void c2xGetStateServer(protocol::ServerParams &p, VolStateGetter getter, const std::string &nodeId, const char *msg)
{
    ProtocolLogger logger(nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 1, msg);
        const std::string &volId = v[0];
        const std::string state = getter(volId).sm.get();
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(StrVec{state});
        packet::Ack(p.sock).send();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

}} // namespace walb::protocol

