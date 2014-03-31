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
 * Protocol name list.
 */
const char *const statusPN = "status";
const char *const initVolPN = "init-vol";
const char *const clearVolPN = "clear-vol";
const char *const listVolPN = "list-vol";
const char *const resetVolPN = "reset-vol";
const char *const startPN = "start";
const char *const stopPN = "stop";
const char *const fullBkpPN = "full-bkp";
const char *const hashBkpPN = "hash-bkp";
const char *const snapshotPN = "snapshot";
const char *const archiveInfoPN = "archive-info";
const char *const wlogTransferPN = "wlog-transfer";
const char *const dirtyFullSyncPN = "dirty-full-sync";
const char *const dirtyHashSyncPN = "dirty-hash-sync";
const char *const restorePN = "restore";
const char *const dropPN = "drop";
const char *const wdiffTransferPN = "wdiff-transfer";
const char *const applyPN = "apply";
const char *const mergePN = "merge";
const char *const resizePN = "resize";
const char *const hostTypePN = "host-type";
const char *const shutdownPN = "shutdown";
const char *const dbgReloadMetadataPN = "dbg-reload-metadata";


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

namespace protocol {

/**
 * RETURN:
 *   Server ID.
 */
inline std::string run1stNegotiateAsClient(
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
        cybozu::Exception e("received NG");
        e << "err" << err << "msg" << msg;
        logger.throwError(e);
    }
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
 * @clientId will be set.
 * @protocol will be set.
 *
 * This function will do only the common negotiation.
 *
 * RETURN:
 *   true if the protocol has finished or failed that is there is nothing to do.
 *   otherwise false.
 */
inline bool run1stNegotiateAsServer(
    cybozu::Socket &sock, const std::string &serverId,
    std::string &protocolName,
    std::string &clientId)
{
    packet::Packet packet(sock);

    //LOGs.debug() << "run1stNegotiateAsServer start";
    packet.read(clientId);
    //LOGs.debug() << "clientId" << clientId;
    packet.read(protocolName);
    //LOGs.debug() << "protocolName" << protocolName;
    packet::Version ver(sock);
    bool isVersionSame = ver.recv();
    //LOGs.debug() << "isVersionSame" << isVersionSame;
    packet.write(serverId);

    ProtocolLogger logger(serverId, clientId);
    packet::Answer ans(sock);

    if (!isVersionSame) {
        std::string msg = cybozu::util::formatString(
            "Version differ: client %" PRIu32 " server %" PRIu32 ""
            , ver.get(), packet::VERSION);
        logger.warn(msg);
        ans.ng(1, msg);
        return true;
    }
    ans.ok();
    logger.debug() << "initial negotiation succeeded" << protocolName;
    return false;

    /* Here command existance has not been checked yet. */
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

/**
 * Server handler type.
 */
using ServerHandler = void (*)(ServerParams &);

inline void shutdownClient(ClientParams &p)
{
    bool isForce = false;
    if (!p.params.empty()) {
        isForce = static_cast<int>(cybozu::atoi(p.params[0])) != 0;
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

inline void clientDispatch(
    const std::string& protocolName, cybozu::Socket& sock, ProtocolLogger& logger,
    const std::vector<std::string> &params,
    const std::map<std::string, ClientHandler> &handlers)
{
    ClientParams clientParams(sock, logger, params);
    if (protocolName == shutdownPN) {
        shutdownClient(clientParams);
        return;
    }
    auto it = handlers.find(protocolName);
    if (it != handlers.cend()) {
        ClientHandler h = it->second;
        h(clientParams);
    } else {
        throw cybozu::Exception("clientDispatch:bad protocoName") << protocolName;
    }
}

/**
 * Server dispatcher.
 */
inline void serverDispatch(
    cybozu::Socket &sock, const std::string &nodeId,
    std::atomic<walb::server::ProcessStatus> &procStat,
    const std::map<std::string, ServerHandler> &handlers) noexcept
{
    std::string clientId, protocolName;
    try {
        if (run1stNegotiateAsServer(sock, nodeId, protocolName, clientId)) {
            /* The protocol has finished or failed. */
            return;
        }
    } catch (std::exception &e) {
        LOGe("run1stNegotiateAsServer failed: %s", e.what());
    } catch (...) {
        LOGe("run1stNegotiateAsServer failed: other error");
    }
    ProtocolLogger logger(nodeId, clientId);
    try {
        ServerParams serverParams(sock, clientId, procStat);
        if (protocolName == shutdownPN) {
            shutdownServer(serverParams);
            return;
        }
        auto it = handlers.find(protocolName);
        if (it != handlers.cend()) {
            ServerHandler h = it->second;
            h(serverParams);
        } else {
            throw cybozu::Exception("serverDispatch:bad protocolName") << protocolName;
        }
    } catch (std::exception &e) {
        logger.error() << e.what();
    } catch (...) {
        logger.error("serverDispatch: other error.");
    }
}

/**
 * If numToSend == 0, it will not check the vector size.
 */
inline void sendStrVec(
    cybozu::Socket &sock,
    const std::vector<std::string> &v, size_t numToSend, const char *msg, bool doAck = true)
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

	if (doAck) {
        packet::Packet pkt(sock);
        std::string res;
        pkt.read(res);
        if (res != msgOk) {
            throw cybozu::Exception(msg) << res;
        }
	}
}

/**
 * If numToRecv == 0, it will not check the vector size.
 */
inline std::vector<std::string> recvStrVec(
    cybozu::Socket &sock, size_t numToRecv, const char *msg, bool doAck = true)
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
    if (doAck) {
        packet::Packet(sock).write("ok");
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

}} // namespace walb::protocol
