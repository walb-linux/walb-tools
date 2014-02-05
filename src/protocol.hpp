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
#include <functional>
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

/* Protocols. */
#include "init_vol.hpp"
#include "echo.hpp"
#include "wlog_send.hpp"
#include "dirty_full_sync.hpp"
#include "dirty_hash_sync.hpp"
#include "storage_status.hpp"
#include "proxy_status.hpp"
#include "archive_status.hpp"

namespace walb {
namespace protocol {

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

/**
 * Client handler type.
 */
using ClientHandler = void (*)(
    cybozu::Socket&, ProtocolLogger&,
    const std::atomic<bool>&,
    const std::vector<std::string>&);

static inline void clientDispatch(
    const std::string& protocolName, cybozu::Socket& sock, ProtocolLogger& logger,
    const std::atomic<bool> &forceQuit, const std::vector<std::string> &params,
    const std::map<std::string, ClientHandler> &handlers)
{
    auto it = handlers.find(protocolName);
    if (it != handlers.cend()) {
        ClientHandler h = it->second;
        h(sock, logger, forceQuit, params);
    } else {
        throw cybozu::Exception("dispatch:receive OK but protocol not found.") << protocolName;
    }
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
    std::string &protocolName,
    std::string &clientId,
    std::atomic<walb::server::ControlFlag> &ctrlFlag)
{
    packet::Packet packet(sock);

    LOGi_("run1stNegotiateAsServer start\n");
    packet.read(clientId);
    LOGi_("clientId: %s\n", clientId.c_str());
    packet.read(protocolName);
    LOGi_("protocolName: %s\n", protocolName.c_str());
    packet::Version ver(sock);
    bool isVersionSame = ver.recv();
    LOGi_("isVersionSame: %d\n", isVersionSame);
    packet.write(serverId);

    ProtocolLogger logger(serverId, clientId);
    packet::Answer ans(sock);

    /* Server shutdown commands. */
    if (protocolName == "graceful-shutdown") {
        ctrlFlag = walb::server::ControlFlag::GRACEFUL_SHUTDOWN;
        logger.info("graceful shutdown.");
        ans.ok();
        return true;
    } else if (protocolName == "force-shutdown") {
        ctrlFlag = walb::server::ControlFlag::FORCE_SHUTDOWN;
        logger.info("force shutdown.");
        ans.ok();
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

    /* Here command existance has not been checked yet. */
}

/**
 * Server handler type.
 */
using ServerHandler = void (*)(
    cybozu::Socket&, ProtocolLogger&,
    const std::string&,
    const std::atomic<bool>&,
    std::atomic<walb::server::ControlFlag>&);

/**
 * Server dispatcher.
 */
static inline void serverDispatch(
    cybozu::Socket &sock, const std::string &serverId, const std::string &baseDirStr,
    const std::atomic<bool> &forceQuit,
    std::atomic<walb::server::ControlFlag> &ctrlFlag,
    const std::map<std::string, ServerHandler> &handlers) noexcept
{
    std::string clientId, protocolName;
    if (run1stNegotiateAsServer(sock, serverId, protocolName, clientId, ctrlFlag)) {
        /* The protocol has finished or failed. */
        return;
    }
    ProtocolLogger logger(serverId, clientId);
    try {
        auto it = handlers.find(protocolName);
        if (it != handlers.cend()) {
            ServerHandler h = it->second;
            h(sock, logger, baseDirStr, forceQuit, ctrlFlag);
        } else {
            throw cybozu::Exception("bad protocolName") << protocolName;
        }
    } catch (std::exception &e) {
        logger.error("runlAsServer failed: %s", e.what());
    } catch (...) {
        logger.error("runAsServer failed: unknown error.");
    }
}

}} //namespace walb::protocol
