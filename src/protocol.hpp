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
#include "packet.hpp"
#include "util.hpp"
#include "sys_logger.hpp"

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

    void write(cybozu::LogPriority pri, const std::string &msg) noexcept {
        cybozu::PutLog(pri, "[%s][%s] %s", selfId_.c_str(), remoteId_.c_str(), msg.c_str());
    }
    void write(cybozu::LogPriority pri, const char *format, va_list args) noexcept {
        try {
            std::string msg;
            cybozu::vformat(msg, format, args);
            write(pri, msg);
        } catch (...) {
            write(pri, "Logger::write() error.");
        }
    }
    void write(cybozu::LogPriority pri, const char *format, ...) noexcept {
        try {
            va_list args;
            va_start(args, format);
            write(pri, format, args);
            va_end(args);
        } catch (...) {
            write(pri, "Logger::write() error.");
        }
    }

    void debug(UNUSED const std::string &msg) noexcept {
#ifdef DEBUG
        write(cybozu::LogDebug, msg);
#endif
    }
    void info(const std::string &msg) noexcept { write(cybozu::LogInfo, msg); }
    void warn(const std::string &msg) noexcept { write(cybozu::LogWarning, msg); }
    void error(const std::string &msg) noexcept { write(cybozu::LogError, msg); }

    void debug(UNUSED const char *format, ...) noexcept {
#ifdef DEBUG
        va_list args;
        va_start(args, format);
        write(cybozu::LogDebug, format, args);
        va_end(args);
#endif
    }
    void info(const char *format, ...) noexcept {
        va_list args;
        va_start(args, format);
        write(cybozu::LogInfo, format, args);
        va_end(args);
    }
    void warn(const char *format, ...) noexcept {
        va_list args;
        va_start(args, format);
        write(cybozu::LogWarning, format, args);
        va_end(args);
    }
    void error(const char *format, ...) noexcept {
        va_list args;
        va_start(args, format);
        write(cybozu::LogError, format, args);
        va_end(args);
    }
};

/**
 * Protocol interface.
 */
class Protocol
{
protected:
    std::string remoteId_;
    std::string name_; /* protocol name */

public:
    virtual bool runAsClient(cybozu::Socket &, Logger &) = 0;
    virtual bool runAsServer(cybozu::Socket &, Logger &) = 0;
};

/**
 * Simple echo.
 */
class EchoProtocol : public Protocol
{
public:
    virtual bool runAsClient(cybozu::Socket &sock, Logger &logger) override {
        packet::Packet packet(sock);
        std::string s0, s1;
        cybozu::Time(true).toString(s0);
        packet.write(s0);
        packet.read(s1);
        if (s0 != s1) {
            throw std::runtime_error("echo-backed string differs from the original.");
        }
        return true;
    }
    virtual bool runAsServer(cybozu::Socket &sock, Logger &logger) override {
        packet::Packet packet(sock);
        std::string s0;
        packet.read(s0);
        packet.write(s0);
        return true;
    }
};

/**
 * Protocol factory.
 */
class ProtocolFactory
{
private:
    std::map<std::string, std::unique_ptr<Protocol> > map_;

public:
    static ProtocolFactory &getInstance() {
        static ProtocolFactory factory;
        return factory;
    }

    Protocol *find(const std::string &name) {
        auto it = map_.find(name);
        if (it == map_.end()) return nullptr;
        return it->second.get();
    }
private:
#define DECLARE_PROTOCOL(name, cls) map_.insert(std::make_pair(#name, std::unique_ptr<cls>(new cls())))
    ProtocolFactory() : map_() {
        DECLARE_PROTOCOL(echo, EchoProtocol);

        /* now editing */
    }
#undef DECLARE_PROTOCOL
};

/**
 * Run a protocol as a client.
 */
static inline bool runProtocolAsClient(
    cybozu::Socket &sock, const std::string &clientId, const std::string &protocolName)
{
    packet::Packet packet(sock);
    packet.write(clientId);
    packet.write(protocolName);
    packet::Version ver(sock);
    ver.send();
    std::string serverId;
    packet.read(serverId);
    ::printf("serverId: %s\n", serverId.c_str()); /* debug */

    Logger logger(clientId, serverId);

    packet::Answer ans(sock);
    int err;
    std::string msg;
    if (!ans.recv(&err, &msg)) {
        logger.warn("received NG: err %d msg %s", err, msg.c_str());
        return false;
    }

    Protocol *protocol = ProtocolFactory::getInstance().find(protocolName);
    if (!protocol) {
        throw std::runtime_error("receive OK but protocol not found.");
    }

    return protocol->runAsClient(sock, logger);
}

/**
 * Run a protocol as a server.
 */
static inline bool runProtocolAsServer(cybozu::Socket &sock, const std::string &serverId)
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
        return false;
    }
    if (!isVersionSame) {
        std::string msg = cybozu::util::formatString(
            "Version differ: server %" PRIu32 "", packet::VERSION);
        logger.info(msg);
        ans.ng(1, msg);
        return false;
    }
    ans.ok();

    logger.info("hoge1"); /* debug */

    return protocol->runAsServer(sock, logger);
}

} //namespace walb
