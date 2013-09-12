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
        for (const std::string &s0 : params) {
            std::string s1;
            packet.write(s0);
            packet.read(s1);
            logger.info("s0: %s s1: %s\n", s0.c_str(), s1.c_str());
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
        logger.info("size: %" PRIu32 "\n", size);
        for (uint32_t i = 0; i < size; i++) {
            std::string s0;
            packet.read(s0);
            packet.write(s0);
        }
    }
};

#if 0
/**
 * Full-sync.
 */
class FullSyncProtocol : public Protocol
{
public:
    /**
     * @params using cybozu::loadFromStr() to convert.
     *   [0] :: string: full path of lv.
     *   [1] :: string: lv identifier.
     *   [2] :: uint64_t: lv size [logical block].
     */
    void runAsClient(cybozu::Socket &sock, Logger &,
                     const std::vector<std::string> &params) override {
        std::string path;
        std::string name;
        uint64_t sizeLb;
        std::tie(path, name, sizeLb) = loadClientParams(params);
        packet::Packet packet(sock);
        packet.write(name_);
        packet.write(sizeLb_);

        /* now editing */
    }
    /**
     *
     * @params using cybozu::loadFromStr() to convert;
     *   [0] :: string:
     */
    void runAsServer(cybozu::Socket &sock, Logger &,
                     const std::vector<std::string> &params) override {

        loadServerParams(params);

        packet::Packet packet(sock);
        packet.read(name_);
        packet.read(sizeLb_);
        std::vector<std::string> &params;

        /* now editing */
    }
private:
    std::tuple<std::string, std::string, uint64_t> loadClientParams(
        const std::vector<std::string> &params) {
        if (params.size() != 3) {
            logger_.log
        }
        /* now editing */



    }
    void loadServerParams(const std::vector<std::string> &params) {
        /* now editing */
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
    protocol->runAsClient(sock, logger, params);
}

/**
 * Run a protocol as a server.
 *
 * TODO: arguments for server data.
 */
static inline void runProtocolAsServer(
    cybozu::Socket &sock, const std::string &serverId)
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

    try {
        protocol->runAsServer(sock, logger, {});
    } catch (std::exception &e) {
        logger.error("[%s] runProtocolAsServer failed: %s.", e.what());
    }
}

} //namespace walb
