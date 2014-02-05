#pragma once

namespace walb {

static inline void clientEcho(cybozu::Socket& sock, ProtocolLogger& logger,
    const std::atomic<bool> &/*forceQuit*/, const std::vector<std::string> &params)
{
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

static inline void serverEcho(cybozu::Socket &sock,
    ProtocolLogger& logger,
    const std::string &/*baseDirStr*/,
    const std::atomic<bool> &/*forceQuit*/,
    std::atomic<walb::server::ControlFlag> &/*ctrlFlag*/)
{
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

} // walb
