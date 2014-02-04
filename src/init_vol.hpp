#pragma once

namespace walb {

static inline void clientInitVol(cybozu::Socket& sock, ProtocolLogger& /*logger*/, 
    const std::atomic<bool> &/*forceQuit*/, const std::vector<std::string> &params)
{
    if (params.size() != 2 || params[0].empty() || params[1].empty()) {
        throw cybozu::Exception("clientInitVol:bad params size");
    }
    packet::Packet packet(sock);
    packet.write(params[0]);
    packet.write(params[1]);

    packet::Ack ack(sock);
    ack.recv();
}

} // walb


