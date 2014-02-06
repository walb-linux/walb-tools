#pragma once
#include "storage_vol_info.hpp"

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

static inline void storageInitVol(cybozu::Socket &sock,
    ProtocolLogger& logger,
    const std::string &baseDirStr,
    const std::atomic<bool> &/*forceQuit*/,
    std::atomic<walb::server::ProcessStatus> &/*procStat*/)
{
    packet::Packet packet(sock);
    std::string volId, wdevPathName;
    packet.read(volId);
    packet.read(wdevPathName);

    StorageVolInfo volInfo(baseDirStr, volId, wdevPathName);
    volInfo.init();

    packet::Ack ack(sock);
    ack.send();

    logger.info("storageInitVol: initialize volId %s wdev %s", volId.c_str(), wdevPathName.c_str());
}

} // walb


