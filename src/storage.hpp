#pragma once
#include "protocol.hpp"
#include "storage_vol_info.hpp"

namespace walb {

static inline void c2sStatusServer(protocol::ServerParams &p)
{
    packet::Packet packet(p.sock);
    std::vector<std::string> params;
    packet.read(params);

    if (params.empty()) {
        // for all volumes
        packet.write("not implemented yet");
        // TODO
    } else {
        // for a volume
        const std::string &volId = params[0];
        try {
            StorageVolInfo volInfo(p.baseDirStr, volId);
            packet.write("ok");
            packet.write(volInfo.getStatusAsStrVec());
        } catch (std::exception &e) {
            packet.write(e.what());
            p.logger.error("c2sStatusServer:failed %s", e.what());
        }
    }
}

static inline void c2sInitVolServer(protocol::ServerParams &p)
{
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 2, "c2sInitVolServer", false);
    const std::string &volId = v[0];
    const std::string &wdevPathName = v[1];

    StorageVolInfo volInfo(p.baseDirStr, volId, wdevPathName);
    volInfo.init();

    packet::Ack ack(p.sock);
    ack.send();

    p.logger.info("c2sInitVolServer: initialize volId %s wdev %s", volId.c_str(), wdevPathName.c_str());
}

static inline void c2sFullSyncServer(protocol::ServerParams &p)
{
}

static inline void s2aDirtyFullSyncClient(protocol::ClientParams &p)
{
}

} // walb
