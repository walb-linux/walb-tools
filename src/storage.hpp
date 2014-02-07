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
        StorageVolInfo volInfo(p.baseDirStr, volId);
        packet.write("ok");
        packet.write(volInfo.getStatusAsStrVec());
    }
}

static inline void c2sInitVolServer(protocol::ServerParams &p)
{
    const StrVec v = protocol::recvStrVec(p.sock, 2, "c2sInitVolServer", false);
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
    const StrVec v = protocol::recvStrVec(p.sock, 2, "c2sFullSyncServer", false);
    const std::string& volId = v[0];
    const uint64_t bulkLb = cybozu::atoi(v[1]);
    const uint64_t curTime = ::time(0);

    StorageVolInfo volInfo(p.baseDirStr, volId);
    packet::Packet packet(p.sock);

    const std::string state = volInfo.getState();
    if (state != "SyncReady") {
        cybozu::Exception e("c2sFullSyncServer:bad state");
        e << state;
        packet.write(e.what());
        throw e;
    }

    volInfo.resetWlog(0);

    const uint64_t sizeLb = getSizeLb(volInfo.getWdevPath());
    const cybozu::Uuid uuid = volInfo.getUuid();

    // ToDo : start master((3) at full-sync as client in storage-daemon.txt)
}

static inline void s2aDirtyFullSyncClient(protocol::ClientParams &p)
{
}

} // walb
