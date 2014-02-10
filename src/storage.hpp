#pragma once
#include "protocol.hpp"
#include "storage_vol_info.hpp"
#include <snappy.h>

namespace walb {

struct StorageSingleton
{
    static StorageSingleton& getInstance() {
        static StorageSingleton instance;
        return instance;
    }
    cybozu::SocketAddr archive;
    std::vector<cybozu::SocketAddr> proxyV;
    std::string nodeId;
    std::string baseDirStr;
};

static inline void c2sStatusServer(protocol::ServerParams &p)
{
    const StorageSingleton &sing = StorageSingleton::getInstance();

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
        StorageVolInfo volInfo(sing.baseDirStr, volId);
        packet.write("ok");
        packet.write(volInfo.getStatusAsStrVec());
    }
}

static inline void c2sInitVolServer(protocol::ServerParams &p)
{
    const StorageSingleton &sing = StorageSingleton::getInstance();

    const StrVec v = protocol::recvStrVec(p.sock, 2, "c2sInitVolServer", false);
    const std::string &volId = v[0];
    const std::string &wdevPathName = v[1];

    StorageVolInfo volInfo(sing.baseDirStr, volId, wdevPathName);
    volInfo.init();

    packet::Ack ack(p.sock);
    ack.send();

    ProtocolLogger logger(sing.nodeId, p.clientId);
    logger.info("c2sInitVolServer: initialize volId %s wdev %s", volId.c_str(), wdevPathName.c_str());
}

static inline void c2sFullSyncServer(protocol::ServerParams &p)
{
    const StorageSingleton &sing = StorageSingleton::getInstance();

    const StrVec v = protocol::recvStrVec(p.sock, 2, "c2sFullSyncServer", false);
    const std::string& volId = v[0];
    const uint64_t bulkLb = cybozu::atoi(v[1]);
    const uint64_t curTime = ::time(0);

    StorageVolInfo volInfo(sing.baseDirStr, volId);
    packet::Packet cPack(p.sock);

    const std::string state = volInfo.getState();
    if (state != "SyncReady") {
        cybozu::Exception e("c2sFullSyncServer:bad state");
        e << state;
        cPack.write(e.what());
        throw e;
    }

    volInfo.resetWlog(0);

    const uint64_t sizeLb = getSizeLb(volInfo.getWdevPath());
    const cybozu::Uuid uuid = volInfo.getUuid();

    // ToDo : start master((3) at full-sync as client in storage-daemon.txt)

    const cybozu::SocketAddr& archive = sing.archive;
    const std::string& nodeId = sing.nodeId;
    std::string archiveId;
    {
        cybozu::Socket aSock;
        aSock.connect(archive);
        archiveId = walb::protocol::run1stNegotiateAsClient(aSock, sing.nodeId, "dirty-full-sync");
        walb::packet::Packet aPack(aSock);
        aPack.write("storageD");
        aPack.write(volId);
        aPack.write(uuid);
        aPack.write(sizeLb);
        aPack.write(curTime);
        aPack.write(bulkLb);
        {
            std::string res;
            aPack.read(res);
            if (res == "ok") {
                cPack.write("ok");
                p.sock.close();
            } else {
                cybozu::Exception e("c2sFullSyncServer:bad response");
                e << archiveId << res;
                cPack.write(e.what());
                throw e;
            }
        }
        // (7) in storage-daemon.txt
        {
            std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
            cybozu::util::BlockDevice bd(volInfo.getWdevPath(), O_RDONLY);
            std::string encBuf;

            uint64_t remainingLb = sizeLb;
            while (0 < remainingLb) {
                uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
                size_t size = lb * LOGICAL_BLOCK_SIZE;
                bd.read(&buf[0], size);
                const size_t encSize = snappy::Compress(&buf[0], size, &encBuf);
                aPack.write(encSize);
                aPack.write(&encBuf[0], encSize);
                remainingLb -= lb;
            }
        }
        // (8), (9) in storage-daemon.txt
        {
            // TODO take a snapshot
            uint64_t gidB = 0, gidE = 1;
            aPack.write(gidB);
            aPack.write(gidE);
        }
        walb::packet::Ack(aSock).recv();
    }

    // TODO (10), ....

    // (11)
    volInfo.setState("Master");
    LOGi("c2sFullSyncServer done, ctrl:%s storage:%s archive:%s", p.clientId.c_str(), nodeId.c_str(), archiveId.c_str());
}

} // walb
