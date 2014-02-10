#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"
#include <snappy.h>
#include "walb/block_size.h"

namespace walb {

struct ArchiveSingleton
{
    static ArchiveSingleton& getInstance() {
        static ArchiveSingleton instance;
        return instance;
    }

    std::string nodeId;
    std::string baseDirStr;
    std::string volumeGroup;
};

inline ArchiveSingleton& getGlobal()
{
    static ArchiveSingleton i;
    return i;
}

const ArchiveSingleton& s = getGlobal();

static inline void c2aStatusServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

static inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, "c2aInitVolServer", false);
    const std::string &volId = v[0];

    ArchiveVolInfo volInfo(s.baseDirStr, volId, s.volumeGroup);
    volInfo.init();

    packet::Ack(p.sock).send();
}

/**
 * Execute dirty full sync protocol as server.
 * Client is storage server or another archive server.
 */
static inline void x2aDirtyFullSyncServer(protocol::ServerParams &p)
{
    ProtocolLogger logger(s.nodeId, p.clientId);

    walb::packet::Packet sPack(p.sock);
    std::string hostType, volId;
    cybozu::Uuid uuid;
    uint64_t sizeLb, curTime, bulkLb;
    sPack.read(hostType);
    if (hostType != "storageD" && hostType != "archiveD") {
        throw cybozu::Exception("x2aDirtyFullSyncServer:invalid hostType") << hostType;
    }
    sPack.read(volId);
    sPack.read(uuid);
    sPack.read(sizeLb);
    sPack.read(curTime);
    sPack.read(bulkLb);
    if (bulkLb == 0) {
        throw cybozu::Exception("x2aDirtyFullSyncServer:bulkLb is zero");
    }

    ArchiveVolInfo volInfo(s.baseDirStr, volId, s.volumeGroup);
    const std::string st = volInfo.getState();
    if (st != "SyncReady") {
        throw cybozu::Exception("x2aDirtyFullSyncServer:state is not SyncReady") << st;
    }
    volInfo.createLv(sizeLb);
    sPack.write("ok");

    // recv and write.
    {
        //TODO: sd.reset(gid_);
        std::string lvPath = volInfo.getLv().path().str();
        cybozu::util::BlockDevice bd(lvPath, O_RDWR);
        std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
        std::vector<char> encBuf;

        uint64_t c = 0;
        uint64_t remainingLb = sizeLb;
        while (0 < remainingLb) {
            const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
            const size_t size = lb * LOGICAL_BLOCK_SIZE;
            size_t encSize;
            sPack.read(encSize);
            if (encSize == 0) {
                throw cybozu::Exception("x2aDirtyFullSyncServer:encSize is zero");
            }
            encBuf.resize(encSize);
            sPack.read(&encBuf[0], encSize);
            size_t decSize;
            if (!snappy::GetUncompressedLength(&encBuf[0], encSize, &decSize)) {
                throw cybozu::Exception("x2aDirtyFullSyncServer:GetUncompressedLength") << encSize;
            }
            if (decSize != size) {
                throw cybozu::Exception("x2aDirtyFullSyncServer:decSize differs") << decSize << size;
            }
            if (!snappy::RawUncompress(&encBuf[0], encSize, &buf[0])) {
                throw cybozu::Exception("x2aDirtyFullSyncServer:RawUncompress");
            }
            bd.write(&buf[0], size);
            remainingLb -= lb;
            c++;
        }
        logger.info("received %" PRIu64 " packets.", c);
        bd.fdatasync();
        logger.info("dirty-full-sync %s done.", volId.c_str());
    }

    uint64_t gidB, gidE;
    sPack.read(gidB);
    sPack.read(gidE);

    // TODO: Save to base file. (5) in archive-daemon.txt

    volInfo.setUuid(uuid);
    volInfo.setState("Archived");
    walb::packet::Ack(p.sock).send();
}

} // walb
