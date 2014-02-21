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

inline ArchiveSingleton& getArchiveGlobal()
{
    return ArchiveSingleton::getInstance();
}

const ArchiveSingleton& ga = getArchiveGlobal();

inline void c2aStatusServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, "c2aInitVolServer", false);
    const std::string &volId = v[0];

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    volInfo.init();

    packet::Ack(p.sock).send();
}

inline void c2aClearVolServer(protocol::ServerParams &p)
{
    StrVec v = protocol::recvStrVec(p.sock, 1, "c2aClearVolServer", false);
    const std::string &volId = v[0];

    // TODO: exclusive access for the volId.

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    volInfo.clear();

    packet::Ack(p.sock).send();

    ProtocolLogger logger(ga.nodeId, p.clientId);
    logger.info("c2aClearVolServer: cleared volId %s", volId.c_str());
}

/**
 * "start" command.
 * params[0]: volId
 */
inline void c2aStartServer(protocol::ServerParams &p)
{
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 1, "c2aStopServer", false);
    const std::string &volId = v[0];

    /*
     * TODO:
     *   Exclusive access for the volId.
     */

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    if (!volInfo.existsVolDir()) {
        throw cybozu::Exception("not exists volume directory") << volId;
    }
    const std::string st = volInfo.getState();
    if (st != "Stopped") {
        throw cybozu::Exception("c2aStartServer:not Stopped state") << st;
    }
    volInfo.setState("Archived");
    packet::Ack(p.sock).send();
}

/**
 * command "stop"
 * params[0]: volId
 * params[1]: isForce
 */
inline void c2aStopServer(protocol::ServerParams &p)
{
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 2, "c2aStopServer", false);
    const std::string &volId = v[0];
    const int isForceInt = cybozu::atoi(v[1]);
    UNUSED const bool isForce = (isForceInt != 0);

    /*
     * TODO:
     *   Exclusive access for the volId.
     *   Force stop related tasks if isForce is true.
     */

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    if (!volInfo.existsVolDir()) {
        throw cybozu::Exception("not exists volume directory") << volId;
    }
    const std::string st = volInfo.getState();
    if (st == "Stopped") {
        logger.info("already stopped %s", volId.c_str());
        packet::Ack(p.sock).send();
        return;
    }
    if (st != "Archived") {
        throw cybozu::Exception("c2aStopServer:not Archived state") << st;
    }
    volInfo.setState("Stopped");
    packet::Ack(p.sock).send();
}

/**
 * Execute dirty full sync protocol as server.
 * Client is storage server or another archive server.
 */
inline void x2aDirtyFullSyncServer(protocol::ServerParams &p)
{
    ProtocolLogger logger(ga.nodeId, p.clientId);

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

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    const std::string st = volInfo.getState();
    if (st != "SyncReady") {
        throw cybozu::Exception("x2aDirtyFullSyncServer:state is not SyncReady") << st;
    }
    volInfo.createLv(sizeLb);
    sPack.write("ok");

    // recv and write.
    {
        std::string lvPath = volInfo.getLv().path().str();
        cybozu::util::BlockDevice bd(lvPath, O_RDWR);
        std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
        std::vector<char> encBuf;

        uint64_t c = 0;
        uint64_t remainingLb = sizeLb;
        while (0 < remainingLb) {
            if (p.forceQuit) {
                logger.warn("x2aDirtyFullSyncServer:force stopped");
                return;
            }
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

    walb::MetaSnap snap(gidB, gidE);
    walb::MetaState state(snap, curTime);
    volInfo.setMetaState(state);

    volInfo.setUuid(uuid);
    volInfo.setState("Archived");
    walb::packet::Ack(p.sock).send();
}

/**
 * Restore command.
 */
inline void c2aRestoreServer(protocol::ServerParams &p)
{
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 2, "c2aRestoreServer", false);
    const std::string &volId = v[0];
    const uint64_t gid = cybozu::atoi(v[1]);

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    const std::string st = volInfo.getState();
    packet::Packet pkt(p.sock);
    if (st != "Archived") {
        cybozu::Exception e("c2aRestoreServer:state is not Archived");
        e << volId << st;
        pkt.write(e.what());
        throw e;
    }
    if (!volInfo.restore(gid)) {
        cybozu::Exception e("c2aRestoreServer:restore failed");
        e << volId << gid;
        pkt.write(e.what());
        throw e;
    }

    pkt.write("ok");
}

} // walb
