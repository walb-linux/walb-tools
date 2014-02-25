#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"
#include <snappy.h>
#include "walb/block_size.h"
#include "state_machine.hpp"
#include "raii_counter.hpp"
#include "state_map.hpp"
#include "constant.hpp"

namespace walb {

/**
 * Actions.
 */
const char *const aMerge = "Merge";
const char *const aApply = "Apply";
const char *const aRestore = "Restore";
const char *const aReplSync = "ReplSync";

inline bool isAllZero(const std::vector<int> &v)
{
    for (int i : v) {
        if (i != 0) return false;
    }
    return true;
}

/**
 * Manage one instance for each volume.
 */
struct ArchiveVolState
{
    std::atomic<int> stopState;
    StateMachine sm;
    std::mutex mutex;
    MultiRaiiCounter actionCounters;

    explicit ArchiveVolState(const std::string& volId) : stopState(NotStopping), actionCounters(mutex) {
        const struct StateMachine::Pair tbl[] = {
            { aClear, atInitVol },
            { atInitVol, aSyncReady },
            { aSyncReady, atClearVol },
            { atClearVol, aClear },

            { aSyncReady, atFullSync },
            { atFullSync, aArchived },

            { aArchived, atHashSync },
            { atHashSync, aArchived },
            { aArchived, atWdiffRecv },
            { atWdiffRecv, aArchived },

            { aArchived, atStop },
            { atStop, aStopped },

            { aStopped, atClearVol },
            { atClearVol, aClear },
            { aStopped, atStart },
            { atStart, aArchived },
        };
        sm.init(tbl);
        initInner(volId);
    }
private:
    void initInner(const std::string& volId);
};

struct ArchiveSingleton
{
    static ArchiveSingleton& getInstance() {
        static ArchiveSingleton instance;
        return instance;
    }

    std::string nodeId;
    std::string baseDirStr;
    std::string volumeGroup;

    StateMap<ArchiveVolState> stMap;
};

inline ArchiveSingleton& getArchiveGlobal()
{
    return ArchiveSingleton::getInstance();
}

const ArchiveSingleton& ga = getArchiveGlobal();

inline void ArchiveVolState::initInner(const std::string& volId)
{
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
    if (volInfo.existsVolDir()) {
        sm.set(volInfo.getState());
    } else {
        sm.set(aClear);
    }
}

inline ArchiveVolState &getArchiveVolState(const std::string &volId)
{
    return getArchiveGlobal().stMap.get(volId);
}

inline void c2aStatusServer(protocol::ServerParams &p)
{
    packet::Packet pkt(p.sock);
    StrVec params;
    pkt.read(params);

    if (params.empty()) {
        // for all volumes
        pkt.write("not implemented yet");
        // TODO
    } else {
        // for a volume
        const std::string &volId = params[0];
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
        pkt.write("ok");
        pkt.write(volInfo.getStatusAsStrVec());
    }
}

inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, "c2aInitVolServer", false);
    const std::string &volId = v[0];

    StateMachine &sm = getArchiveVolState(volId).sm;
    {
        StateMachineTransaction tran(sm, aClear, atInitVol, "c2ainitVolServer");
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
        volInfo.init();
        tran.commit(aSyncReady);
    }

    packet::Ack(p.sock).send();
}

inline void checkNoActionRunning(const std::string &volId, const char *msg)
{
    MultiRaiiCounter &actionCounters = getArchiveVolState(volId).actionCounters;
    std::vector<int> v = actionCounters.getValues({aMerge, aApply, aRestore, aReplSync});
    assert(v.size() == 4);
    if (!isAllZero(v)) {
        throw cybozu::Exception(msg)
            << "there are running actions"
            << v[0] << v[1] << v[2] << v[3];
    }
}

inline void c2aClearVolServer(protocol::ServerParams &p)
{
    StrVec v = protocol::recvStrVec(p.sock, 1, "c2aClearVolServer", false);
    const std::string &volId = v[0];

    checkNoActionRunning(volId, "c2aClearVolServer");
    ArchiveVolState &volSt = getArchiveVolState(volId);
    StateMachine &sm = volSt.sm;
    const std::string &currSt = sm.get(); // Stopped or SyncReady.
    {
        StateMachineTransaction tran(sm, currSt, atClearVol, "c2aClearVolServer");
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
        volInfo.clear();
        tran.commit(aClear);
    }

    // TODO: This will be race.
    std::unique_ptr<ArchiveVolState> volStP = getArchiveGlobal().stMap.del(volId);

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

    checkNoActionRunning(volId, "c2aStartServer");
    StateMachine &sm = getArchiveVolState(volId).sm;
    {
        StateMachineTransaction tran(sm, aStopped, atStart, "c2aStartServer");
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
        const std::string st = volInfo.getState();
        if (st != aStopped) {
            throw cybozu::Exception("c2aStartServer:not Stopped state") << st;
        }
        volInfo.setState(aArchived);
        tran.commit(aArchived);
    }

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
    const bool isForce = (int)cybozu::atoi(v[1]) != 0;

    ArchiveVolState &volSt = getArchiveVolState(volId);
    StateMachine &sm = volSt.sm;

    /*
     * Only one thread can make the stopping flag true at once.
     * Notify other threads working on the volume.
     */
    util::Stopper stopper(volSt.stopState);
    stopper.begin(isForce);

    /*
     * Wait for all background tasks to be stopped.
     * Tasks: all actions, HashSync, WdiffRecv, and FullSync.
     */
    size_t c = 0;
    for (;;) {
        if (c > DEFAULT_TIMEOUT) { // TODO: client should send timeout parameter.
            throw cybozu::Exception("c2aStopServer:timeout") << DEFAULT_TIMEOUT;
        }
        std::vector<int> v = volSt.actionCounters.getValues(
            {aMerge, aApply, aRestore, aReplSync});
        if (!isAllZero(v)) {
            util::sleepMs(1000);
            c++;
            continue;
        }
        const std::string &st = sm.get();
        if (st == atHashSync || st == atWdiffRecv || st == atFullSync) {
            util::sleepMs(1000);
            c++;
            continue;
        }
    }
    const std::string &st = sm.get();
    logger.info("Tasks have been stopped volId: %s state: %s"
                , volId.c_str(), st.c_str());
    if (st != aArchived) {
        packet::Ack(p.sock).send();
        return;
    }
    {
        StateMachineTransaction tran(sm, aArchived, atStop, "c2aStopServer");
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
        const std::string st = volInfo.getState();
        if (st != aArchived) {
            throw cybozu::Exception("c2aStopServer:not Archived state") << st;
        }
        volInfo.setState(aStopped);
        tran.commit(aStopped);
    }

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

    checkNoActionRunning(volId, "x2aDirtyFullSyncServer");
    ArchiveVolState &volSt = getArchiveVolState(volId);

    if (volSt.stopState != NotStopping) {
        cybozu::Exception e("x2aDirtyFullSyncServer:notStopping");
        e << volId << volSt.stopState;
        sPack.write(e.what());
        throw e;
    }

    StateMachine &sm = volSt.sm;
    {
        StateMachineTransaction tran(sm, aSyncReady, atFullSync, "x2aDirtyFullSyncServer");

        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);
        const std::string st = volInfo.getState();
        if (st != aSyncReady) {
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
                if (volSt.stopState == forceStopping || p.forceQuit) {
                    logger.warn("x2aDirtyFullSyncServer:force stopped:%s", volId.c_str());
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
        volInfo.setState(aArchived);

        tran.commit(aArchived);
    }

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
    packet::Packet pkt(p.sock);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    StateMachine &sm = volSt.sm;
    const std::string &st0 = sm.get();
    bool matched = false;
    for (const char *const st1 : {aArchived, atHashSync, atWdiffRecv}) {
        if (st0 == st1) {
            matched = true;
        }
    }
    if (!matched) {
        cybozu::Exception e("c2aRestoreServer:state is not matched");
        e << volId << st0;
        pkt.write(e.what());
        throw e;
    }

    std::unique_lock<RaiiCounter> counterLk(
        getArchiveVolState(volId).actionCounters.getLock(aRestore));

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup);

    // TODO: volinfo.restore(gid, volSt.forceStop, p.forceQuit);
    if (!volInfo.restore(gid)) {
        cybozu::Exception e("c2aRestoreServer:restore failed");
        e << volId << gid;
        pkt.write(e.what());
        throw e;
    }

    pkt.write("ok");
}

} // walb
