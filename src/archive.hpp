#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"
#include <algorithm>
#include <snappy.h>
#include "walb/block_size.h"
#include "state_machine.hpp"
#include "action_counter.hpp"
#include "atomic_map.hpp"
#include "constant.hpp"

namespace walb {

/**
 * Actions.
 */
const char *const aMerge = "Merge";
const char *const aApply = "Apply";
const char *const aRestore = "Restore";
const char *const aReplSync = "ReplSync";

/**
 * Manage one instance for each volume.
 */
struct ArchiveVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac;

    MetaDiffManager diffMgr;

    explicit ArchiveVolState(const std::string& volId)
        : stopState(NotStopping)
        , sm(mu)
        , ac(mu)
        , diffMgr() {
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

    /**
     * Read-only except for daemon initialization.
     */
    std::string nodeId;
    std::string baseDirStr;
    std::string volumeGroup;

    /**
     * Writable and must be thread-safe.
     */
    std::atomic<bool> forceQuit;
    AtomicMap<ArchiveVolState> stMap;
};

inline ArchiveSingleton& getArchiveGlobal()
{
    return ArchiveSingleton::getInstance();
}

const ArchiveSingleton& ga = getArchiveGlobal();

inline void ArchiveVolState::initInner(const std::string& volId)
{
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, diffMgr);
    if (volInfo.existsVolDir()) {
        sm.set(volInfo.getState());
        WalbDiffFiles wdiffs(diffMgr, volInfo.volDir.str());
        wdiffs.reload();
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

        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup,
                               getArchiveVolState(volId).diffMgr);
        pkt.write("ok");
        pkt.write(volInfo.getStatusAsStrVec());
    }
}

inline void c2aListVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = util::getDirNameList(ga.baseDirStr);
    protocol::sendStrVec(p.sock, v, 0, FUNC, false);
    ProtocolLogger logger(ga.nodeId, p.clientId);
    logger.debug() << FUNC << "succeeded";
}

inline void verifyNoArchiveActionRunning(const ActionCounters& ac, const char *msg)
{
    verifyNoActionRunning(ac, StrVec{aMerge, aApply, aRestore, aReplSync}, msg);
}

inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNoArchiveActionRunning(volSt.ac, FUNC);
    {
        StateMachineTransaction tran(volSt.sm, aClear, atInitVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        volInfo.init();
        tran.commit(aSyncReady);
    }

    packet::Ack(p.sock).send();
}

inline void c2aClearVolServer(protocol::ServerParams &p)
{
    const char *FUNC = __func__;
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    verifyNoArchiveActionRunning(volSt.ac, FUNC);
    StateMachine &sm = volSt.sm;
    const std::string &currSt = sm.get(); // Stopped or SyncReady.
    {
        StateMachineTransaction tran(sm, currSt, atClearVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        volInfo.clear();
        tran.commit(aClear);
    }

    packet::Ack(p.sock).send();

    ProtocolLogger logger(ga.nodeId, p.clientId);
    logger.info() << FUNC << "cleared volId" << volId;
}

/**
 * "start" command.
 * params[0]: volId
 */
inline void c2aStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];

    ArchiveVolState& volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNoArchiveActionRunning(volSt.ac, FUNC);
    StateMachine &sm = volSt.sm;
    {
        StateMachineTransaction tran(sm, aStopped, atStart, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup,
                               getArchiveVolState(volId).diffMgr);
        const std::string st = volInfo.getState();
        if (st != aStopped) {
            throw cybozu::Exception(FUNC) << "not Stopped state" << st;
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
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &volId = v[0];
    const bool isForce = (int)cybozu::atoi(v[1]) != 0;

    ArchiveVolState &volSt = getArchiveVolState(volId);
    packet::Ack(p.sock).send();

    Stopper stopper(volSt.stopState, isForce);
    if (!stopper.isSuccess()) return;

    UniqueLock ul(volSt.mu);
    StateMachine &sm = volSt.sm;

    waitUntil(ul, [&]() {
            bool go = volSt.ac.isAllZero(StrVec{aMerge, aApply, aRestore, aReplSync});
            if (!go) return false;
            return isStateIn(sm.get(), {aClear, aSyncReady, aArchived, aStopped});
        }, FUNC);

    const std::string &st = sm.get();
    logger.info() << "Tasks have been stopped"
                  << "volId" << volId
                  << "state" << st;
    if (st != aArchived) {
        return;
    }

    StateMachineTransaction tran(sm, aArchived, atStop, FUNC);
    ul.unlock();
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
    const std::string fst = volInfo.getState();
    if (fst != aArchived) {
        throw cybozu::Exception(FUNC) << "not Archived state" << fst;
    }
    volInfo.setState(aStopped);
    tran.commit(aStopped);
}

/**
 * Execute dirty full sync protocol as server.
 * Client is storage server or another archive server.
 */
inline void x2aDirtyFullSyncServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);

    walb::packet::Packet sPack(p.sock);
    std::string hostType, volId;
    cybozu::Uuid uuid;
    uint64_t sizeLb, curTime, bulkLb;
    sPack.read(hostType);
    if (hostType != storageHT && hostType != archiveHT) {
        throw cybozu::Exception(FUNC) << "invalid hostType" << hostType;
    }
    sPack.read(volId);
    sPack.read(uuid);
    sPack.read(sizeLb);
    sPack.read(curTime);
    sPack.read(bulkLb);
    if (bulkLb == 0) {
        throw cybozu::Exception(FUNC) << "bulkLb is zero";
    }

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyNoArchiveActionRunning(volSt.ac, FUNC);

    StateMachine &sm = volSt.sm;
    {
        StateMachineTransaction tran(sm, aSyncReady, atFullSync, FUNC);
        ul.unlock();

        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        const std::string st = volInfo.getState();
        if (st != aSyncReady) {
            throw cybozu::Exception(FUNC) << "state is not SyncReady" << st;
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
                if (volSt.stopState == ForceStopping || ga.forceQuit) {
                    logger.warn() << FUNC << "force stopped" << volId;
                    return;
                }
                const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
                const size_t size = lb * LOGICAL_BLOCK_SIZE;
                size_t encSize;
                sPack.read(encSize);
                if (encSize == 0) {
                    throw cybozu::Exception(FUNC) << "encSize is zero";
                }
                encBuf.resize(encSize);
                sPack.read(&encBuf[0], encSize);
                size_t decSize;
                if (!snappy::GetUncompressedLength(&encBuf[0], encSize, &decSize)) {
                    throw cybozu::Exception(FUNC)
                        << "GetUncompressedLength" << encSize;
                }
                if (decSize != size) {
                    throw cybozu::Exception(FUNC)
                        << "decSize differs" << decSize << size;
                }
                if (!snappy::RawUncompress(&encBuf[0], encSize, &buf[0])) {
                    throw cybozu::Exception(FUNC) << "RawUncompress";
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
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &volId = v[0];
    const uint64_t gid = cybozu::atoi(v[1]);
    packet::Packet pkt(p.sock);

    try {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        const StateMachine &sm = volSt.sm;
        verifyStateIn(sm.get(), {aArchived, atHashSync, atWdiffRecv}, FUNC);

        ActionCounterTransaction tran(volSt.ac, volId);
        ul.unlock();

        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        // TODO: volinfo.restore(gid, volSt.forceStop, ga.forceQuit);
        if (!volInfo.restore(gid)) {
            throw cybozu::Exception(FUNC) << "restore failed" << volId << gid;
        }
        pkt.write("ok");

    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }
}

/**
 * params[0]: volId.
 *
 * !!!CAUSION!!!
 * This is for test and debug.
 */
inline void c2aReloadMetadataServer(protocol::ServerParams &p)
{
    const char * const FUNC = __func__;
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyNoArchiveActionRunning(volSt.ac, FUNC);
    {
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        WalbDiffFiles wdiffs(volSt.diffMgr, volInfo.volDir.str());
        wdiffs.reload();
    }
    packet::Ack(p.sock).send();
}

namespace proxy_local {

/**
 * Wdiff header has been written already before calling this.
 */
inline void recvAndWriteDiffs(cybozu::Socket &sock, diff::Writer &writer, Logger &logger)
{
    const char *const FUNC = __func__;
    packet::StreamControl ctrl(sock);
    while (ctrl.isNext()) {
        diff::PackHeader packH;
        sock.read(packH.rawData(), packH.rawSize());
        if (!packH.isValid()) {
            cybozu::Exception e(FUNC);
            e << "bad packH";
            logger.throwError(e);
        }
        for (size_t i = 0; i < packH.nRecords(); i++) {
            diff::IoData io;
            const walb_diff_record& rec = packH.record(i);
            io.set(rec);
            if (rec.data_size == 0) {
                writer.writeDiff(rec, {});
                continue;
            }
            sock.read(io.data.data(), rec.data_size);
            if (!io.isValid()) {
                cybozu::Exception e(FUNC);
                e << "bad io";
                logger.throwError(e);
            }
            uint32_t csum = io.calcChecksum();
            if (csum != rec.checksum) {
                cybozu::Exception e(FUNC);
                e << "bad io checksum" << csum << rec.checksum;
                logger.throwError(e);
            }
            writer.writeDiff(rec, std::move(io.data));
        }
        ctrl.reset();
    }
    if (!ctrl.isEnd()) {
        cybozu::Exception e(FUNC);
        e << "bad ctrl not end";
        throw e;
    }
}

} // proxy_local

inline void x2aWdiffTransferServer(protocol::ServerParams &p)
{
    const char * const FUNC = __func__;

    ProtocolLogger logger(ga.nodeId, p.clientId);

    packet::Packet pkt(p.sock);
    std::string volId;
    pkt.read(volId);
    if (volId.empty()) {
        throw cybozu::Exception(FUNC) << "empty volId";
    }
    std::string clientType;
    pkt.read(clientType);
    if (clientType != proxyHT && clientType != archiveHT) {
        throw cybozu::Exception(FUNC) << "bad clientType" << clientType;
    }
    cybozu::Uuid uuid;
    pkt.read(uuid);
    uint16_t maxIoBlocks;
    pkt.read(maxIoBlocks);
    uint64_t sizeLb;
    pkt.read(sizeLb);
    MetaDiff diff;
    pkt.read(diff);

    logger.debug() << FUNC << volId << uuid << maxIoBlocks << sizeLb << diff;

    ArchiveVolState& volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
    if (!volInfo.existsVolDir()) {
        const char *msg = "archive-not-found";
        logger.info() << msg << volId;
        pkt.write(msg);
        return;
    }
    StateMachine &sm = volSt.sm;
    if (sm.get() == aStopped) {
        const char *msg = "stopped";
        logger.info() << msg << volId;
        pkt.write(msg);
        return;
    }
    if (clientType == "proxy" && volInfo.getUuid() != uuid) {
        const char *msg = "different-uuid";
        logger.info() << msg << volId;
        pkt.write(msg);
        return;
    }
    const uint64_t curSizeLb = volInfo.getLv().sizeLb();
    if (curSizeLb < sizeLb) {
        const char *msg = "large-lv-size";
        logger.error() << msg << volId;
        pkt.write(msg);
        return;
    }
    if (sizeLb < curSizeLb) {
        logger.warn() << "small lv size" << volId << sizeLb << curSizeLb;
    }
    const MetaState metaState = volInfo.getMetaState();
    const MetaSnap latestSnap = volSt.diffMgr.getLatestSnapshot(metaState);
    const Relation rel = getRelation(latestSnap, diff);

    if (rel != Relation::APPLICABLE_DIFF) {
        const char *msg = getRelationStr(rel);
        logger.info() << msg << volId;
        pkt.write(msg);
        return;
    }
    pkt.write("ok");

    StateMachineTransaction tran(sm, aArchived, atWdiffRecv, FUNC);
    ul.unlock();

    const std::string fName = createDiffFileName(diff);
    const std::string baseDir = volInfo.volDir.str();
    cybozu::TmpFile tmpFile(baseDir);
    cybozu::FilePath fPath(baseDir);
    fPath += fName;
    diff::Writer writer(tmpFile.fd());
    diff::FileHeaderRaw fileH;
    fileH.setMaxIoBlocksIfNecessary(maxIoBlocks);
    fileH.setUuid(uuid.rawData());
    writer.writeHeader(fileH);
    logger.debug() << FUNC << "write header";
    proxy_local::recvAndWriteDiffs(p.sock, writer, logger);
    logger.debug() << FUNC << "close";
    writer.close();
    tmpFile.save(fPath.str());

    ul.lock();
    volSt.diffMgr.add(diff);
    tran.commit(aArchived);

    packet::Ack(p.sock).send();
}

inline void c2aResizeServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

} // walb
