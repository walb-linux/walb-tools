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
            { aStopped, atResetVol },
            { atResetVol, aSyncReady },
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
    size_t maxForegroundTasks;

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
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);
    StrVec params;
    pkt.read(params);

    StrVec statusStrVec;
    bool sendErr = true;
    try {
        if (params.empty()) {
            // for all volumes
            throw cybozu::Exception("not implemented yet");
            // TODO
        } else {
            // for a volume
            const std::string &volId = params[0];
            ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup,
                                   getArchiveVolState(volId).diffMgr);
            statusStrVec = volInfo.getStatusAsStrVec();
        }
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(statusStrVec);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aListVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = util::getDirNameList(ga.baseDirStr);
    protocol::sendStrVec(p.sock, v, 0, FUNC, false);
    packet::Ack(p.sock).send();
    ProtocolLogger logger(ga.nodeId, p.clientId);
    logger.debug() << "listVol succeeded";
}

inline void verifyNoArchiveActionRunning(const ActionCounters& ac, const char *msg)
{
    verifyNoActionRunning(ac, StrVec{aMerge, aApply, aRestore, aReplSync}, msg);
}

inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    try {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);

        StateMachineTransaction tran(volSt.sm, aClear, atInitVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        volInfo.init();
        tran.commit(aSyncReady);
        pkt.write(msgOk);
        logger.info() << "initVol succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

inline void c2aClearVolServer(protocol::ServerParams &p)
{
    const char *FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    try {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNoArchiveActionRunning(volSt.ac, FUNC);
        StateMachine &sm = volSt.sm;
        const std::string &currSt = sm.get(); // aStopped or aSyncReady

        StateMachineTransaction tran(sm, currSt, atClearVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        volInfo.clear();
        tran.commit(aClear);
        pkt.write(msgOk);
        logger.info() << "clearVol succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
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
    packet::Packet pkt(p.sock);

    try {
        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);

        StateMachineTransaction tran(volSt.sm, aStopped, atStart, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup,
                               getArchiveVolState(volId).diffMgr);
        const std::string st = volInfo.getState();
        if (st != aStopped) {
            throw cybozu::Exception(FUNC) << "not Stopped state" << st;
        }
        volInfo.setState(aArchived);
        tran.commit(aArchived);

        pkt.write(msgOk);
        logger.info() << "start succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
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
    packet::Packet pkt(p.sock);

    try {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        Stopper stopper(volSt.stopState, isForce);
        if (!stopper.isSuccess()) {
            throw cybozu::Exception(FUNC) << "already under stopping" << volId;
        }
        UniqueLock ul(volSt.mu);
        StateMachine &sm = volSt.sm;

        waitUntil(ul, [&]() {
                bool go = volSt.ac.isAllZero(StrVec{aMerge, aApply, aRestore, aReplSync});
                if (!go) return false;
                return isStateIn(sm.get(), {aClear, aSyncReady, aArchived, aStopped});
            }, FUNC);

        logger.info() << "Tasks have been stopped" << volId;
        verifyStateIn(sm.get(), {aArchived}, FUNC);

        StateMachineTransaction tran(sm, aArchived, atStop, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        const std::string fst = volInfo.getState();
        if (fst != aArchived) {
            throw cybozu::Exception(FUNC) << "not Archived state" << fst;
        }
        volInfo.setState(aStopped);
        tran.commit(aStopped);

        pkt.write(msgOk);
        logger.info() << "stop succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

namespace archive_local {

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool recvDirtyFullImage(
    packet::Packet &pkt, ArchiveVolInfo &volInfo,
    uint64_t sizeLb, uint64_t bulkLb, const std::atomic<int> &stopState)
{
    const char *const FUNC = __func__;
    const std::string lvPath = volInfo.getLv().path().str();
    cybozu::util::BlockDevice bd(lvPath, O_RDWR);
    std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
    std::vector<char> encBuf;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ga.forceQuit) {
            return false;
        }
        const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        size_t encSize;
        pkt.read(encSize);
        if (encSize == 0) {
            throw cybozu::Exception(FUNC) << "encSize is zero";
        }
        encBuf.resize(encSize);
        pkt.read(&encBuf[0], encSize);
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
    LOGs.debug() << "number of received packets" << c;
    bd.fdatasync();
    return true;
}

} // archive_local

/**
 * Execute dirty full sync protocol as server.
 * Client is storage server or another archive server.
 */
inline void x2aDirtyFullSyncServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);

    walb::packet::Packet pkt(p.sock);
    std::string hostType, volId;
    cybozu::Uuid uuid;
    uint64_t sizeLb, curTime, bulkLb;
    pkt.read(hostType);
    if (hostType != storageHT && hostType != archiveHT) {
        throw cybozu::Exception(FUNC) << "invalid hostType" << hostType;
    }
    pkt.read(volId);
    pkt.read(uuid);
    pkt.read(sizeLb);
    pkt.read(curTime);
    pkt.read(bulkLb);
    logger.debug() << hostType << volId << uuid << sizeLb << curTime << bulkLb;

    ForegroundCounterTransaction foregroundTasksTran;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    StateMachine &sm = volSt.sm;

    try {
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb is zero";
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);
        verifyStateIn(sm.get(), {aSyncReady}, FUNC);
    } catch (std::exception &e) {
        logger.warn() << e.what();
        pkt.write(e.what());
        return;
    }
    pkt.write(msgAccept);

    StateMachineTransaction tran(sm, aSyncReady, atFullSync, FUNC);
    ul.unlock();

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
    const std::string st = volInfo.getState();
    if (st != aSyncReady) {
        throw cybozu::Exception(FUNC) << "state is not SyncReady" << st;
    }
    volInfo.createLv(sizeLb);

    if (!archive_local::recvDirtyFullImage(pkt, volInfo, sizeLb, bulkLb, volSt.stopState)) {
        logger.warn() << FUNC << "force stopped" << volId;
        return;
    }

    uint64_t gidB, gidE;
    pkt.read(gidB);
    pkt.read(gidE);

    walb::MetaSnap snap(gidB, gidE);
    walb::MetaState state(snap, curTime);
    volInfo.setMetaState(state);

    volInfo.setUuid(uuid);
    volInfo.setState(aArchived);

    tran.commit(aArchived);

    walb::packet::Ack(p.sock).send();
    logger.info() << "dirty-full-sync succeeded" << volId;
}


namespace archive_local {

/**
 * Restore a snapshot.
 * (1) create lvm snapshot of base lv. (with temporal lv name)
 * (2) apply appropriate wdiff files.
 * (3) rename the lvm snapshot.
 *
 * RETURN:
 *   false if force stopped.
 */
inline bool restore(const std::string &volId, uint64_t gid)
{
    using namespace walb::diff;

    const char *const FUNC = __func__;

    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    cybozu::lvm::Lv lv = volInfo.getLv();
    const std::string targetName = volInfo.restoredSnapshotName(gid);
    const std::string tmpLvName = targetName + "_tmp";
    if (lv.hasSnapshot(tmpLvName)) {
        lv.getSnapshot(tmpLvName).remove();
    }
    if (lv.hasSnapshot(targetName)) {
        throw cybozu::Exception(FUNC) << "already restored" << volId << gid;
    }
    cybozu::lvm::Lv lvSnap = lv.takeSnapshot(tmpLvName, true);
    const int maxRetryNum = 10;
    int retryNum = 0;
    std::vector<cybozu::util::FileOpener> ops;
  retry:
    {
        const std::vector<MetaDiff> metaDiffList = volSt.diffMgr.getDiffListToRestore(volInfo.getMetaState(), gid);
        if (metaDiffList.empty()) {
            throw cybozu::Exception(FUNC)
                << "can not restore" << volId << gid;
        }
        // apply wdiff files indicated by metaDiffList to lvSnap.
        for (const MetaDiff& diff : metaDiffList) {
            cybozu::util::FileOpener op;
            if (!op.open(volInfo.getDiffPath(diff).str(), O_RDONLY)) {
                retryNum++;
                if (retryNum == maxRetryNum) {
                    throw cybozu::Exception(FUNC) << "exceed max retry";
                }
                ops.clear();
                goto retry;
            }
            ops.push_back(std::move(op));
        }
    }
    diff::Merger merger;
    merger.addWdiffs(std::move(ops));
    diff::RecIo recIo;
    cybozu::util::BlockDevice bd(lvSnap.path().str(), O_RDWR);
    std::vector<char> zero;
	const uint64_t lvSnapSizeLb = lvSnap.sizeLb();
    while (merger.pop(recIo)) {
        if (volSt.stopState == ForceStopping || ga.forceQuit) {
            return false;
        }
        const DiffRecord& rec = recIo.record();
        assert(!rec.isCompressed());
        const uint64_t ioAddress = rec.io_address;
        const uint64_t ioBlocks = rec.io_blocks;
		//LOGs.debug() << "ioAddress" << ioAddress << "ioBlocks" << ioBlocks;
		if (ioAddress + ioBlocks > lvSnapSizeLb) {
			throw cybozu::Exception(FUNC) << "out of range" << ioAddress << ioBlocks << lvSnapSizeLb;
		}
        const uint64_t ioAddrB = ioAddress * LOGICAL_BLOCK_SIZE;
        const uint64_t ioSizeB = ioBlocks * LOGICAL_BLOCK_SIZE;

        const char *data;
        // Curently a discard IO is converted to an all-zero IO.
        if (rec.isAllZero() || rec.isDiscard()) {
            if (zero.size() < ioSizeB) zero.resize(ioSizeB);
            data = &zero[0];
        } else {
            data = recIo.io().get();
        }
        bd.write(ioAddrB, ioSizeB, data);
    }
    bd.fdatasync();
    bd.close();
    cybozu::lvm::renameLv(lv.vgName(), tmpLvName, targetName);
    return true;
}

/**
 * Drop a restored volume.
 */
inline void drop(const std::string &volId, uint64_t gid)
{
    const char *const FUNC = __func__;

    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    cybozu::lvm::Lv lv = volInfo.getLv();
    const std::string targetName = volInfo.restoredSnapshotName(gid);
    if (!lv.hasSnapshot(targetName)) {
        throw cybozu::Exception(FUNC)
            << "restored volume not found" << volId << gid;
    }
    lv.getSnapshot(targetName).remove();
}

} // namespace archive_local

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

    ForegroundCounterTransaction foregroundTasksTran;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    try {
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived, atHashSync, atWdiffRecv}, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aRestore}, FUNC);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
        return;
    }
    pkt.write(msgAccept);

    ActionCounterTransaction tran(volSt.ac, volId);
    ul.unlock();
    if (!archive_local::restore(volId, gid)) {
        logger.warn() << FUNC << "force stopped" << volId << gid;
        return;
    }
    logger.info() << "restore succeeded" << volId << gid;
}

/**
 * Drop command.
 */
inline void c2aDropServer(protocol::ServerParams &p)
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
        verifyStateIn(volSt.sm.get(), {aArchived, atHashSync, atWdiffRecv}, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aRestore}, FUNC);
        ul.unlock();

        archive_local::drop(volId, gid);
        logger.info() << "drop succeeded" << volId << gid;
        pkt.write(msgOk);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
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
    ProtocolLogger logger(ga.nodeId, p.clientId);
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    try {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);

        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        WalbDiffFiles wdiffs(volSt.diffMgr, volInfo.volDir.str());
        wdiffs.reload();
        pkt.write(msgOk);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

namespace proxy_local {

/**
 * Wdiff header has been written already before calling this.
 *
 * RETURN:
 *   false if force stopped.
 */
inline bool recvAndWriteDiffs(
    cybozu::Socket &sock, diff::Writer &writer, const std::atomic<int> &stopState)
{
    const char *const FUNC = __func__;
    packet::StreamControl ctrl(sock);
    while (ctrl.isNext()) {
        if (stopState == ForceStopping || ga.forceQuit) {
            return false;
        }
        DiffPackHeader packH;
        sock.read(packH.rawData(), packH.rawSize());
        if (!packH.isValid()) {
            throw cybozu::Exception(FUNC) << "bad packH";
        }
        for (size_t i = 0; i < packH.nRecords(); i++) {
            DiffIo io;
            const DiffRecord& rec = packH.record(i);
            io.set(rec);
            if (rec.data_size == 0) {
                writer.writeDiff(rec, {});
                continue;
            }
            sock.read(io.get(), rec.data_size);
            if (!io.isValid()) {
                throw cybozu::Exception(FUNC) << "bad io";
            }
            uint32_t csum = io.calcChecksum();
            if (csum != rec.checksum) {
                throw cybozu::Exception(FUNC)
                    << "bad io checksum" << csum << rec.checksum;
            }
            writer.writeDiff(rec, std::move(io.data));
        }
        ctrl.reset();
    }
    if (!ctrl.isEnd()) {
        throw cybozu::Exception(FUNC) << "bad ctrl not end";
    }
    return true;
}

} // proxy_local

inline void x2aWdiffTransferServer(protocol::ServerParams &p)
{
    const char * const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);
    std::string volId;
    std::string hostType;
    cybozu::Uuid uuid;
    uint16_t maxIoBlocks;
    uint64_t sizeLb;
    MetaDiff diff;

    pkt.read(volId);
    pkt.read(hostType);
    pkt.read(uuid);
    pkt.read(maxIoBlocks);
    pkt.read(sizeLb);
    pkt.read(diff);
    logger.debug() << "recv" << volId << hostType << uuid << maxIoBlocks << sizeLb << diff;

    ForegroundCounterTransaction foregroundTasksTran;
    ArchiveVolState& volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    StateMachine &sm = volSt.sm;
    try {
        if (volId.empty()) {
            throw cybozu::Exception(FUNC) << "empty volId";
        }
        if (hostType != proxyHT && hostType != archiveHT) {
            throw cybozu::Exception(FUNC) << "bad hostType" << hostType;
        }
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(sm.get(), {aArchived, aStopped}, FUNC);
    } catch (std::exception &e) {
        logger.warn() << e.what();
        pkt.write(e.what());
        return;
    }

    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
    if (!volInfo.existsVolDir()) {
        const char *msg = "archive-not-found";
        logger.info() << msg << volId;
        pkt.write(msg);
        return;
    }
    if (sm.get() == aStopped) {
        const char *msg = "stopped";
        logger.info() << msg << volId;
        pkt.write(msg);
        return;
    }
    if (hostType == proxyHT && volInfo.getUuid() != uuid) {
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
    pkt.write(msgAccept);

    StateMachineTransaction tran(sm, aArchived, atWdiffRecv, FUNC);
    ul.unlock();

    const std::string fName = createDiffFileName(diff);
    const std::string baseDir = volInfo.volDir.str();
    cybozu::TmpFile tmpFile(baseDir);
    cybozu::FilePath fPath(baseDir);
    fPath += fName;
    diff::Writer writer(tmpFile.fd());
    DiffFileHeader fileH;
    fileH.setMaxIoBlocksIfNecessary(maxIoBlocks);
    fileH.setUuid(uuid.rawData());
    writer.writeHeader(fileH);
    logger.debug() << FUNC << "write header";
    if (!proxy_local::recvAndWriteDiffs(p.sock, writer, volSt.stopState)) {
        logger.warn() << FUNC << "force stopped" << volId;
        return;
    }
    logger.debug() << FUNC << "close";
    writer.close();
    tmpFile.save(fPath.str());

    ul.lock();
    volSt.diffMgr.add(diff);
    tran.commit(aArchived);

    packet::Ack(p.sock).send();
    logger.info() << "wdiff-transfer succeeded" << volId;
}

inline void c2aResizeServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline void c2aHostTypeServer(protocol::ServerParams &p)
{
    protocol::runHostTypeServer(p, archiveHT);
}

/**
 * params[0]: volId
 */
inline void c2aResetVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC, false);
        if (v.empty()) {
            throw cybozu::Exception(FUNC) << "specify volId";
        }
        const std::string &volId = v[0];

        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);
        const std::string &currSt = volSt.sm.get(); // aStopped or aSyncReady

        StateMachineTransaction tran(volSt.sm, currSt, atResetVol, FUNC);
        ul.unlock();

        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup,
                               getArchiveVolState(volId).diffMgr);
        volInfo.clear();
        volInfo.init();
        tran.commit(aSyncReady);

        pkt.write(msgOk);
        logger.info() << "reset succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

} // walb
