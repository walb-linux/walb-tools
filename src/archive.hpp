#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"
#include <algorithm>
#include <snappy.h>
#include "walb/block_size.h"
#include "walb_diff_virt.hpp"
#include "murmurhash3.hpp"
#include "state_machine.hpp"
#include "action_counter.hpp"
#include "atomic_map.hpp"
#include "constant.hpp"
#include "host_info.hpp"
#include "dirty_full_sync.hpp"
#include "dirty_hash_sync.hpp"
#include "wdiff_transfer.hpp"

namespace walb {

/**
 * Actions.
 */
const char *const aMerge = "Merge";
const char *const aApply = "Apply";
const char *const aRestore = "Restore";
const char *const aReplSync = "ReplSyncAsClient";
const char *const aResize = "Resize";

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

            { aSyncReady, atReplSync },
            { atReplSync, aArchived },

            { aArchived, atHashSync },
            { atHashSync, aArchived },
            { aArchived, atWdiffRecv },
            { atWdiffRecv, aArchived },
            { aArchived, atReplSync },
            { atReplSync, aArchived },

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
    size_t socketTimeout;

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

inline ArchiveVolState &getArchiveVolState(const std::string &volId)
{
    return getArchiveGlobal().stMap.get(volId);
}

inline void verifyNoArchiveActionRunning(const ActionCounters& ac, const char *msg)
{
    verifyNoActionRunning(ac, StrVec{aMerge, aApply, aRestore, aReplSync, aResize}, msg);
}

namespace archive_local {

template <typename F>
inline std::pair<MetaState, MetaDiffVec> tryOpenDiffs(
    std::vector<cybozu::util::File>& fileV, ArchiveVolInfo& volInfo,
    bool allowEmpty, F getDiffList)
{
    const char *const FUNC = __func__;

    const int maxRetryNum = 10;
    int retryNum = 0;
  retry:
    const MetaState st = volInfo.getMetaState();
    const MetaDiffVec diffV = getDiffList(st);
    if (!allowEmpty && diffV.empty()) {
        throw cybozu::Exception(FUNC) << "diffV empty" << volInfo.volId;
    }
    // Try to open all wdiff files.
    for (const MetaDiff& diff : diffV) {
        cybozu::util::File op;
        if (!op.open(volInfo.getDiffPath(diff).str(), O_RDONLY)) {
            retryNum++;
            if (retryNum == maxRetryNum) {
                throw cybozu::Exception(FUNC) << "exceed max retry";
            }
            fileV.clear();
            goto retry;
        }
        fileV.push_back(std::move(op));
    }
    return {st, diffV};
}

const bool allowEmpty = true;

inline void prepareVirtualFullScanner(
    diff::VirtualFullScanner &virt,
    ArchiveVolInfo &volInfo, uint64_t sizeLb, const MetaSnap &snap)
{
    const char *const FUNC = __func__;

    cybozu::lvm::Lv lv = volInfo.getLv();
    if (sizeLb != lv.sizeLb()) {
        throw cybozu::Exception(FUNC) << "bad sizeLb" << sizeLb << lv.sizeLb();
    }
    std::vector<cybozu::util::File> fileV;
    tryOpenDiffs(fileV, volInfo, allowEmpty, [&](const MetaState &st) {
            return volInfo.getDiffMgr().getDiffListToSync(st, snap);
        });
    cybozu::util::File fileR(lv.path().str(), O_RDONLY);
    virt.init(std::move(fileR), std::move(fileV));
}

inline void verifyApplicable(const std::string& volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
    UniqueLock ul(volSt.mu);

    const MetaState st = volInfo.getMetaState();
    if (volSt.diffMgr.getDiffListToApply(st, gid).empty()) {
        throw cybozu::Exception(__func__) << "There is no diff to apply" << volId;
    }
}

inline bool applyOpenedDiffs(std::vector<cybozu::util::File>&& fileV, cybozu::lvm::Lv& lv, const std::atomic<int>& stopState)
{
    const char *const FUNC = __func__;
    diff::Merger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();
    DiffRecIo recIo;
    cybozu::util::BlockDevice bd(lv.path().str(), O_RDWR);
    std::vector<char> zero;
	const uint64_t lvSnapSizeLb = lv.sizeLb();
    while (merger.getAndRemove(recIo)) {
        if (stopState == ForceStopping || ga.forceQuit) {
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
            data = zero.data();
        } else {
            data = recIo.io().get();
        }
        bd.write(ioAddrB, ioSizeB, data);
    }
    bd.fdatasync();
    bd.close();
    return true;
}

inline bool applyDiffsToVolume(const std::string& volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    MetaDiffManager &mgr = volSt.diffMgr;
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, mgr);

    std::vector<cybozu::util::File> fileV;
    MetaState st0;
    MetaDiffVec diffV;
    std::tie(st0, diffV) = tryOpenDiffs(
        fileV, volInfo, !allowEmpty, [&](const MetaState &st) {
            return mgr.getDiffListToApply(st, gid);
        });

    const MetaState st1 = applying(st0, diffV);
    volInfo.setMetaState(st1);

    cybozu::lvm::Lv lv = volInfo.getLv();
    if (!applyOpenedDiffs(std::move(fileV), lv, volSt.stopState)) {
        return false;
    }

    const MetaState st2 = apply(st0, diffV);
    volInfo.setMetaState(st2);

    volInfo.removeDiffs(diffV);
    return true;
}

inline void verifyMergeable(const std::string &volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    MetaDiffVec diffV = volSt.diffMgr.getMergeableDiffList(gid);
    if (diffV.size() < 2) {
        throw cybozu::Exception(__func__) << "There is no mergeable diff.";
    }
}

inline bool mergeDiffs(const std::string &volId, uint64_t gidB, bool isSize, uint64_t param3)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    MetaDiffManager &mgr = volSt.diffMgr;
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, mgr);

    std::vector<cybozu::util::File> fileV;
    MetaState st0;
    MetaDiffVec diffV;
    std::tie(st0, diffV) = tryOpenDiffs(
        fileV, volInfo, allowEmpty, [&](const MetaState &) {
            if (isSize) {
                const uint64_t maxSize = param3 * MEBI;
                return volInfo.getDiffListToMerge(gidB, maxSize);
            } else {
                const uint64_t gidE = param3;
                return volInfo.getDiffListToMergeGid(gidB, gidE);
            }
        });
    if (fileV.size() < 2) {
        throw cybozu::Exception(__func__) << "There is no mergeable diff.";
    }

    const MetaDiff mergedDiff = merge(diffV);
    const cybozu::FilePath diffPath = volInfo.getDiffPath(mergedDiff);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    diff::Merger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();

    diff::Writer writer(tmpFile.fd());
    DiffFileHeader wdiffH = merger.header();
    writer.writeHeader(wdiffH);
    DiffRecIo recIo;
    while (merger.getAndRemove(recIo)) {
        if (volSt.stopState == ForceStopping || ga.forceQuit) {
            return false;
        }
        writer.compressAndWriteDiff(recIo.record(), recIo.io().get());
    }
    writer.flush();

    tmpFile.save(diffPath.str());
    mgr.add(mergedDiff);
    volInfo.removeDiffs(diffV);

    LOGs.info() << "merged" << diffV.size() << mergedDiff;
    return true;
}

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
    const uint64_t snapSizeLb = uint64_t(((double)(lv.sizeLb()) * 1.2));
    cybozu::lvm::Lv lvSnap = lv.takeSnapshot(tmpLvName, true, snapSizeLb);

    const MetaState baseSt = volInfo.getMetaState();
    const bool noNeedToApply =
        !baseSt.isApplying && baseSt.snapB.isClean() && baseSt.snapB.gidB == gid;

    if (!noNeedToApply) {
        std::vector<cybozu::util::File> fileV;
        tryOpenDiffs(fileV, volInfo, !allowEmpty, [&](const MetaState &st) {
                return volSt.diffMgr.getDiffListToRestore(st, gid);
            });
        if (!applyOpenedDiffs(std::move(fileV), lvSnap, volSt.stopState)) {
            return false;
        }
    }
    cybozu::lvm::renameLv(lv.vgName(), tmpLvName, targetName);
    return true;
}

/**
 * Delete a restored snapshot.
 */
inline void delRestored(const std::string &volId, uint64_t gid)
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

/**
 * Get list of all restored volumes.
 */
inline StrVec listRestored(const std::string &volId)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    const std::vector<uint64_t> gidV = volInfo.getRestoredSnapshots();
    StrVec ret;
    for (uint64_t gid : gidV) ret.push_back(cybozu::itoa(gid));
    return ret;
}

inline StrVec listRestorable(const std::string &volId, bool isAll = false, bool isVerbose = false)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    StrVec ret;
    const std::vector<MetaState> stV = volInfo.getRestorableSnapshots(isAll);
    for (const MetaState &st : stV) {
        ret.push_back(cybozu::itoa(st.snapB.gidB));
        if (isVerbose) {
            ret.back() += " ";
            ret.back() += cybozu::unixTimeToStr(st.timestamp);
        }
    }
    return ret;
}

inline void backupServer(protocol::ServerParams &p, bool isFull)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    walb::packet::Packet pkt(p.sock);

    std::string hostType, volId;
    uint64_t sizeLb, curTime, bulkLb;
    pkt.read(hostType);
    pkt.read(volId);
    pkt.read(sizeLb);
    pkt.read(curTime);
    pkt.read(bulkLb);
    logger.debug() << hostType << volId << sizeLb << curTime << bulkLb;

    ForegroundCounterTransaction foregroundTasksTran;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    StateMachine &sm = volSt.sm;
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    const std::string &stFrom = isFull ? aSyncReady : aArchived;
    MetaSnap snapFrom;
    try {
        if (hostType != storageHT && hostType != archiveHT) {
            throw cybozu::Exception(FUNC) << "invalid hostType" << hostType;
        }
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb is zero";
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);
        verifyStateIn(sm.get(), {stFrom}, FUNC);
        if (!isFull) {
            snapFrom = volSt.diffMgr.getLatestSnapshot(volInfo.getMetaState());
        }
    } catch (std::exception &e) {
        logger.warn() << e.what();
        pkt.write(e.what());
        return;
    }
    pkt.write(msgAccept);
    if (!isFull) pkt.write(snapFrom);
    cybozu::Uuid uuid;
    pkt.read(uuid);
    packet::Ack(p.sock).send();

    const std::string &stPass = isFull ? atFullSync : atHashSync;
    StateMachineTransaction tran(sm, stFrom, stPass, FUNC);
    ul.unlock();

    const std::string st = volInfo.getState();
    if (st != stFrom) {
        throw cybozu::Exception(FUNC) << "state is not" << stFrom << "but" << st;
    }
    bool isOk;
    std::unique_ptr<cybozu::TmpFile> tmpFileP;
    if (isFull) {
        volInfo.createLv(sizeLb);
        const std::string lvPath = volInfo.getLv().path().str();
        isOk = dirtyFullSyncServer(pkt, lvPath, sizeLb, bulkLb, volSt.stopState, ga.forceQuit);
    } else {
        const uint32_t hashSeed = curTime;
        tmpFileP.reset(new cybozu::TmpFile(volInfo.volDir.str()));
        diff::VirtualFullScanner virt;
        archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, snapFrom);
        isOk = dirtyHashSyncServer(pkt, virt, sizeLb, bulkLb, uuid, hashSeed, tmpFileP->fd(), volSt.stopState, ga.forceQuit);
    }
    if (!isOk) {
        logger.warn() << FUNC << "force stopped" << volId;
        return;
    }

    MetaSnap snapTo;
    pkt.read(snapTo);
    if (isFull) {
        MetaState state(snapTo, curTime);
        volInfo.setMetaState(state);
    } else {
        const MetaDiff diff(snapFrom, snapTo, true, curTime);
        tmpFileP->save(volInfo.getDiffPath(diff).str());
        tmpFileP.reset();
        volSt.diffMgr.add(diff);
    }
    volInfo.setUuid(uuid);
    volInfo.setState(aArchived);

    tran.commit(aArchived);

    packet::Ack(p.sock).send();
    p.sock.waitForClose();
    logger.info() << (isFull ? dirtyFullSyncPN : dirtyHashSyncPN)
                  << "succeeded" << volId;
}

inline cybozu::Socket runReplSync1stNegotiation(const std::string &volId, const AddrPort &addrPort)
{
    cybozu::Socket sock;
    const cybozu::SocketAddr server = addrPort.getSocketAddr();
    util::connectWithTimeout(sock, server, ga.socketTimeout);
    protocol::run1stNegotiateAsClient(sock, ga.nodeId, replSyncPN);
    protocol::sendStrVec(sock, {volId}, 1, __func__, msgAccept);
    return sock;
}

inline void verifyVolumeSize(ArchiveVolInfo &volInfo, uint64_t sizeLb, Logger &logger)
{
    const char *const FUNC = __func__;
    if (!volInfo.lvExists()) {
        logger.debug() << FUNC << "lv does not exist" << volInfo.volId;
        return;
    }
    const uint64_t selfSizeLb = volInfo.getLv().sizeLb();
    if (sizeLb > selfSizeLb) {
        throw cybozu::Exception(FUNC)
            << "volume size is smaller than the received size"
            << volInfo.volId << sizeLb << selfSizeLb;
    }
    if (sizeLb < selfSizeLb) {
        logger.warn()
            << FUNC << "volume size is larger than the received size"
            << volInfo.volId << sizeLb << selfSizeLb;
    }
}

inline bool runFullReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, uint64_t bulkLb, Logger &logger)
{
    const char *const FUNC = __func__;
    const uint64_t sizeLb = volInfo.getLv().sizeLb();
    const MetaState metaSt = volInfo.getMetaState();
    const cybozu::Uuid uuid = volInfo.getUuid();
    pkt.write(sizeLb);
    pkt.write(bulkLb);
    pkt.write(metaSt);
    pkt.write(uuid);
    logger.debug() << "full-repl-client" << sizeLb << bulkLb << metaSt << uuid;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;

    const std::string lvPath = volInfo.getLv().path().str();
    if (!dirtyFullSyncClient(pkt, lvPath, sizeLb, bulkLb, volSt.stopState, ga.forceQuit)) {
        logger.warn() << "full-repl-client force-stopped" << volId;
        return false;
    }
    logger.info() << "full-repl-client done" << volId;
    return true;
}

inline bool runFullReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, Logger &logger)
{
    const char *const FUNC = __func__;
    uint64_t sizeLb, bulkLb;
    MetaState metaSt;
    cybozu::Uuid uuid;
    try {
        pkt.read(sizeLb);
        pkt.read(bulkLb);
        pkt.read(metaSt);
        pkt.read(uuid);
        logger.debug() << "full-repl-server" << sizeLb << bulkLb << metaSt << uuid;
        if (sizeLb == 0) throw cybozu::Exception(FUNC) << "sizeLb must not be 0";
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb must not be 0";
        verifyVolumeSize(volInfo, sizeLb, logger);
        pkt.write(msgOk);
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }

    volInfo.createLv(sizeLb);
    const std::string lvPath = volInfo.getLv().path().str();
    if (!dirtyFullSyncServer(pkt, lvPath, sizeLb, bulkLb, volSt.stopState, ga.forceQuit)) {
        logger.warn() << "full-repl-server force-stopped" << volId;
        return false;
    }
    volInfo.setMetaState(metaSt);
    volInfo.setUuid(uuid);
    volInfo.setState(aArchived);
    logger.info() << "full-repl-server done" << volId;
    return true;
}

inline bool runHashReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, uint64_t bulkLb, const MetaDiff &diff, Logger &logger)
{
    const char *const FUNC = __func__;
    const uint64_t sizeLb = volInfo.getLv().sizeLb();
    const cybozu::Uuid uuid = volInfo.getUuid();
    const uint32_t hashSeed = diff.timestamp;
    pkt.write(sizeLb);
    pkt.write(bulkLb);
    pkt.write(diff);
    pkt.write(uuid);
    pkt.write(hashSeed);
    logger.debug() << "hash-repl-client" << sizeLb << bulkLb << diff << uuid << hashSeed;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;

    diff::VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, diff.snapE);
    if (!dirtyHashSyncClient(pkt, virt, sizeLb, bulkLb, hashSeed, volSt.stopState, ga.forceQuit)) {
        logger.warn() << "hash-repl-client force-stopped" << volId;
        return false;
    }
    logger.info() << "hash-repl-client done" << volId;
    return true;
}

inline bool runHashReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, Logger &logger)
{
    const char *const FUNC = __func__;
    uint64_t sizeLb, bulkLb;
    MetaDiff diff;
    cybozu::Uuid uuid;
    uint32_t hashSeed;
    try {
        pkt.read(sizeLb);
        pkt.read(bulkLb);
        pkt.read(diff);
        pkt.read(uuid);
        pkt.read(hashSeed);
        logger.debug() << "hash-repl-server" << sizeLb << bulkLb << diff << uuid << hashSeed;
        if (sizeLb == 0) throw cybozu::Exception(FUNC) << "sizeLb must not be 0";
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb must not be 0";
        verifyVolumeSize(volInfo, sizeLb, logger);
        pkt.write(msgOk);
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }

    diff::VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, diff.snapB);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    if (!dirtyHashSyncServer(pkt, virt, sizeLb, bulkLb, uuid, hashSeed, tmpFile.fd(),
                             volSt.stopState, ga.forceQuit)) {
        logger.warn() << "hash-repl-server force-stopped" << volId;
        return false;
    }
    tmpFile.save(volInfo.getDiffPath(diff).str());
    volSt.diffMgr.add(diff);
    volInfo.setUuid(uuid);
    logger.info() << "hash-repl-server done" << volId;
    return true;
}

inline bool runDiffReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, const MetaSnap &srvLatestSnap, const CompressOpt &cmpr, uint64_t wdiffMergeSize, Logger &logger)
{
    const char *const FUNC = __func__;
    MetaState st0;
    MetaDiffVec diffV;
    std::vector<cybozu::util::File> fileV;
    std::tie(st0, diffV) = tryOpenDiffs(fileV, volInfo, !allowEmpty, [&](const MetaState &) {
            return volInfo.getDiffListToSend(srvLatestSnap, wdiffMergeSize);
        });

    const MetaDiff mergedDiff = merge(diffV);
    diff::Merger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();

    const uint64_t sizeLb = volInfo.getLv().sizeLb();
    const DiffFileHeader &fileH = merger.header();
    const uint16_t maxIoBlocks = fileH.getMaxIoBlocks();
    const cybozu::Uuid uuid = fileH.getUuid();
    pkt.write(sizeLb);
    pkt.write(maxIoBlocks);
    pkt.write(uuid);
    pkt.write(mergedDiff);
    logger.debug() << "diff-repl-client" << sizeLb << maxIoBlocks << uuid << mergedDiff;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;

    if (!wdiffTransferClient(
            pkt, merger, cmpr, volSt.stopState, ga.forceQuit)) {
        logger.warn() << "diff-repl-client force-stopped" << volId;
        return false;
    }
    packet::Ack(pkt.sock()).recv();
    logger.info() << "diff-repl-client done" << volId << mergedDiff;
    return true;
}

inline bool runDiffReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, Logger &logger)
{
    const char *const FUNC = __func__;
    uint64_t sizeLb;
    uint16_t maxIoBlocks;
    cybozu::Uuid uuid;
    MetaDiff diff;
    try {
        pkt.read(sizeLb);
        pkt.read(maxIoBlocks);
        pkt.read(uuid);
        pkt.read(diff);
        logger.debug() << "diff-repl-server" << sizeLb << maxIoBlocks << uuid << diff;
        verifyVolumeSize(volInfo, sizeLb, logger);
        const MetaSnap snap = volInfo.getLatestSnapshot();
        if (!canApply(snap, diff)) {
            throw cybozu::Exception(FUNC) << "can not apply" << snap << diff;
        }
        pkt.write(msgOk);
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }

    const cybozu::FilePath fPath = volInfo.getDiffPath(diff);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    cybozu::util::File fileW(tmpFile.fd());
    writeDiffFileHeader(fileW, maxIoBlocks, uuid);
    if (!wdiffTransferServer(pkt, tmpFile.fd(), volSt.stopState, ga.forceQuit)) {
        logger.warn() << "diff-repl-server force-stopped" << volId;
        return false;
    }
    tmpFile.save(fPath.str());
    volSt.diffMgr.add(diff);
    packet::Ack(pkt.sock()).send();
    logger.info() << "diff-repl-server done" << volId << diff;
    return true;
}

inline bool runReplSyncClient(const std::string &volId, cybozu::Socket &sock, const HostInfoForRepl &hostInfo, bool isSize, uint64_t param, Logger &logger)
{
    packet::Packet pkt(sock);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    bool isFull;
    pkt.read(isFull);
    if (isFull) {
        if (!runFullReplClient(volId, volSt, volInfo, pkt, hostInfo.bulkLb, logger)) {
            return false;
        }
    }

    for (;;) {
        MetaSnap srvLatestSnap;
        pkt.read(srvLatestSnap);
        const MetaSnap cliLatestSnap = volInfo.getLatestSnapshot();
        const int repl = volInfo.shouldDoRepl(srvLatestSnap, cliLatestSnap, isSize, param);
        logger.debug() << "srvLatestSnap" << srvLatestSnap << "cliLatestSnap" << cliLatestSnap
                       << repl;
        pkt.write(repl);
        if (repl == ArchiveVolInfo::DONT_REPL) break;
        if (repl == ArchiveVolInfo::DO_HASH_REPL) {
            const MetaState metaSt = volInfo.getMetaState();
            const MetaSnap cliOldestSnap(volInfo.getOldestCleanSnapshot());
            if (srvLatestSnap.gidB >= cliOldestSnap.gidB) {
                throw cybozu::Exception(__func__)
                    << "could not execute hash-repl" << srvLatestSnap << cliOldestSnap;
            }
            const MetaDiff diff(srvLatestSnap, cliOldestSnap, true, metaSt.timestamp);
            if (!runHashReplClient(volId, volSt, volInfo, pkt, hostInfo.bulkLb, diff, logger)) {
                return false;
            }
        } else {
            if (!runDiffReplClient(
                    volId, volSt, volInfo, pkt, srvLatestSnap,
                    hostInfo.cmpr, hostInfo.maxWdiffMergeSize, logger)) return false;
        }
    }
    packet::Ack(sock).recv();
    return true;
}

inline bool runReplSyncServer(const std::string &volId, bool isFull, cybozu::Socket &sock, Logger &logger)
{
    packet::Packet pkt(sock);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);

    pkt.write(isFull);
    if (isFull) {
        if (!runFullReplServer(volId, volSt, volInfo, pkt, logger)) return false;
    }
    for (;;) {
        const MetaSnap latestSnap = volInfo.getLatestSnapshot();
        pkt.write(latestSnap);
        int repl;
        pkt.read(repl);
        if (repl == ArchiveVolInfo::DONT_REPL) break;

        if (repl == ArchiveVolInfo::DO_HASH_REPL) {
            if (!runHashReplServer(volId, volSt, volInfo, pkt, logger)) return false;
        } else {
            if (!runDiffReplServer(volId, volSt, volInfo, pkt, logger)) return false;
        }
    }
    packet::Ack(sock).send();
    sock.waitForClose();
    return true;
}

} // namespace archive_local

inline void ArchiveVolState::initInner(const std::string& volId)
{
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, diffMgr);
    if (volInfo.existsVolDir()) {
        sm.set(volInfo.getState());
        WalbDiffFiles wdiffs(diffMgr, volInfo.volDir.str());
        wdiffs.reload();
        if (volInfo.removeFilligZeroFile()) {
            LOGs.warn() << "filling-zero file has been removed." << volId;
        }
    } else {
        sm.set(aClear);
    }
}

inline void c2aGetStateServer(protocol::ServerParams &p)
{
    protocol::c2xGetStateServer(p, getArchiveVolState, ga.nodeId, __func__);
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
        packet::Ack(p.sock).send();
        p.sock.waitForClose();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aListDiffServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
        const std::string &volId = v[0];

        ArchiveVolState &volSt = getArchiveVolState(volId);
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        UniqueLock ul(volSt.mu);

        const std::vector<std::pair<MetaDiff, uint64_t>> diffSizeV = volInfo.getDiffListWithSize();
        StrVec diffListStrV;
        diffListStrV.emplace_back("#snapB-->snapE isMergeable timestamp sizeB");
        for (const std::pair<MetaDiff, uint64_t> &p : diffSizeV) {
            const MetaDiff &diff = p.first;
            const uint64_t sizeB = p.second;
            diffListStrV.push_back(
                cybozu::util::formatString(
                    "%s %d %s %" PRIu64 ""
                    , diff.str().c_str()
                    , diff.isMergeable ? 1 : 0
                    , cybozu::unixTimeToStr(diff.timestamp).c_str()
                    , sizeB));
        }
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(diffListStrV);
        packet::Ack(p.sock).send();
        p.sock.waitForClose();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aListVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = util::getDirNameList(ga.baseDirStr);
    protocol::sendStrVec(p.sock, v, 0, FUNC);
    packet::Ack(p.sock).send();
    p.sock.waitForClose();
    ProtocolLogger logger(ga.nodeId, p.clientId);
    logger.debug() << "listVol succeeded";
}

inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
        const std::string &volId = v[0];

        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);

        StateMachineTransaction tran(volSt.sm, aClear, atInitVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        volInfo.init();
        tran.commit(aSyncReady);
        pkt.write(msgOk);
        p.sock.waitForClose();
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
    const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
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
        p.sock.waitForClose();
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
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
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
        p.sock.waitForClose();
        logger.info() << "start succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * command "stop"
 * params[0]: volId
 * params[1]: StopOpt as string (optional)
 */
inline void c2aStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        std::string volId;
        StopOpt stopOpt;
        std::tie(volId, stopOpt) = parseStopParams(v, FUNC);

        ArchiveVolState &volSt = getArchiveVolState(volId);
        Stopper stopper(volSt.stopState);
        if (!stopper.changeFromNotStopping(stopOpt.isForce() ? ForceStopping : Stopping)) {
            throw cybozu::Exception(FUNC) << "already under stopping" << volId;
        }
        pkt.write(msgAccept);
        sendErr = false;
        p.sock.waitForClose();
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
        logger.info() << "stop succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * Execute dirty full sync protocol as server.
 * Client is storage server or another archive server.
 */
inline void x2aDirtyFullSyncServer(protocol::ServerParams &p)
{
    const bool isFull = true;
    archive_local::backupServer(p, isFull);
}

/**
 * Execute dirty hash sync protocol as server.
 * Client is storage server or another archive server.
 */
inline void x2aDirtyHashSyncServer(protocol::ServerParams &p)
{
    const bool isFull = false;
    archive_local::backupServer(p, isFull);
}

/**
 * Restore command.
 */
inline void c2aRestoreServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
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
    p.sock.waitForClose();

    ActionCounterTransaction tran(volSt.ac, aRestore);
    ul.unlock();
    if (!archive_local::restore(volId, gid)) {
        logger.warn() << FUNC << "force stopped" << volId << gid;
        return;
    }
    logger.info() << "restore succeeded" << volId << gid;
}

/**
 * del-restored command.
 */
inline void c2aDelRestoredServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const uint64_t gid = cybozu::atoi(v[1]);

        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived, atHashSync, atWdiffRecv}, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aRestore}, FUNC);
        ul.unlock();

        archive_local::delRestored(volId, gid);
        logger.info() << "del-restored succeeded" << volId << gid;
        pkt.write(msgOk);
        sendErr = false;
        p.sock.waitForClose();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aListRestoredServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
        const std::string &volId = v[0];
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived, atHashSync, atWdiffRecv}, FUNC);
//        verifyNoActionRunning(volSt.ac, StrVec{aRestore}, FUNC);
        const StrVec strV = archive_local::listRestored(volId);
        ul.unlock();
        logger.info() << "list-restored succeeded" << volId;
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(strV);
        packet::Ack(p.sock).send();
        p.sock.waitForClose();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aListRestorableServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        if (v.empty()) throw cybozu::Exception(FUNC) << "no vol";
        const std::string& volId = v[0];
        bool isAll = false;
        bool isVerbose = false;
        for (size_t i = 1; i < v.size(); i++) {
            if (v[i] == "vervose") {
                isVerbose = true;
            } else if (v[i] == "all") {
                isAll = true;
            } else {
                throw cybozu::Exception(FUNC) << "bad opt" << v[i];
            }
        }

        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        const std::string st = volSt.sm.get();
        verifyNoActionRunning(volSt.ac, StrVec{aRestore}, FUNC);
        StrVec strV;
        if (isStateIn(st, {aArchived, atHashSync, atWdiffRecv})) {
            strV = archive_local::listRestorable(volId, isAll, isVerbose);
        }
        ul.unlock();
        logger.info() << "list-restored succeeded" << volId;
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(strV);
        packet::Ack(p.sock).send();
        p.sock.waitForClose();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
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
    const StrVec v =
        protocol::recvStrVec(p.sock, 1, FUNC);
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
        p.sock.waitForClose();
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

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
        verifyStateIn(sm.get(), {aArchived, aStopped, atWdiffRecv}, FUNC);
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
        p.sock.waitForClose();
        return;
    }
    {
        const std::string st = sm.get();
		const char *msg = nullptr;
		if (st == aStopped) {
			msg = "stopped";
		} else if (st == atWdiffRecv) {
			msg = "wdiff-recv";
		}
		if (msg) {
            logger.info() << msg << volId;
            pkt.write(msg);
            p.sock.waitForClose();
            return;
        }
    }
    if (hostType == proxyHT && volInfo.getUuid() != uuid) {
        const char *msg = "different-uuid";
        logger.info() << msg << volId;
        pkt.write(msg);
        p.sock.waitForClose();
        return;
    }
    const uint64_t selfSizeLb = volInfo.getLv().sizeLb();
    if (selfSizeLb < sizeLb) {
        const char *msg = "smaller-lv-size";
        logger.error() << msg << volId << sizeLb << selfSizeLb;
        pkt.write(msg);
        p.sock.waitForClose();
        return;
    }

    if (sizeLb < selfSizeLb) {
        logger.warn() << "larger lv size" << volId << sizeLb << selfSizeLb;
    }
    const MetaState metaState = volInfo.getMetaState();
    const MetaSnap latestSnap = volSt.diffMgr.getLatestSnapshot(metaState);
    const Relation rel = getRelation(latestSnap, diff);

    if (rel != Relation::APPLICABLE_DIFF) {
        const char *msg = getRelationStr(rel);
        logger.info() << msg << volId;
        pkt.write(msg);
        p.sock.waitForClose();
        return;
    }
    pkt.write(msgAccept);

    StateMachineTransaction tran(sm, aArchived, atWdiffRecv, FUNC);
    ul.unlock();

    const cybozu::FilePath fPath = volInfo.getDiffPath(diff);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    cybozu::util::File fileW(tmpFile.fd());
    writeDiffFileHeader(fileW, maxIoBlocks, uuid);
    if (!wdiffTransferServer(pkt, tmpFile.fd(), volSt.stopState, ga.forceQuit)) {
        logger.warn() << FUNC << "force stopped" << volId;
        return;
    }
    tmpFile.save(fPath.str());

    ul.lock();
    volSt.diffMgr.add(diff);
    tran.commit(aArchived);

    packet::Ack(p.sock).send();
    p.sock.waitForClose();
    logger.info() << "wdiff-transfer succeeded" << volId;
}

/**
 * This function will Work as a repl-sync client.
 */
inline void c2aReplicateServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        if (v.size() < 3) throw cybozu::Exception(FUNC) << "volId type param2 are required";
        const std::string &volId = v[0];
        bool isSize;
        if (v[1] == "size") {
            isSize = true;
        } else if (v[1] == "gid") {
            isSize = false;
        } else {
            throw cybozu::Exception(FUNC) << "bad type" << v[1];
        }
        const uint64_t param2 = cybozu::atoi(v[2]);
        const HostInfoForRepl hostInfo = parseHostInfoForRepl(v, 3);

        ForegroundCounterTransaction foregroundTasksTran;
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aReplSync}, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived, atHashSync, atWdiffRecv}, FUNC);

        ActionCounterTransaction tran(volSt.ac, aReplSync);
        ul.unlock();
        cybozu::Socket aSock = archive_local::runReplSync1stNegotiation(volId, hostInfo.addrPort);
        pkt.write(msgAccept);
        sendErr = false;
        p.sock.waitForClose();
        if (!archive_local::runReplSyncClient(volId, aSock, hostInfo, isSize, param2, logger)) {
            logger.warn() << FUNC << "replication as client force stopped" << volId << hostInfo;
            return;
        }
        logger.info() << "replication as client succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void a2aReplSyncServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
        const std::string &volId = v[0];

        ForegroundCounterTransaction foregroundTasksTran;
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoArchiveActionRunning(volSt.ac, FUNC);
        std::string stFrom = volSt.sm.get();
        verifyStateIn(stFrom, {aSyncReady, aArchived}, FUNC);

        pkt.write(msgAccept);
        sendErr = false;

        StateMachineTransaction tran(volSt.sm, stFrom, atReplSync, FUNC);
        ul.unlock();
        const bool isFull = stFrom == aSyncReady;
        if (!archive_local::runReplSyncServer(volId, isFull, p.sock, logger)) {
            logger.warn() << FUNC << "replication as server force stopped" << volId;
            return;
        }
        tran.commit(aArchived);
        logger.info() << "replication as server succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aApplyServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const uint64_t gid = cybozu::atoi(v[1]);

        ForegroundCounterTransaction foregroundTasksTran;
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aApply, aRestore, aReplSync}, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived}, FUNC);
        archive_local::verifyApplicable(volId, gid);

        pkt.write(msgAccept);
        sendErr = false;
        p.sock.waitForClose();

        ActionCounterTransaction tran(volSt.ac, aApply);
        ul.unlock();
        if (!archive_local::applyDiffsToVolume(volId, gid)) {
            logger.warn() << FUNC << "stopped force" << volId << gid;
            return;
        }
        logger.info() << "apply succeeded" << volId << gid;
    } catch (std::exception& e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2aMergeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 4, FUNC);
        const std::string& volId = v[0];
        const uint64_t gidB = cybozu::atoi(v[1]);
        const std::string& type = v[2];
        const uint64_t param3 = cybozu::atoi(v[3]); // gidE or maxSizeMb
        bool isSize;
        if (type == "size") {
            isSize = true;
        } else if (type == "gid") {
            isSize = false;
            if (param3 <= gidB) throw cybozu::Exception(FUNC) << "bad gid range" << gidB << param3;
        } else {
            throw cybozu::Exception(FUNC) << "bad type" << type;
        }

        ForegroundCounterTransaction foregroundTasksTran;
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aMerge, aReplSync}, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived, atWdiffRecv}, FUNC);
        archive_local::verifyMergeable(volId, gidB);

        pkt.write(msgAccept);
        sendErr = false;
        p.sock.waitForClose();

        ActionCounterTransaction tran(volSt.ac, aMerge);
        ul.unlock();
        if (!archive_local::mergeDiffs(volId, gidB, isSize, param3)) {
            logger.warn() << FUNC << "stopped force" << volId << gidB << type << param3;
            return;
        }
        logger.info() << "merge succeeded" << volId << gidB << type << param3;
    } catch (std::exception& e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * params[0]: volId
 * params[1]: size [byte] suffix k/m/g/t can be used.
 * params[2]: doZeroClear. 'zeroclear'. optional.
 */
inline void c2aResizeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        std::string volId, newSizeLbStr, doZeroClearStr;
        cybozu::util::parseStrVec(v, 0, 2, {&volId, &newSizeLbStr, &doZeroClearStr});
        const uint64_t newSizeLb = util::parseSizeLb(newSizeLbStr, FUNC);
        bool doZeroClear;
        if (doZeroClearStr.empty()) {
            doZeroClear = false;
        } else if (doZeroClearStr == "zeroclear") {
            doZeroClear = true;
        } else {
            throw cybozu::Exception(FUNC) << "bad param" << doZeroClearStr;
        }

        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{aApply, aRestore, aReplSync, aResize}, FUNC);
        verifyStateIn(volSt.sm.get(), {aArchived, atWdiffRecv, atHashSync, aStopped}, FUNC);

        ActionCounterTransaction tran(volSt.ac, aResize);
        ul.unlock();

        if (doZeroClear) {
            pkt.writeFin(msgOk);
            // this is asynchronous.
            volInfo.growLv(newSizeLb, true);
        } else {
            volInfo.growLv(newSizeLb, false);
            pkt.writeFin(msgOk);
        }
        logger.info() << "resize succeeded" << volId << newSizeLb << doZeroClear;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
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

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
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
        sendErr = false;
        p.sock.waitForClose();
        logger.info() << "reset succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * return whether vol is under filling zero or not.
 *
 * params[0]: volId
 */
inline void c2aIsFillingZeroServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
        const std::string &volId = v[0];
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        if (volSt.sm.get() == aClear) {
            throw cybozu::Exception(FUNC) << "bad state";
        }
        const ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, volSt.diffMgr);
        const bool isFillingZero = volInfo.isFillingZero();
        ul.unlock();
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(isFillingZero);
        packet::Ack(p.sock).sendFin();
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

} // namespace walb
