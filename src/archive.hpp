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
 * Manage one instance for each volume.
 */
struct ArchiveVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac;

    MetaDiffManager diffMgr;

    /**
     * Timestamp of the latest sync (full, hash).
     * Lock of mu is required to access these variables.
     * 0 means none did not do sync after the daemon started.
     */
    uint64_t lastSyncTime;
    /**
     * Timestamp of the latest wdiff received from a proxy server.
     * Lock of mu is required to access these variables.
     * 0 means no diff was received after the daemon started.
     */
    uint64_t lastWdiffReceivedTime;

    explicit ArchiveVolState(const std::string& volId)
        : stopState(NotStopping)
        , sm(mu)
        , ac(mu)
        , diffMgr()
        , lastSyncTime(0)
        , lastWdiffReceivedTime(0) {
        sm.init(statePairTbl);
        initInner(volId);
    }
    void updateLastSyncTime() {
        UniqueLock ul(mu);
        lastSyncTime = ::time(0);
    }
    void updateLastWdiffReceivedTime() {
        UniqueLock ul(mu);
        lastWdiffReceivedTime = ::time(0);
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
    std::string thinpool;
    size_t maxForegroundTasks;
    size_t socketTimeout;

    /**
     * Writable and must be thread-safe.
     */
    ProcessStatus ps;
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

inline ArchiveVolInfo getArchiveVolInfo(const std::string &volId)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    return ArchiveVolInfo(ga.baseDirStr, volId, ga.volumeGroup, ga.thinpool, volSt.diffMgr);
}

inline bool isThinpool()
{
    return !ga.thinpool.empty();
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
    VirtualFullScanner &virt,
    ArchiveVolInfo &volInfo, uint64_t sizeLb, const MetaSnap &snap)
{
    const char *const FUNC = __func__;

    cybozu::lvm::Lv lv = volInfo.getLv();
    if (sizeLb != lv.sizeLb()) {
        throw cybozu::Exception(FUNC) << "bad sizeLb" << sizeLb << lv.sizeLb();
    }
    std::vector<cybozu::util::File> fileV;
    MetaState st0;
    MetaDiffVec diffV;
    std::tie(st0, diffV) =
        tryOpenDiffs(fileV, volInfo, allowEmpty, [&](const MetaState &st) {
                return volInfo.getDiffMgr().getDiffListToSync(st, snap);
            });
    LOGs.debug() << "virtual-full-scan-diffs" << st0 << diffV;
    cybozu::util::File fileR(lv.path().str(), O_RDONLY);
    virt.init(std::move(fileR), std::move(fileV));
}

inline void verifyApplicable(const std::string& volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    UniqueLock ul(volSt.mu);

    const MetaState st = volInfo.getMetaState();
    if (volSt.diffMgr.getDiffListToApply(st, gid).empty()) {
        throw cybozu::Exception(__func__) << "There is no diff to apply" << volId;
    }
}

inline bool applyOpenedDiffs(std::vector<cybozu::util::File>&& fileV, cybozu::lvm::Lv& lv,
                             const std::atomic<int>& stopState,
                             DiffStatistics& statIn, DiffStatistics& statOut, std::string& memUsageStr)
{
    const char *const FUNC = __func__;
    statOut.clear();
    DiffMerger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();
    DiffRecIo recIo;
    cybozu::util::File file(lv.path().str(), O_RDWR);
    std::vector<char> zero;
	const uint64_t lvSnapSizeLb = lv.sizeLb();
    while (merger.getAndRemove(recIo)) {
        if (stopState == ForceStopping || ga.ps.isForceShutdown()) {
            return false;
        }
        const DiffRecord& rec = recIo.record();
        statOut.update(rec);
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
        file.pwrite(data, ioSizeB, ioAddrB);
    }
    file.fdatasync();
    file.close();
    statIn = merger.statIn();
    statOut.wdiffNr = -1;
    statOut.dataSize = -1;
    memUsageStr = merger.memUsageStr();
    return true;
}

inline bool applyDiffsToVolume(const std::string& volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    MetaDiffManager &mgr = volSt.diffMgr;
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    std::vector<cybozu::util::File> fileV;
    MetaState st0;
    MetaDiffVec diffV;
    std::tie(st0, diffV) = tryOpenDiffs(
        fileV, volInfo, !allowEmpty, [&](const MetaState &st) {
            return mgr.getDiffListToApply(st, gid);
        });

    LOGs.debug() << "apply-diffs" << st0 << diffV;
    const MetaState st1 = beginApplying(st0, diffV);
    volInfo.setMetaState(st1);

    cybozu::lvm::Lv lv = volInfo.getLv();
    DiffStatistics statIn, statOut;
    std::string memUsageStr;
    if (!applyOpenedDiffs(std::move(fileV), lv, volSt.stopState, statIn, statOut, memUsageStr)) {
        return false;
    }
    LOGs.info() << "apply-mergeIn " << volId << statIn;
    LOGs.info() << "apply-mergeOut" << volId << statOut;
    LOGs.info() << "apply-mergeMemUsage" << volId << memUsageStr;

    const MetaState st2 = endApplying(st1, diffV);
    volInfo.setMetaState(st2);

    volInfo.removeBeforeGid(st2.snapB.gidB);
    return true;
}

inline void verifyNotApplying(const std::string &volId)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    if (volInfo.getMetaState().isApplying) {
        throw cybozu::Exception(__func__)
            << "merge is not permitted because the volume is under applying"
            << volId;
    }
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
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

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

    MetaDiff mergedDiff = merge(diffV);
    LOGs.debug() << "merge-diffs" << mergedDiff << diffV;
    const cybozu::FilePath diffPath = volInfo.getDiffPath(mergedDiff);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    DiffMerger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();

    DiffWriter writer(tmpFile.fd());
    DiffFileHeader wdiffH = merger.header();
    writer.writeHeader(wdiffH);
    DiffRecIo recIo;
    while (merger.getAndRemove(recIo)) {
        if (volSt.stopState == ForceStopping || ga.ps.isForceShutdown()) {
            return false;
        }
        // TODO: currently we can use snappy only.
        writer.compressAndWriteDiff(recIo.record(), recIo.io().get());
    }
    writer.flush();

    mergedDiff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(diffPath.str());
    mgr.add(mergedDiff);
    volInfo.removeDiffs(diffV);

    LOGs.info() << "merge-mergeIn " << volId << merger.statIn();
    LOGs.info() << "merge-mergeOut" << volId << writer.getStat();
    LOGs.info() << "merge-mergeMemUsage" << volId << merger.memUsageStr();
    LOGs.info() << "merged" << diffV.size() << mergedDiff;
    return true;
}

inline void removeSnapshot(cybozu::lvm::Lv& lv, const std::string& name)
{
    if (lv.hasSnapshot(name)) {
        lv.getSnapshot(name).remove();
    }
}

struct TmpSnapshotDeleter
{
    cybozu::lvm::Lv& lv;
    std::string name;
    ~TmpSnapshotDeleter()
        try
    {
        removeSnapshot(lv, name);
    } catch (...) {
    }
};

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
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    cybozu::lvm::Lv lv = volInfo.getLv();
    const std::string targetName = volInfo.restoredSnapshotName(gid);
    const std::string tmpLvName = targetName + RESTORE_TMP_SUFFIX;
    removeSnapshot(lv, tmpLvName);

    cybozu::lvm::Lv lvSnap;
    if (isThinpool()) {
        lv = lv.createSnapshot(tmpLvName, true);
    } else {
        const uint64_t snapSizeLb = uint64_t((double)(lv.sizeLb()) * 1.2);
        lv = lv.createSnapshot(tmpLvName, true, snapSizeLb);
    }
    TmpSnapshotDeleter deleter{lv, tmpLvName};

    const MetaState baseSt = volInfo.getMetaState();
    const bool noNeedToApply =
        !baseSt.isApplying && baseSt.snapB.isClean() && baseSt.snapB.gidB == gid;

    if (!noNeedToApply) {
        std::vector<cybozu::util::File> fileV;
        MetaState st0;
        MetaDiffVec diffV;
        std::tie(st0, diffV) =
            tryOpenDiffs(fileV, volInfo, !allowEmpty, [&](const MetaState &st) {
                    return volSt.diffMgr.getDiffListToRestore(st, gid);
                });
        LOGs.debug() << "restore-diffs" << st0 << diffV;
        DiffStatistics statIn, statOut;
        std::string memUsageStr;
        if (!applyOpenedDiffs(std::move(fileV), lvSnap, volSt.stopState, statIn, statOut, memUsageStr)) {
            return false;
        }
        LOGs.info() << "restore-mergeIn " << volId << statIn;
        LOGs.info() << "restore-mergeOut" << volId << statOut;
        LOGs.info() << "restore-mergeMemUsage" << volId << memUsageStr;
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

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
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
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    const std::vector<uint64_t> gidV = volInfo.getRestoredSnapshots();
    StrVec ret;
    for (uint64_t gid : gidV) ret.push_back(cybozu::itoa(gid));
    return ret;
}

inline StrVec listRestorable(const std::string &volId, bool isAll = false)
{
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    StrVec ret;
    const std::vector<MetaState> stV = volInfo.getRestorableSnapshots(isAll);
    for (const MetaState &st : stV) {
        ret.push_back(cybozu::itoa(st.snapB.gidB) + ' ' + util::timeToPrintable(st.timestamp));
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
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    const std::string &stFrom = isFull ? aSyncReady : aArchived;
    MetaSnap snapFrom;
    try {
        if (hostType != storageHT && hostType != archiveHT) {
            throw cybozu::Exception(FUNC) << "invalid hostType" << hostType;
        }
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb is zero";
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);
        verifyStateIn(sm.get(), {stFrom}, FUNC);
        if (!isFull) snapFrom = volInfo.getLatestSnapshot();
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
        const bool skipZero = isThinpool();
        isOk = dirtyFullSyncServer(pkt, lvPath, sizeLb, bulkLb, volSt.stopState, ga.ps, skipZero);
    } else {
        const uint32_t hashSeed = curTime;
        tmpFileP.reset(new cybozu::TmpFile(volInfo.volDir.str()));
        VirtualFullScanner virt;
        archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, snapFrom);
        isOk = dirtyHashSyncServer(pkt, virt, sizeLb, bulkLb, uuid, hashSeed, tmpFileP->fd(), volSt.stopState, ga.ps);
        if (isOk) {
            logger.info() << "hash-backup-mergeIn " << volId << virt.statIn();
            logger.info() << "hash-backup-mergeOut" << volId << virt.statOut();
            logger.info() << "hash-backup-mergeMemUsage" << volId << virt.memUsageStr();
        }
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
        /*
            if snapFrom is clean, then the snapshot must be restorable,
            then the diff must not be mergeable.
        */
        MetaDiff diff(snapFrom, snapTo, !snapFrom.isClean(), curTime);
        diff.dataSize = cybozu::FileStat(tmpFileP->fd()).size();
        tmpFileP->save(volInfo.getDiffPath(diff).str());
        tmpFileP.reset();
        volSt.diffMgr.add(diff);
    }
    volInfo.setUuid(uuid);
    volInfo.setState(aArchived);
    volSt.updateLastSyncTime();
    tran.commit(aArchived);

    packet::Ack(p.sock).sendFin();
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
    if (!dirtyFullSyncClient(pkt, lvPath, sizeLb, bulkLb, volSt.stopState, ga.ps)) {
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
    const bool skipZero = isThinpool();
    if (!dirtyFullSyncServer(pkt, lvPath, sizeLb, bulkLb, volSt.stopState, ga.ps, skipZero)) {
        logger.warn() << "full-repl-server force-stopped" << volId;
        return false;
    }
    volInfo.setMetaState(metaSt);
    volInfo.setUuid(uuid);
    volInfo.setState(aArchived);
    volSt.updateLastSyncTime();
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

    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, diff.snapE);
    if (!dirtyHashSyncClient(pkt, virt, sizeLb, bulkLb, hashSeed, volSt.stopState, ga.ps)) {
        logger.warn() << "hash-repl-client force-stopped" << volId;
        return false;
    }
    logger.info() << "hash-repl-client-mergeIn " << volId << virt.statIn();
    logger.info() << "hash-repl-client-mergeOut" << volId << virt.statOut();
    logger.info() << "hash-repl-client-mergeMemUsage" << volId << virt.memUsageStr();
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

    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, diff.snapB);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    if (!dirtyHashSyncServer(pkt, virt, sizeLb, bulkLb, uuid, hashSeed, tmpFile.fd(),
                             volSt.stopState, ga.ps)) {
        logger.warn() << "hash-repl-server force-stopped" << volId;
        return false;
    }
    diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(volInfo.getDiffPath(diff).str());
    volSt.diffMgr.add(diff);
    volInfo.setUuid(uuid);
    volSt.updateLastSyncTime();
    logger.info() << "hash-repl-server-mergeIn " << volId << virt.statIn();
    logger.info() << "hash-repl-server-mergeOut" << volId << virt.statOut();
    logger.info() << "hash-repl-server-mergeMemUsage" << volId << virt.memUsageStr();
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
    LOGs.debug() << "diff-repl-diffs" << st0 << mergedDiff << diffV;
    DiffMerger merger;
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

    DiffStatistics statOut;
    if (!wdiffTransferClient(pkt, merger, cmpr, volSt.stopState, ga.ps, statOut)) {
        logger.warn() << "diff-repl-client force-stopped" << volId;
        return false;
    }
    packet::Ack(pkt.sock()).recv();
    logger.info() << "diff-repl-mergeIn " << volId << merger.statIn();
    logger.info() << "diff-repl-mergeOut" << volId << statOut;
    logger.info() << "diff-repl-mergeMemUsage" << volId << merger.memUsageStr();
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
    if (!wdiffTransferServer(pkt, tmpFile.fd(), volSt.stopState, ga.ps)) {
        logger.warn() << "diff-repl-server force-stopped" << volId;
        return false;
    }
    diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(fPath.str());
    volSt.diffMgr.add(diff);
    packet::Ack(pkt.sock()).send();
    volSt.updateLastWdiffReceivedTime();
    logger.info() << "diff-repl-server done" << volId << diff;
    return true;
}

inline bool runReplSyncClient(const std::string &volId, cybozu::Socket &sock, const HostInfoForRepl &hostInfo, bool isSize, uint64_t param, Logger &logger)
{
    packet::Packet pkt(sock);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

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
            MetaDiff diff(srvLatestSnap, cliOldestSnap, true, metaSt.timestamp);
            diff.isCompDiff = true;
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
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

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
    packet::Ack(sock).sendFin();
    return true;
}

inline StrVec getAllStatusAsStrVec()
{
    auto fmt = cybozu::util::formatString;
    StrVec v;

    v.push_back("-----ArchiveGlobal-----");
    v.push_back(fmt("nodeId %s", ga.nodeId.c_str()));
    v.push_back(fmt("baseDir %s", ga.baseDirStr.c_str()));
    v.push_back(fmt("volumeGroup %s", ga.volumeGroup.c_str()));
    v.push_back(fmt("maxForegroundTasks %zu", ga.maxForegroundTasks));
    v.push_back(fmt("socketTimeout %zu", ga.socketTimeout));

    v.push_back("-----Volume-----");
    for (const std::string &volId : ga.stMap.getKeyList()) {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        std::string s;
        const std::string state = volSt.sm.get();
        if (state == aClear) continue;

        s += fmt("volume %s", volId.c_str());
        s += fmt(" state %s", state.c_str());
        const int totalNumAction = getTotalNumActions(volSt.ac, allActionVec);
        s += fmt(" totalNumAction %d", totalNumAction);
        s += fmt(" stopState %s", stopStateToStr(StopState(volSt.stopState.load())));
        s += fmt(" lastSyncTime %s"
                 , util::timeToPrintable(volSt.lastSyncTime).c_str());
        s += fmt(" lastWdiffReceivedTime %s"
                 , util::timeToPrintable(volSt.lastWdiffReceivedTime).c_str());
        s += fmt(" numDiff %zu", volSt.diffMgr.size());

        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        if (!volInfo.lvExists()) {
            s += fmt(" baseLv NOT FOUND");
            v.push_back(s);
            continue;
        }
        const MetaSnap latestSnap = volInfo.getLatestSnapshot();
        s += fmt(" latestSnapshot %s", latestSnap.str().c_str());
        const uint64_t sizeLb = volInfo.getLv().sizeLb();
        const std::string sizeS = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
        s += fmt(" size %s", sizeS.c_str());

        v.push_back(s);
    }
    return v;
}

inline StrVec getVolStatusAsStrVec(const std::string &volId)
{
    auto fmt = cybozu::util::formatString;
    StrVec v;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    const std::string &state = volSt.sm.get();
    v.push_back(fmt("volume %s", volId.c_str()));
    v.push_back(fmt("state %s", state.c_str()));
    if (state == aClear) return v;
    v.push_back(formatActions("action", volSt.ac, allActionVec));
    v.push_back(fmt("stopState %s", stopStateToStr(StopState(volSt.stopState.load()))));
    v.push_back(fmt("lastSyncTime %s"
                    , util::timeToPrintable(volSt.lastSyncTime).c_str()));
    v.push_back(fmt("lastWdiffReceivedTime %s"
                    , util::timeToPrintable(volSt.lastWdiffReceivedTime).c_str()));

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    if (!volInfo.lvExists()) {
        v.push_back("baseLv NOT FOUND");
        return v;
    }
	for (std::string& s : volInfo.getStatusAsStrVec()) {
		v.push_back(std::move(s));
	}
    return v;
}

inline void getState(protocol::GetCommandParams &p)
{
    protocol::runGetStateServer(p, getArchiveVolState);
}

inline void getHostType(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, archiveHT);
}

inline void getVolList(protocol::GetCommandParams &p)
{
    StrVec v = util::getDirNameList(ga.baseDirStr);
    protocol::sendValueAndFin(p, v);
}

inline void getPid(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, static_cast<size_t>(::getpid()));
}

inline void parseVolIdAndGidRange(const StrVec &v, size_t pos,
                                  std::string &volId, uint64_t &gid0, uint64_t &gid1)
{
    std::string gid0S, gid1S;
    cybozu::util::parseStrVec(v, pos, 1, {&volId, &gid0S, &gid1S});
    gid0 = 0;
    gid1 = -1;
    if (!gid0S.empty()) gid0 = cybozu::atoi(gid0S);
    if (!gid1S.empty()) gid1 = cybozu::atoi(gid1S);
    if (gid0 >= gid1) {
        throw cybozu::Exception(__func__) << "bad gid range" << gid0 << gid1;
    }
}

inline void getDiffList(protocol::GetCommandParams &p)
{
    std::string volId;
    uint64_t gid0, gid1;
    parseVolIdAndGidRange(p.params, 1, volId, gid0, gid1);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    const MetaDiffVec diffV = volSt.diffMgr.getAll(gid0, gid1);
    StrVec v;
    for (const MetaDiff &diff : diffV) {
        v.push_back(formatMetaDiff("", diff));
    }
    ul.unlock();
    protocol::sendValueAndFin(p, v);
}

inline void getTotalDiffSize(protocol::GetCommandParams &p)
{
    std::string volId;
    uint64_t gid0, gid1;
    parseVolIdAndGidRange(p.params, 1, volId, gid0, gid1);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    size_t totalSize = 0;
    const MetaDiffVec diffV = volSt.diffMgr.getAll(gid0, gid1);
    for (const MetaDiff &d : diffV) {
        totalSize += d.dataSize;
    }
    ul.unlock();
    protocol::sendValueAndFin(p, totalSize);
}

inline void existsDiff(protocol::GetCommandParams &p)
{
    std::string volId, gidS[4];
    cybozu::util::parseStrVec(p.params, 1, 5, {&volId, &gidS[0], &gidS[1], &gidS[2], &gidS[3]});
    uint64_t gid[4];
    for (size_t i = 0; i < 4; i++) {
        gid[i] = cybozu::atoi(gidS[i]);
    }

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyStateIn(volSt.sm.get(), aActiveOrStopped, __func__);

    MetaDiff d(MetaSnap(gid[0], gid[1]), MetaSnap(gid[2], gid[3]));
    const bool s = volSt.diffMgr.exists(d);
    ul.unlock();
    protocol::sendValueAndFin(p, s);
}

/**
 * return whether an action is running on a volume or not.
 *
 * params[1]: volId
 * params[2]: action name.
 */
inline void getNumAction(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    std::string volId, action;
    cybozu::util::parseStrVec(p.params, 1, 2, {&volId, &action});

    if (std::find(allActionVec.begin(), allActionVec.end(), action) == allActionVec.end()) {
        throw cybozu::Exception(FUNC) << "no such action" << action;
    }
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (st == aClear) {
        throw cybozu::Exception(FUNC) << "bad state" << volId << action << st;
    }
    const size_t num = volSt.ac.getValue(action);
    ul.unlock();
    protocol::sendValueAndFin(p, num);
    p.logger.debug() << "get num-action succeeded" << volId;
}

inline void getRestored(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    std::string volId;
    cybozu::util::parseStrVec(p.params, 1, 1, {&volId});

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyStateIn(volSt.sm.get(), aActiveOrStopped, FUNC);
    const StrVec strV = archive_local::listRestored(volId);
    ul.unlock();
    protocol::sendValueAndFin(p, strV);
    p.logger.debug() << "get restored succeeded" << volId;
}

inline void getRestorable(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    std::string volId, opt[2];
    cybozu::util::parseStrVec(p.params, 1, 1, {&volId, &opt[0], &opt[1]});
    bool isAll = false;
    for (const std::string &o : opt) {
        if (o.empty()) break;
        if (o == "all") {
            isAll = true;
        } else {
            throw cybozu::Exception(FUNC) << "bad opt" << o;
        }
    }

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    const std::string st = volSt.sm.get();
    StrVec strV;
    if (isStateIn(st, aActive)) {
        strV = archive_local::listRestorable(volId, isAll);
    }
    ul.unlock();
    protocol::sendValueAndFin(p, strV);
    p.logger.debug() << "get restorable succeeded" << volId;
}

inline void getUuid(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    std::string volId;
    cybozu::util::parseStrVec(p.params, 1, 1, {&volId});
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (!isStateIn(st, aActive)) {
        throw cybozu::Exception(FUNC) << "bad state" << volId << st;
    }
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const cybozu::Uuid uuid = volInfo.getUuid();
    ul.unlock();
    const std::string uuidStr = uuid.str();
    protocol::sendValueAndFin(p, uuidStr);
    p.logger.debug() << "get uuid succeeded" << volId << uuidStr;
}

inline void getBase(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    std::string volId;
    cybozu::util::parseStrVec(p.params, 1, 1, {&volId});
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (!isStateIn(st, aActive)) {
        throw cybozu::Exception(FUNC) << "bad state" << volId << st;
    }
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const MetaState metaSt = volInfo.getMetaState();
    ul.unlock();
    const std::string metaStStr = metaSt.strTs();
    protocol::sendValueAndFin(p, metaStStr);
    p.logger.debug() << "get base succeeded" << volId << metaStStr;
}

/**
 * For test use only.
 */
inline bool getBlockHash(
    const std::string &volId, uint64_t gid, uint64_t bulkLb,
    packet::Packet &pkt, Logger &, cybozu::murmurhash3::Hash &hash)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const uint64_t sizeLb = volInfo.getLv().sizeLb();

    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volInfo, sizeLb, MetaSnap(gid));

    std::vector<char> buf;
    buf.reserve(bulkLb * LOGICAL_BLOCK_SIZE);
    packet::StreamControl ctrl(pkt.sock());
    cybozu::murmurhash3::Hasher hasher(0); // seed is 0.
    hash.clear();
    uint64_t remaining = sizeLb;
    double t0 = cybozu::util::getTime();
    while (remaining > 0) {
        if (volSt.stopState == ForceStopping || ga.ps.isForceShutdown()) {
            ctrl.end();
            return false;
        }
        const uint64_t lb = std::min(remaining, bulkLb);
        buf.resize(lb * LOGICAL_BLOCK_SIZE);
        virt.read(buf.data(), buf.size());
        const cybozu::murmurhash3::Hash h = hasher(buf.data(), buf.size());
        hash.doXor(h);
        const double t1 = cybozu::util::getTime();
        if (t1 - t0 > 1.0) { // to avoid timeout.
            ctrl.dummy();
            t0 = t1;
        }
        remaining -= bulkLb;
    }
    ctrl.end();
    return true;
}

} // namespace archive_local

inline void ArchiveVolState::initInner(const std::string& volId)
{
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, ga.thinpool, diffMgr);
    if (volInfo.existsVolDir()) {
        sm.set(volInfo.getState());
        WalbDiffFiles wdiffs(diffMgr, volInfo.volDir.str());
        wdiffs.reload();
    } else {
        sm.set(aClear);
    }
}

inline void verifyArchiveVol(const std::string& volId)
{
    const char *const FUNC = __func__;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();

    if (st == aClear) return;

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const std::string st2 = volInfo.getState();
    if (st2 != st) {
        throw cybozu::Exception(FUNC) << "invalid state" << volId << st << st2;
    }
    // file existance check.
    volInfo.getUuid();
    volInfo.getMetaState();

    if (st == aSyncReady) return;

    assert(isStateIn(st, aActive));
    if (!volInfo.lvExists()) {
        throw cybozu::Exception(FUNC) << "base lv must exist" << volId;
    }
}

inline void gcArchiveVol(const std::string& volId)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();

    if (!isStateIn(st, aActive)) return;

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const size_t nrDiffs = volInfo.gcDiffs();
    if (nrDiffs > 0) {
        LOGs.info() << volId << "garbage collected wdiff files" << nrDiffs;
    }
    const size_t nrVols = volInfo.gcVolumes();
    if (nrVols) {
        LOGs.info() << volId << "garbage collected incompleted restored volumes" << nrVols;
    }
}

inline void c2aStatusServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        StrVec strV;
        if (v.empty()) {
            strV = archive_local::getAllStatusAsStrVec();
        } else {
            const std::string &volId = v[0];
            strV = archive_local::getVolStatusAsStrVec(volId);
        }
        protocol::sendValueAndFin(pkt, sendErr, strV);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
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
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);

        StateMachineTransaction tran(volSt.sm, aClear, atInitVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volInfo.init();
        tran.commit(aSyncReady);
        pkt.writeFin(msgOk);
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

        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);
        StateMachine &sm = volSt.sm;
        const std::string &currSt = sm.get(); // aStopped or aSyncReady

        StateMachineTransaction tran(sm, currSt, atClearVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volInfo.clear();
        tran.commit(aClear);
        pkt.writeFin(msgOk);
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
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);

        StateMachineTransaction tran(volSt.sm, aStopped, atStart, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        const std::string st = volInfo.getState();
        if (st != aStopped) {
            throw cybozu::Exception(FUNC) << "not Stopped state" << st;
        }
        volInfo.setState(aArchived);
        tran.commit(aArchived);

        pkt.writeFin(msgOk);
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
        pkt.writeFin(msgAccept);
        sendErr = false;
        UniqueLock ul(volSt.mu);
        StateMachine &sm = volSt.sm;

        waitUntil(ul, [&]() {
                return isStateIn(sm.get(), aSteadyStates)
                    && volSt.ac.isAllZero(allActionVec);
            }, FUNC);

        logger.info() << "Tasks have been stopped" << volId;
        verifyStateIn(sm.get(), {aArchived}, FUNC);

        StateMachineTransaction tran(sm, aArchived, atStop, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
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
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        const std::string targetName = volInfo.restoredSnapshotName(gid);
        cybozu::lvm::Lv lv = volInfo.getLv();
        if (lv.hasSnapshot(targetName)) {
            throw cybozu::Exception(FUNC) << "already restored" << volId << gid;
        }
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        verifyActionNotRunning(volSt.ac, aDenyForRestore, FUNC);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
        return;
    }
    pkt.writeFin(msgAccept);

    ActionCounterTransaction tran(volSt.ac, aaRestore);
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
        verifyStateIn(volSt.sm.get(), aActiveOrStopped, FUNC);
        verifyActionNotRunning(volSt.ac, aActionOnLvm, FUNC);
        ul.unlock();

        archive_local::delRestored(volId, gid);
        logger.info() << "del-restored succeeded" << volId << gid;
        pkt.writeFin(msgOk);
        sendErr = false;
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
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);

        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        WalbDiffFiles wdiffs(volSt.diffMgr, volInfo.volDir.str());
        wdiffs.reload();
        pkt.writeFin(msgOk);
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
    bool isErr = true;
    bool sendErr = true;
    try {
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
        if (volId.empty()) {
            isErr = false;
            throw cybozu::Exception(FUNC) << "empty volId";
        }
        if (hostType != proxyHT && hostType != archiveHT) {
            isErr = false;
            throw cybozu::Exception(FUNC) << "bad hostType" << hostType;
        }
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        {
            const std::string st = sm.get();
            const char *msg = nullptr;
            if (st == aStopped || st == atStart) {
                msg = msgStopped;
            } else if (st == atFullSync || st == atHashSync) {
                msg = msgSyncing;
            } else if (st == atWdiffRecv) {
                msg = msgWdiffRecv;
            }
            if (msg) {
                logger.info() << msg << volId;
                pkt.writeFin(msg);
                return;
            }
            if (st != aArchived) {
                isErr = false;
                throw cybozu::Exception(FUNC) << "bad state" << st;
            }
        }

        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        if (!volInfo.existsVolDir()) {
            const char *msg = msgArchiveNotFound;
            logger.info() << msg << volId;
            pkt.writeFin(msg);
            return;
        }
        if (hostType == proxyHT && volInfo.getUuid() != uuid) {
            const char *msg = msgDifferentUuid;
            logger.info() << msg << volId;
            pkt.writeFin(msg);
            return;
        }
        const uint64_t selfSizeLb = volInfo.getLv().sizeLb();
        if (selfSizeLb < sizeLb) {
            const char *msg = msgSmallerLvSize;
            logger.error() << msg << volId << sizeLb << selfSizeLb;
            pkt.writeFin(msg);
            return;
        }

        if (sizeLb < selfSizeLb) {
            logger.warn() << "larger lv size" << volId << sizeLb << selfSizeLb;
            // no problem to continue.
        }
        const MetaSnap latestSnap = volInfo.getLatestSnapshot();
        const Relation rel = getRelation(latestSnap, diff);

        if (rel != Relation::APPLICABLE_DIFF) {
            const char *msg;
            switch (rel) {
            case Relation::TOO_OLD_DIFF:
                msg = msgTooOldDiff;
                break;
            case Relation::TOO_NEW_DIFF:
                msg = msgTooNewDiff;
                break;
            default:
                throw cybozu::Exception(FUNC)
                    << "bad meta diff relation" << (int)rel
                    << latestSnap << diff;
            }
            logger.info() << msg << volId;
            pkt.writeFin(msg);
            return;
        }
        pkt.write(msgAccept);
        sendErr = false;

        // main procedure
        StateMachineTransaction tran(sm, aArchived, atWdiffRecv, FUNC);
        ul.unlock();

        const cybozu::FilePath fPath = volInfo.getDiffPath(diff);
        cybozu::TmpFile tmpFile(volInfo.volDir.str());
        cybozu::util::File fileW(tmpFile.fd());
        writeDiffFileHeader(fileW, maxIoBlocks, uuid);
        if (!wdiffTransferServer(pkt, tmpFile.fd(), volSt.stopState, ga.ps)) {
            logger.warn() << FUNC << "force stopped" << volId;
            return;
        }
        diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
        tmpFile.save(fPath.str());

        ul.lock();
        volSt.diffMgr.add(diff);
        tran.commit(aArchived);
        packet::Ack(p.sock).sendFin();
        volSt.updateLastWdiffReceivedTime();
        logger.info() << "wdiff-transfer succeeded" << volId;
    } catch (std::exception &e) {
        if (isErr) {
            logger.error() << e.what();
        } else {
            logger.warn() << e.what();
        }
        if (sendErr) pkt.write(e.what());
    }
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
        verifyActionNotRunning(volSt.ac, aDenyForReplSyncClient, FUNC);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);

        ActionCounterTransaction tran(volSt.ac, aaReplSync);
        ul.unlock();
        cybozu::Socket aSock = archive_local::runReplSync1stNegotiation(volId, hostInfo.addrPort);
        pkt.writeFin(msgAccept);
        sendErr = false;
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
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);
        const std::string stFrom = volSt.sm.get();
        verifyStateIn(stFrom, aAcceptForReplicateServer, FUNC);
        const bool isFull = stFrom == aSyncReady;
        const std::string stTo = isFull ? atFullSync : atReplSync;

        pkt.write(msgAccept);
        sendErr = false;

        StateMachineTransaction tran(volSt.sm, stFrom, stTo, FUNC);
        ul.unlock();
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
        verifyActionNotRunning(volSt.ac, aDenyForApply, FUNC);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        archive_local::verifyApplicable(volId, gid);

        pkt.writeFin(msgAccept);
        sendErr = false;

        ActionCounterTransaction tran(volSt.ac, aaApply);
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
        verifyActionNotRunning(volSt.ac, aDenyForMerge, FUNC);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        archive_local::verifyNotApplying(volId);
        archive_local::verifyMergeable(volId, gidB);

        pkt.writeFin(msgAccept);
        sendErr = false;

        ActionCounterTransaction tran(volSt.ac, aaMerge);
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
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyActionNotRunning(volSt.ac, aDenyForResize, FUNC);
        verifyStateIn(volSt.sm.get(), aAcceptForResize, FUNC);

        ActionCounterTransaction tran(volSt.ac, aaResize);
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

/**
 * params[0]: volId
 */
inline void c2aResetVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
        const std::string &volId = v[0];

        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);
        const std::string &currSt = volSt.sm.get(); // aStopped or aSyncReady

        StateMachineTransaction tran(volSt.sm, currSt, atResetVol, FUNC);
        ul.unlock();

        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volInfo.clear();
        volInfo.init();
        tran.commit(aSyncReady);

        pkt.writeFin(msgOk);
        logger.info() << "reset succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * params[0]: volId
 * params[1]: gid
 * params[2]: bulkSizeU (optional)
 */
inline void c2aBlockHashServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);
    bool sendErr = true;

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        std::string volId, gidStr, bulkSizeU;
        cybozu::util::parseStrVec(v, 0, 2, {&volId, &gidStr, &bulkSizeU});
        const uint64_t gid = cybozu::atoi(gidStr);
        uint64_t bulkLb = DEFAULT_BULK_LB;
        if (!bulkSizeU.empty()) bulkLb = util::parseBulkLb(bulkSizeU, FUNC);

        ArchiveVolState &volSt = getArchiveVolState(volId);
        // This does not lock volSt.
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        pkt.write(msgAccept);

        cybozu::murmurhash3::Hash hash;
        if (!archive_local::getBlockHash(volId, gid, bulkLb, pkt, logger, hash)) {
            throw cybozu::Exception(FUNC) << "force stopped" << volId;
        }
        pkt.write(msgOk);
        sendErr = false;
        pkt.writeFin(hash);
        logger.info() << "bhash succeeded" << volId << hash;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * This is dangerous. Use for debug purpose.
 */
inline void c2aSetUuidServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const std::string &uuidStr = v[1];
        cybozu::Uuid uuid;
        uuid.set(uuidStr);
        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volInfo.setUuid(uuid);
        ul.unlock();
        pkt.writeFin(msgOk);
        logger.info() << "set-uuid succeeded" << volId << uuid;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * This is dangerous. Use for debug purpose.
 */
inline void c2aSetStateServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const std::string &state = v[1];
        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volSt.sm.set(state);
        volInfo.setState(state);
        ul.unlock();
        pkt.writeFin(msgOk);
        logger.info() << "set-state succeeded" << volId << state;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * This is dangerous. Use for debug purpose.
 */
inline void c2aSetBaseServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const std::string &metaStateStr = v[1];
        MetaState metaSt = strToMetaState(metaStateStr);
        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volInfo.setMetaState(metaSt);
        ul.unlock();
        pkt.writeFin(msgOk);
        logger.info() << "set-base succeeded" << volId << metaSt;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

const protocol::GetCommandHandlerMap archiveGetHandlerMap = {
    { stateTN, archive_local::getState },
    { hostTypeTN, archive_local::getHostType },
    { volTN, archive_local::getVolList },
    { pidTN, archive_local::getPid },
    { diffTN, archive_local::getDiffList },
    { totalDiffSizeTN, archive_local::getTotalDiffSize },
    { existsDiffTN, archive_local::existsDiff },
    { numActionTN, archive_local::getNumAction },
    { restoredTN, archive_local::getRestored },
    { restorableTN, archive_local::getRestorable },
    { uuidTN, archive_local::getUuid },
    { baseTN, archive_local::getBase },
};

inline void c2aGetServer(protocol::ServerParams &p)
{
    protocol::runGetCommandServer(p, ga.nodeId, archiveGetHandlerMap);
}

inline void c2aExecServer(protocol::ServerParams &p)
{
    protocol::runExecServer(p, ga.nodeId);
}

const protocol::Str2ServerHandler archiveHandlerMap = {
    // commands
    { statusCN, c2aStatusServer },
    { initVolCN, c2aInitVolServer },
    { clearVolCN, c2aClearVolServer },
    { resetVolCN, c2aResetVolServer },
    { startCN, c2aStartServer },
    { stopCN, c2aStopServer },
    { restoreCN, c2aRestoreServer },
    { delRestoredCN, c2aDelRestoredServer },
    { dbgReloadMetadataCN, c2aReloadMetadataServer },
    { replicateCN, c2aReplicateServer },
    { applyCN, c2aApplyServer },
    { mergeCN, c2aMergeServer },
    { resizeCN, c2aResizeServer },
    { blockHashCN, c2aBlockHashServer },
    { dbgSetUuid, c2aSetUuidServer },
    { dbgSetState, c2aSetStateServer },
    { dbgSetBase, c2aSetBaseServer },
    { getCN, c2aGetServer },
    { execCN, c2aExecServer },
    // protocols.
    { dirtyFullSyncPN, x2aDirtyFullSyncServer },
    { dirtyHashSyncPN, x2aDirtyHashSyncServer },
    { wdiffTransferPN, x2aWdiffTransferServer },
    { replSyncPN, a2aReplSyncServer },
};

} // namespace walb
