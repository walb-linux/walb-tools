#include "archive.hpp"
#include "server_util.hpp"

namespace walb {

namespace archive_local {

/**
 * @gid UINT64_MAX --> use base image.
 *      other --> use cold snapshot with the gid.
 */
void prepareRawFullScanner(
    cybozu::util::File &file, ArchiveVolState &volSt, uint64_t sizeLb, uint64_t gid)
{
    const bool useCold = (gid != UINT64_MAX);
    VolLvCache& lvC = volSt.lvCache;
    cybozu::lvm::Lv baseLv = lvC.getLv();
    cybozu::lvm::Lv lv = (useCold ? lvC.getCold(gid) : baseLv);
    if (sizeLb > lv.sizeLb()) {
        if (useCold && sizeLb == baseLv.sizeLb()) {
            lv.resize(sizeLb);
            lvC.resizeCold(gid, sizeLb);
        } else {
            throw cybozu::Exception(__func__) << "bad sizeLb" << sizeLb << lv.sizeLb();
        }
    }
    file.open(lv.path().str(), O_RDONLY);
}


void prepareVirtualFullScanner(
    VirtualFullScanner &virt, ArchiveVolState &volSt,
    ArchiveVolInfo &volInfo, uint64_t sizeLb, const MetaSnap &snap)
{
    MetaState st0;
    bool isCold = false;
    if (snap.isClean()) {
        st0 = volInfo.getMetaStateForRestore(snap.gidB, isCold);
    } else {
        st0 = volInfo.getMetaState();
    }

    cybozu::util::File fileR;
    const uint64_t gid = (isCold ? st0.snapB.gidB : UINT64_MAX);
    prepareRawFullScanner(fileR, volSt, sizeLb, gid);

    std::vector<cybozu::util::File> fileV;
    MetaDiffVec diffV = tryOpenDiffs(
        fileV, volInfo, allowEmpty, st0, [&](const MetaState &st) {
            return volInfo.getDiffMgr().getDiffListToSync(st, snap);
        });
    LOGs.debug() << "virtual-full-scan-diffs" << st0 << diffV;

    virt.init(std::move(fileR), std::move(fileV));
}


void verifyApplicable(const std::string& volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    UniqueLock ul(volSt.mu);

    const MetaState st = volInfo.getMetaState();
    if (volSt.diffMgr.getDiffListToApply(st, gid).empty()) {
        throw cybozu::Exception(__func__) << "There is no diff to apply" << volId;
    }
}


#define USE_AIO_FOR_APPLY_OPENED_DIFFS
const size_t ASYNC_IO_BUFFER_SIZE = (4U << 20);  // bytes


bool applyOpenedDiffs(std::vector<cybozu::util::File>&& fileV, cybozu::lvm::Lv& lv,
                      const std::atomic<int>& stopState,
                      DiffStatistics& statIn, DiffStatistics& statOut, std::string& memUsageStr)
{
    const char *const FUNC = __func__;
    statOut.clear();
    DiffMerger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();
    DiffRecIo recIo;
    const std::string lvPathStr = lv.path().str();
    cybozu::util::File file(lvPathStr, O_RDWR | O_DIRECT);
#ifdef USE_AIO_FOR_APPLY_OPENED_DIFFS
    AsyncBdevWriter writer(file.fd(), ASYNC_IO_BUFFER_SIZE);
#else
    AlignedArray zero;
#endif
    const uint64_t lvSnapSizeLb = lv.sizeLb();
    double t0 = cybozu::util::getTimeMonotonic();
    size_t totalSleepMs = 0;
    size_t writtenSize = 0; // bytes
    Sleeper sleeper;
    const size_t minMs = 100, maxMs = 1000;
    sleeper.init(ga.pctApplySleep * 10, minMs, maxMs, t0);
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
#ifdef USE_AIO_FOR_APPLY_OPENED_DIFFS
        issueAio(writer, ga.discardType, rec, recIo.moveIoFrom());
#else
        issueIo(file, ga.discardType, rec, recIo.io().data(), zero);
#endif

        writtenSize += ioBlocks * LOGICAL_BLOCK_SIZE;
        if (writtenSize >= ga.fsyncIntervalSize) {
#ifdef USE_AIO_FOR_APPLY_OPENED_DIFFS
            writer.waitForAll();
#endif
            file.fdatasync();
            writtenSize = 0;
        }

        const double t1 = cybozu::util::getTimeMonotonic();
        if (t1 - t0 > PROGRESS_INTERVAL_SEC) {
            LOGs.info() << FUNC << "progress" << lvPathStr
                        << cybozu::util::formatString("%" PRIu64 "/%" PRIu64 "", ioAddress, lvSnapSizeLb);
            t0 = t1;
        }
        totalSleepMs += sleeper.sleepIfNecessary(t1);
    }
#ifdef USE_AIO_FOR_APPLY_OPENED_DIFFS
    writer.waitForAll();
#endif
    file.fdatasync();
    file.close();
    statIn = merger.statIn();
    statOut.wdiffNr = -1;
    statOut.dataSize = -1;
    memUsageStr = merger.memUsageStr();
    LOGs.info() << FUNC << "totalSleepMs" << lvPathStr << totalSleepMs;
    return true;
}


enum class ApplyState {
    FAILURE,
    REMAINING,
    DONE,
};

static ApplyState applyDiffsToVolumeOnce(const std::string& volId, const MetaState& st0, uint64_t gid, MetaState& st1)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    MetaDiffManager &mgr = volSt.diffMgr;
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    VolLvCache &lvC = volSt.lvCache;

    std::vector<cybozu::util::File> fileV;
    MetaDiffVec diffV = tryOpenDiffs(
        fileV, volInfo, allowEmpty, st0, [&](const MetaState &st) {
            return mgr.getDiffListToApply(st, gid, ga.maxOpenDiffs);
        });
    if (diffV.empty()) return ApplyState::DONE;

    LOGs.debug() << "apply-diffs" << volId << st0 << diffV;
    const MetaState st01 = beginApplying(st0, diffV);
    volInfo.setMetaState(st01);

    cybozu::lvm::Lv lv = lvC.getLv(); // base image.
    DiffStatistics statIn, statOut;
    std::string memUsageStr;
    if (!applyOpenedDiffs(std::move(fileV), lv, volSt.stopState, statIn, statOut, memUsageStr)) {
        return ApplyState::FAILURE;
    }
    st1 = endApplying(st01, diffV);

    LOGs.info() << "apply-mergeIn " << volId << statIn;
    LOGs.info() << "apply-mergeOut" << volId << statOut;
    LOGs.info() << "apply-mergeMemUsage" << volId << memUsageStr;
    LOGs.info() << "apply-status" << volId << st0 << st1;

    volInfo.setMetaState(st1);
    volInfo.removeBeforeGid(st1.snapB.gidB);
    return ApplyState::REMAINING;
}


bool applyDiffsToVolume(const std::string& volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    {
        UniqueLock ul(volSt.mu);
        volInfo.recoverColdToBaseIfNecessary();
    }

    bool useCold;
    MetaState st0 = volInfo.getMetaStateForApply(gid, useCold);

    if (useCold) {
        UniqueLock ul(volSt.mu);
        const uint64_t coldGid = st0.snapB.gidB;
        LOGs.info() << "make cold image to base: start" << volId << coldGid;
        volInfo.makeColdToBase(coldGid);
        LOGs.info() << "make cold image to base: end" << volId << coldGid;

        if (coldGid == gid) {
            LOGs.info() << "no need to apply-diffs." << volId << coldGid;
            return true;
        }
    }

    for (;;) {
        MetaState st1;
        const ApplyState ret = applyDiffsToVolumeOnce(volId, st0, gid, st1);
        switch (ret) {
        case ApplyState::DONE:
            return true;
        case ApplyState::FAILURE:
            return false;
        case ApplyState::REMAINING:
            st0 = st1;
            continue;
        default:
            throw cybozu::Exception(__func__) << "BUG";
        }
    }
}


void verifyNotApplying(const std::string &volId)
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


void verifyMergeable(const std::string &volId, uint64_t gid)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    MetaDiffVec diffV = volSt.diffMgr.getMergeableDiffList(gid);
    if (diffV.size() < 2) {
        throw cybozu::Exception(__func__) << "There is no mergeable diff.";
    }
}


bool mergeDiffs(const std::string &volId, uint64_t gidB, bool isSize, uint64_t param3)
{
    ArchiveVolState& volSt = getArchiveVolState(volId);
    MetaDiffManager &mgr = volSt.diffMgr;
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    std::vector<cybozu::util::File> fileV;
    MetaState st0 = volInfo.getMetaState();
    MetaDiffVec diffV = tryOpenDiffs(
        fileV, volInfo, allowEmpty, st0, [&](const MetaState &) {
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

    SortedDiffWriter writer;
    writer.setFd(tmpFile.fd());
    DiffFileHeader wdiffH = merger.header();
    writer.writeHeader(wdiffH);
    DiffRecIo recIo;
    while (merger.getAndRemove(recIo)) {
        if (volSt.stopState == ForceStopping || ga.ps.isForceShutdown()) {
            return false;
        }
        // TODO: currently we can use snappy only.
        writer.compressAndWriteDiff(recIo.record(), recIo.io().data());
    }
    writer.close();

    mergedDiff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(diffPath.str());
    mgr.add(mergedDiff);
    volInfo.removeDiffs(diffV);

    LOGs.info() << "merge-mergeIn " << volId << merger.statIn();
    LOGs.info() << "merge-mergeOut" << volId << writer.getStat();
    LOGs.info() << "merge-mergeMemUsage" << volId << merger.memUsageStr();
    LOGs.info() << "merged" << volId << diffV.size() << mergedDiff;
    return true;
}


struct TmpSnapshotDeleter
{
    std::string vgName;
    std::string name;
    ~TmpSnapshotDeleter()
        try
    {
        removeLv(vgName, name);
    } catch (...) {
    }
};

static void removeColdSnapshotsExceptTheLatestOne(ArchiveVolInfo &volInfo)
{
    const char *const FUNC = __func__;
    VolLvCache &lvC = volInfo.lvCache();
    VolLvCache::LvMap lvMap = lvC.getColdMap();

    // Remove the last element from the map to keep the latest cold snapshot.
    if (lvMap.empty()) return;
    VolLvCache::LvMap::iterator it = lvMap.end();
    --it;
    lvMap.erase(it);
    if (lvMap.empty()) return;

    // Remove the remaining cold snapshots.
    std::vector<uint64_t> removed;
    for (VolLvCache::LvMap::value_type& p : lvMap) {
        const uint64_t gid = p.first;
        cybozu::lvm::Lv& coldLv = p.second;
        if (coldLv.exists()) {
            try {
                util::flushBdevBufs(coldLv.path().str());
                coldLv.remove();
            } catch (std::exception &e) {
                LOGs.warn() << FUNC << e.what();
                continue;
            }
        }
        volInfo.removeColdTimestamp(gid);
        lvC.removeCold(gid);
        removed.push_back(gid);
    }
    LOGs.info() << "Removed cold snapshots" << volInfo.volId
                << cybozu::util::concat(removed, ", ");
}


static bool applyDiffsToRestore(
    const std::string& volId, cybozu::lvm::Lv& tmpLv,
    const MetaState& st0, uint64_t gid, MetaState& st1)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    std::vector<cybozu::util::File> fileV;
    MetaDiffVec diffV = tryOpenDiffs(
        fileV, volInfo, !allowEmpty, st0, [&](const MetaState &st) {
            return volSt.diffMgr.getDiffListToRestore(st, gid, ga.maxOpenDiffs);
        });
    LOGs.debug() << "restore-diffs" << volId << st0 << diffV;
    DiffStatistics statIn, statOut;
    std::string memUsageStr;
    if (!applyOpenedDiffs(std::move(fileV), tmpLv, volSt.stopState, statIn, statOut, memUsageStr)) {
        return false;
    }
    st1 = apply(st0, diffV);

    LOGs.info() << "restore-mergeIn " << volId << statIn;
    LOGs.info() << "restore-mergeOut" << volId << statOut;
    LOGs.info() << "restore-mergeMemUsage" << volId << memUsageStr;
    LOGs.info() << "restore-status" << volId << st0 << st1;

    return true;
}


/**
 * Restore a snapshot.
 * (1) create lvm snapshot of base lv. (with temporary lv name)
 * (2) apply appropriate wdiff files.
 * (3) rename the lvm snapshot.
 *
 * RETURN:
 *   false if force stopped.
 */
bool restore(const std::string &volId, uint64_t gid)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    VolLvCache& lvC = volSt.lvCache;
    cybozu::lvm::Lv baseLv = lvC.getLv();
    const std::string targetName = volInfo.restoredSnapshotName(gid);
    const std::string tmpLvName = volInfo.tmpRestoredSnapshotName(gid);
    removeLv(baseLv.vgName(), tmpLvName);

    bool useCold;
    MetaState st0 = volInfo.getMetaStateForRestore(gid, useCold);

    cybozu::lvm::Lv tmpLv;
    if (isThinpool()) {
        if (useCold) {
            /* Here the cold lv must exist. */
            cybozu::lvm::Lv coldLv = lvC.getCold(st0.snapB.gidB);
            tmpLv = coldLv.createTvSnap(tmpLvName, true);
            if (tmpLv.sizeLb() < baseLv.sizeLb()) {
                tmpLv.resize(baseLv.sizeLb());
            }
        } else {
            /* Use base image for snapshot origin. */
            tmpLv = baseLv.createTvSnap(tmpLvName, true);
        }
    } else {
        assert(!useCold);
        const uint64_t snapSizeLb = uint64_t((double)(baseLv.sizeLb()) * 1.2);
        tmpLv = baseLv.createLvSnap(tmpLvName, true, snapSizeLb);
    }
    TmpSnapshotDeleter deleter{baseLv.vgName(), tmpLvName};

    bool noNeedToApply =
        !st0.isApplying && st0.snapB.isClean() && st0.snapB.gidB == gid;
    MetaState st1 = st0;
    while (!noNeedToApply) {
        if (!applyDiffsToRestore(volId, tmpLv, st0, gid, st1)) return false;
        noNeedToApply = !st1.isApplying && st1.snapB.isClean() && st1.snapB.gidB == gid;
        st0 = st1;
    }
    if (isThinpool()) {
        util::flushBdevBufs(tmpLv.path().str());
        const std::string coldLvName = volInfo.coldSnapshotName(gid);
        if (!cybozu::lvm::existsFile(baseLv.vgName(), coldLvName)) {
            cybozu::lvm::Lv coldLv = tmpLv.createTvSnap(coldLvName, false);
            lvC.addCold(gid, coldLv);
            // If crash occurs here, the timestamp file does not exist but lv does.
            // Such situations must be recovered by verifyAndRecoverArchiveVol().
            volInfo.setColdTimestamp(gid, st1.timestamp);
        }
    }
    cybozu::lvm::Lv snapLv = cybozu::lvm::renameLv(baseLv.vgName(), tmpLvName, targetName);
    lvC.addRestored(gid, snapLv);
    util::flushBdevBufs(snapLv.path().str());
    if (isThinpool() && ga.keepOneColdSnapshot) {
        removeColdSnapshotsExceptTheLatestOne(volInfo);
    }
    return true;
}


void delSnapshot(const std::string &volId, uint64_t gid, bool isCold)
{
    const char *const FUNC = __func__;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    VolLvCache &lvC = volSt.lvCache;
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    if ((isCold && !lvC.hasCold(gid)) || (!isCold && !lvC.hasRestored(gid))) {
        throw cybozu::Exception(FUNC)
            << "volume not found" << volId << gid << (isCold ? "cold" : "restored");
    }
    cybozu::lvm::Lv snap = isCold ? lvC.getCold(gid) : lvC.getRestored(gid);
    if (snap.exists()) {
        util::flushBdevBufs(snap.path().str());
        snap.remove();
    }
    if (isCold) {
        lvC.removeCold(gid);
        volInfo.removeColdTimestamp(gid);
    } else {
        lvC.removeRestored(gid);
    }
}


StrVec listRestorable(const std::string &volId, bool isAll)
{
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    StrVec ret;
    const std::vector<MetaState> stV = volInfo.getRestorableSnapshots(isAll);
    for (const MetaState &st : stV) {
        ret.push_back(cybozu::itoa(st.snapB.gidB)
                      + ' ' + util::timeToPrintable(st.timestamp)
                      + ' ' + (st.isExplicit ? '1' : '0'));
    }
    return ret;
}


/**
 * Do auto-resize base image if necessary.
 */
void doAutoResizeIfNecessary(ArchiveVolState& volSt, ArchiveVolInfo& volInfo, uint64_t sizeLb)
{
    if (!ga.doAutoResize) return;
    if (!volSt.lvCache.exists()) return;
    cybozu::lvm::Lv lv = volSt.lvCache.getLv();
    if (sizeLb <= lv.sizeLb()) return;

    const std::string& volId = volInfo.volId;
    LOGs.info() << "try to auto-resize" << volId << lv.sizeLb() << sizeLb;
    // Currently zero-fill is not supported cause it will take a long time.
    volInfo.growLv(sizeLb, false);
    LOGs.info() << "auto-resize succeeded" << volId << lv.sizeLb() << sizeLb;
}


/**
 * This is debug code.
 */
void dbgVerifyLatestMetaState(const std::string &volId)
{
#ifdef NDEBUG
    unusedVar(volId);
#else
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string state = volSt.sm.get();
    if (!isStateIn(state, aActiveOrStopped)) return;

    const MetaState metaSt0 = volSt.getLatestMetaState();
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const MetaState metaSt1 = volInfo.getLatestState();
    if (metaSt0 != metaSt1) {
        throw cybozu::Exception(__func__) << "meta state cache is invalid" << metaSt0 << metaSt1;
    }
#endif
}


void backupServer(protocol::ServerParams &p, bool isFull)
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
        if (hostType != storageHT) {
            throw cybozu::Exception(FUNC) << "invalid hostType" << hostType;
        }
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb is zero";
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);
        verifyStateIn(sm.get(), {stFrom}, FUNC);
        if (!isFull) snapFrom = volSt.getLatestMetaState().snapB;
    } catch (std::exception &e) {
        logger.warn() << e.what();
        pkt.write(e.what());
        return;
    }
    volSt.progressLb = 0;
    ZeroResetter resetter(volSt.progressLb);
    pkt.write(msgAccept);
    if (!isFull) pkt.write(snapFrom);
    pkt.flush();
    cybozu::Uuid uuid;
    pkt.read(uuid);
    packet::Ack(p.sock).send();
    pkt.flush();

    const std::string &stPass = isFull ? atFullSync : atHashSync;
    StateMachineTransaction tran(sm, stFrom, stPass, FUNC);
    ul.unlock();

    cybozu::Stopwatch stopwatch;
    const std::string st = volInfo.getState();
    if (st != stFrom) {
        throw cybozu::Exception(FUNC) << "state is not" << stFrom << "but" << st;
    }
    logger.info() << (isFull ? dirtyFullSyncPN : dirtyHashSyncPN) << "started" << volId
                  << p.clientId << sizeLb << bulkLb;
    bool isOk;
    std::unique_ptr<cybozu::TmpFile> tmpFileP;
    if (isFull) {
        volInfo.createLv(sizeLb);
        const std::string lvPath = volSt.lvCache.getLv().path().str();
        const bool skipZero = isThinpool();
        isOk = dirtyFullSyncServer(pkt, lvPath, 0, sizeLb, bulkLb, volSt.stopState, ga.ps, volSt.progressLb,
                                   skipZero, ga.fsyncIntervalSize);
    } else {
        doAutoResizeIfNecessary(volSt, volInfo, sizeLb);
        const uint32_t hashSeed = curTime;
        tmpFileP.reset(new cybozu::TmpFile(volInfo.volDir.str()));
        VirtualFullScanner virt;
        archive_local::prepareVirtualFullScanner(virt, volSt, volInfo, sizeLb, snapFrom);
        isOk = dirtyHashSyncServer(pkt, virt, sizeLb, bulkLb, uuid, hashSeed, true, tmpFileP->fd(),
                                   ga.discardType, volSt.stopState, ga.ps, volSt.progressLb,
                                   ga.fsyncIntervalSize);
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
    MetaState metaSt(snapTo, curTime);
    if (isFull) {
        volInfo.setMetaState(metaSt);
        volInfo.generateArchiveUuid();
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
    volSt.setLatestMetaState(metaSt);
    dbgVerifyLatestMetaState(volId);
    volInfo.setUuid(uuid);
    volSt.updateLastSyncTime();
    volInfo.setState(aArchived);
    tran.commit(aArchived);
    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());

    packet::Ack(p.sock).sendFin();
    logger.info() << (isFull ? dirtyFullSyncPN : dirtyHashSyncPN)
                  << "succeeded" << volId << elapsed;
}


void delSnapshotServer(protocol::ServerParams &p, bool isCold)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const VolIdAndGidParam param = parseVolIdAndGidParam(protocol::recvStrVec(p.sock, 2, FUNC), 0, true, 0);
        const std::string &volId = param.volId;
        const uint64_t gid = param.gid;

        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), aActiveOrStopped, FUNC);
        verifyActionNotRunning(volSt.ac, aActionOnLvm, FUNC);
        ul.unlock();

        delSnapshot(volId, gid, isCold);

        logger.info() << cybozu::util::formatString("del-%s succeeded", isCold ? "cold" : "restored")
                      << volId << gid;
        pkt.writeFin(msgOk);
        sendErr = false;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void verifyVolumeSize(ArchiveVolState &volSt, ArchiveVolInfo &volInfo, uint64_t sizeLb, Logger &logger)
{
    const char *const FUNC = __func__;
    if (!volInfo.lvExists()) {
        logger.debug() << FUNC << "lv does not exist" << volInfo.volId;
        return;
    }
    const uint64_t selfSizeLb = volSt.lvCache.getLv().sizeLb();
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


bool runFullReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, uint64_t bulkLb, Logger &logger)
{
    const char *const FUNC = __func__;
    cybozu::lvm::Lv lv = volSt.lvCache.getLv();
    const uint64_t sizeLb = lv.sizeLb();
    const MetaState metaSt = volInfo.getMetaState();
    const cybozu::Uuid uuid = volInfo.getUuid();
    pkt.write(sizeLb);
    pkt.write(bulkLb);
    pkt.write(metaSt);
    pkt.write(uuid);
    pkt.flush();
    logger.debug() << "full-repl-client" << sizeLb << bulkLb << metaSt << uuid;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;
    uint64_t startLb;
    pkt.read(startLb);
    logger.info() << "full-repl-client started" << volId << dstId << sizeLb << bulkLb << startLb;

    const std::string lvPath = lv.path().str();
    const std::atomic<uint64_t> fullScanLbPerSec(0);
    if (!dirtyFullSyncClient(pkt, lvPath, startLb, sizeLb, bulkLb, volSt.stopState, ga.ps, fullScanLbPerSec)) {
        logger.warn() << "full-repl-client force-stopped" << volId;
        return false;
    }
    logger.info() << "full-repl-client done" << volId;
    getArchiveGlobal().remoteSnapshotManager.update(volId, dstId, metaSt);
    return true;
}


bool runFullReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, const cybozu::Uuid &archiveUuid, UniqueLock &ul, Logger &logger)
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
        doAutoResizeIfNecessary(volSt, volInfo, sizeLb);
        verifyVolumeSize(volSt, volInfo, sizeLb, logger);
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }
    FullReplState fullReplSt;
    const uint64_t startLb = volInfo.initFullReplResume(sizeLb, archiveUuid, metaSt, fullReplSt);
    logger.info() << "full-repl-server started" << volId << sizeLb << bulkLb << startLb;
    volSt.progressLb = startLb;
    ZeroResetter resetter(volSt.progressLb);

    pkt.write(msgOk);
    pkt.write(startLb);
    pkt.flush();

    cybozu::Stopwatch stopwatch;
    StateMachineTransaction tran(volSt.sm, aSyncReady, atFullSync, FUNC);
    ul.unlock();
    volInfo.setArchiveUuid(archiveUuid);
    volInfo.prepareBaseImageForFullRepl(sizeLb, startLb);
    const std::string lvPath = volSt.lvCache.getLv().path().str();
    const bool skipZero = isThinpool();
    if (!dirtyFullSyncServer(pkt, lvPath, startLb, sizeLb, bulkLb, volSt.stopState, ga.ps,
                             volSt.progressLb, skipZero, ga.fsyncIntervalSize,
                             &fullReplSt, volInfo.volDir, volInfo.getFullReplStateFileName())) {
        logger.warn() << "full-repl-server force-stopped" << volId;
        return false;
    }
    ul.lock();
    volInfo.setMetaState(metaSt);
    volSt.setLatestMetaState(metaSt);
    dbgVerifyLatestMetaState(volId);
    volInfo.setUuid(uuid);
    volSt.updateLastSyncTime();
    volInfo.removeFullReplState();
    volInfo.setState(aArchived);
    tran.commit(aArchived);
    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
    logger.info() << "full-repl-server done" << volId << elapsed;
    return true;
}


bool runHashReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, uint64_t bulkLb, const MetaDiff &diff, Logger &logger)
{
    const char *const FUNC = __func__;
    const uint64_t sizeLb = volSt.lvCache.getLv().sizeLb();
    const cybozu::Uuid uuid = volInfo.getUuid();
    const uint32_t hashSeed = diff.timestamp;
    pkt.write(sizeLb);
    pkt.write(bulkLb);
    pkt.write(diff);
    pkt.write(uuid);
    pkt.write(hashSeed);
    pkt.flush();
    logger.debug() << "hash-repl-client" << sizeLb << bulkLb << diff << uuid << hashSeed;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;

    logger.info() << "hash-repl-client started" << volId << dstId << sizeLb << bulkLb << diff;
    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volSt, volInfo, sizeLb, diff.snapE);
    const std::atomic<uint64_t> fullScanLbPerSec(0);
    if (!dirtyHashSyncClient(pkt, virt, sizeLb, bulkLb, hashSeed, volSt.stopState, ga.ps, fullScanLbPerSec)) {
        logger.warn() << "hash-repl-client force-stopped" << volId;
        return false;
    }
    logger.info() << "hash-repl-client-mergeIn " << volId << virt.statIn();
    logger.info() << "hash-repl-client-mergeOut" << volId << virt.statOut();
    logger.info() << "hash-repl-client-mergeMemUsage" << volId << virt.memUsageStr();
    logger.info() << "hash-repl-client done" << volId;

    const MetaState dstMetaSt(diff.snapE, diff.timestamp);
    getArchiveGlobal().remoteSnapshotManager.update(volId, dstId, dstMetaSt);

    return true;
}


bool runHashReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, UniqueLock &ul, const MetaState &metaSt, Logger &logger)
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
        if (!canApply(metaSt, diff)) {
            throw cybozu::Exception(FUNC) << "diff is not applicable" << metaSt << diff;
        }
        doAutoResizeIfNecessary(volSt, volInfo, sizeLb);
        verifyVolumeSize(volSt, volInfo, sizeLb, logger);
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }
    volSt.progressLb = 0;
    ZeroResetter resetter(volSt.progressLb);
    pkt.write(msgOk);
    pkt.flush();

    logger.info() << "hash-repl-server started" << volId << sizeLb << bulkLb << diff;
    cybozu::Stopwatch stopwatch;
    StateMachineTransaction tran(volSt.sm, aArchived, atReplSync, FUNC);
    ul.unlock();
    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volSt, volInfo, sizeLb, diff.snapB);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    if (!dirtyHashSyncServer(pkt, virt, sizeLb, bulkLb, uuid, hashSeed, true, tmpFile.fd(),
                             ga.discardType, volSt.stopState, ga.ps, volSt.progressLb,
                             ga.fsyncIntervalSize)) {
        logger.warn() << "hash-repl-server force-stopped" << volId;
        return false;
    }
    diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(volInfo.getDiffPath(diff).str());
    ul.lock();
    volSt.diffMgr.add(diff);
    volSt.setLatestMetaState(apply(metaSt, diff));
    dbgVerifyLatestMetaState(volId);
    volInfo.setUuid(uuid);
    volSt.updateLastSyncTime();
    tran.commit(aArchived);
    logger.info() << "hash-repl-server-mergeIn " << volId << virt.statIn();
    logger.info() << "hash-repl-server-mergeOut" << volId << virt.statOut();
    logger.info() << "hash-repl-server-mergeMemUsage" << volId << virt.memUsageStr();
    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
    logger.info() << "hash-repl-server done" << volId << elapsed;
    return true;
}


bool runNoMergeDiffReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, const MetaSnap &srvLatestSnap, Logger &logger)
{
    const char *const FUNC = __func__;
    MetaState st0 = volInfo.getMetaState();
    std::vector<cybozu::util::File> fileV;
    MetaDiffVec diffV = tryOpenDiffs(
        fileV, volInfo, !allowEmpty, st0, [&](const MetaState &) {
            MetaDiff diff;
            if (volSt.diffMgr.getApplicableDiff(srvLatestSnap, diff)) {
                return MetaDiffVec{diff};
            } else {
                return MetaDiffVec{};
            }
        });
    assert(diffV.size() == 1);
    assert(fileV.size() == 1);
    MetaDiff diff = diffV[0];
    cybozu::util::File &fileR = fileV[0];

    const uint64_t sizeLb = volSt.lvCache.getLv().sizeLb();
    DiffFileHeader fileH;
    fileH.readFrom(fileR);
    const uint32_t maxIoBlocks = 0; // unused
    const cybozu::Uuid uuid = fileH.getUuid();
    pkt.write(sizeLb);
    pkt.write(maxIoBlocks);
    pkt.write(uuid);
    pkt.write(diff);
    pkt.flush();
    logger.debug() << "diff-repl-client" << sizeLb << uuid << diff;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;

    if (!wdiffTransferNoMergeClient(pkt, fileR, fileH, volSt.stopState, ga.ps)) {
        logger.warn() << "diff-repl-nomerge-client force-stopped" << volId;
        return false;
    }
    packet::Ack(pkt.sock()).recv();
    logger.info() << "diff-repl-nomerge-client done" << volId << diff;

    const MetaState dstMetaSt(diff.snapE, diff.timestamp);
    getArchiveGlobal().remoteSnapshotManager.update(volId, dstId, dstMetaSt);

    return true;
}


bool runDiffReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, const MetaSnap &srvLatestSnap, const CompressOpt &cmpr, uint64_t wdiffMergeSize, Logger &logger)
{
    const char *const FUNC = __func__;
    MetaState st0 = volInfo.getMetaState();
    std::vector<cybozu::util::File> fileV;
    MetaDiffVec diffV = tryOpenDiffs(
        fileV, volInfo, !allowEmpty, st0, [&](const MetaState &) {
            return volInfo.getDiffListToSend(srvLatestSnap, wdiffMergeSize, ga.maxWdiffSendNr);
        });

    const MetaDiff mergedDiff = merge(diffV);
    LOGs.debug() << "diff-repl-diffs" << st0 << mergedDiff << diffV;
    DiffMerger merger;
    merger.addWdiffs(std::move(fileV));
    merger.prepare();

    const uint64_t sizeLb = volSt.lvCache.getLv().sizeLb();
    const DiffFileHeader &fileH = merger.header();
    const uint32_t maxIoBlocks = 0; // unused
    const cybozu::Uuid uuid = fileH.getUuid();
    pkt.write(sizeLb);
    pkt.write(maxIoBlocks);
    pkt.write(uuid);
    pkt.write(mergedDiff);
    pkt.flush();
    logger.debug() << "diff-repl-client" << sizeLb << uuid << mergedDiff;

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

    const MetaState dstMetaSt(mergedDiff.snapE, mergedDiff.timestamp);
    getArchiveGlobal().remoteSnapshotManager.update(volId, dstId, dstMetaSt);

    return true;
}


bool runDiffReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, UniqueLock &ul, const MetaState &metaSt, Logger &logger)
{
    const char *const FUNC = __func__;
    uint64_t sizeLb;
    uint32_t maxIoBlocks;
    cybozu::Uuid uuid;
    MetaDiff diff;
    try {
        pkt.read(sizeLb);
        pkt.read(maxIoBlocks); // unused
        pkt.read(uuid);
        pkt.read(diff);
        logger.debug() << "diff-repl-server" << sizeLb << uuid << diff;
        doAutoResizeIfNecessary(volSt, volInfo, sizeLb);
        verifyVolumeSize(volSt, volInfo, sizeLb, logger);
        if (!canApply(metaSt, diff)) {
            throw cybozu::Exception(FUNC) << "can not apply" << metaSt << diff;
        }
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }
    pkt.write(msgOk);
    pkt.flush();

    cybozu::Stopwatch stopwatch;
    StateMachineTransaction tran(volSt.sm, aArchived, atReplSync, FUNC);
    ul.unlock();
    const cybozu::FilePath fPath = volInfo.getDiffPath(diff);
    cybozu::TmpFile tmpFile(volInfo.volDir.str());
    cybozu::util::File fileW(tmpFile.fd());
    writeDiffFileHeader(fileW, uuid);
    if (!wdiffTransferServer(pkt, tmpFile.fd(), volSt.stopState, ga.ps, ga.fsyncIntervalSize)) {
        logger.warn() << "diff-repl-server force-stopped" << volId;
        return false;
    }
    diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
    tmpFile.save(fPath.str());
    volSt.diffMgr.add(diff);
    volSt.setLatestMetaState(apply(metaSt, diff));
    dbgVerifyLatestMetaState(volId);
    packet::Ack(pkt.sock()).send();
    pkt.flush();
    volSt.updateLastWdiffReceivedTime();
    const size_t nrGc = volInfo.gcDiffsRange(diff.snapB.gidB, diff.snapE.gidB);
    ul.lock();
    tran.commit(aArchived);
    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
    logger.info() << "diff-repl-server done" << volId << diff << elapsed << nrGc;
    return true;
}


bool runResyncReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, uint64_t bulkLb, Logger &logger)
{
    const char *const FUNC = __func__;
    const uint64_t sizeLb = volSt.lvCache.getLv().sizeLb();
    const MetaState metaSt = volInfo.getOldestMetaState();
    const cybozu::Uuid uuid = volInfo.getUuid();
    const uint32_t hashSeed = uint32_t(metaSt.timestamp);
    const cybozu::Uuid archiveUuid = volInfo.getArchiveUuid();

    pkt.write(sizeLb);
    pkt.write(bulkLb);
    pkt.write(metaSt);
    pkt.write(uuid);
    pkt.write(archiveUuid);
    pkt.write(hashSeed);
    pkt.flush();
    logger.debug() << "resync-repl-client" << sizeLb << bulkLb << metaSt << uuid << archiveUuid << hashSeed;

    std::string res;
    pkt.read(res);
    if (res != msgOk) throw cybozu::Exception(FUNC) << "not ok" << res;

    logger.info() << "resync-repl-client started" << volId << sizeLb << bulkLb << metaSt;
    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volSt, volInfo, sizeLb, metaSt.snapB);
    const std::atomic<uint64_t> fullScanLbPerSec(0);
    if (!dirtyHashSyncClient(pkt, virt, sizeLb, bulkLb, hashSeed, volSt.stopState, ga.ps, fullScanLbPerSec)) {
        logger.warn() << "resync-repl-client force-stopped" << volId;
        return false;
    }
    logger.info() << "resync-repl-client-mergeIn " << volId << virt.statIn();
    logger.info() << "resync-repl-client-mergeOut" << volId << virt.statOut();
    logger.info() << "resync-repl-client-mergeMemUsage" << volId << virt.memUsageStr();
    logger.info() << "resync-repl-client done" << volId << metaSt;

    getArchiveGlobal().remoteSnapshotManager.update(volId, dstId, metaSt);

    return true;
}


bool runResyncReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, UniqueLock &ul, Logger &logger)
{
    const char *const FUNC = __func__;
    uint64_t sizeLb, bulkLb;
    MetaState metaSt;
    cybozu::Uuid uuid, archiveUuid;
    uint32_t hashSeed;
    try {
        pkt.read(sizeLb);
        pkt.read(bulkLb);
        pkt.read(metaSt);
        pkt.read(uuid);
        pkt.read(archiveUuid);
        pkt.read(hashSeed);
        logger.debug() << "resync-repl-server" << sizeLb << bulkLb << metaSt << uuid << hashSeed;
        if (sizeLb == 0) throw cybozu::Exception(FUNC) << "sizeLb must not be 0";
        if (bulkLb == 0) throw cybozu::Exception(FUNC) << "bulkLb must not be 0";
        doAutoResizeIfNecessary(volSt, volInfo, sizeLb);
        verifyVolumeSize(volSt, volInfo, sizeLb, logger);
    } catch (std::exception &e) {
        pkt.write(e.what());
        throw;
    }
    volSt.progressLb = 0;
    ZeroResetter resetter(volSt.progressLb);
    pkt.write(msgOk);
    pkt.flush();

    logger.info() << "resync-repl-server started" << volId << sizeLb << bulkLb << metaSt;
    cybozu::Stopwatch stopwatch;

    if (volSt.sm.get() == aArchived) {
        StateMachineTransaction tran0(volSt.sm, aArchived, atStop, FUNC);
        tran0.commit(aStopped);
        StateMachineTransaction tran1(volSt.sm, aStopped, atResetVol, FUNC);
        volInfo.setState(aSyncReady);
        tran1.commit(aSyncReady);
    }
    StateMachineTransaction tran2(volSt.sm, aSyncReady, atResync, FUNC);
    ul.unlock();

    volInfo.clearAllSnapLv();
    volInfo.clearAllWdiffs();
    {
        cybozu::util::File reader;
        prepareRawFullScanner(reader, volSt, sizeLb);
        cybozu::util::File writer(volSt.lvCache.getLv().path().str(), O_RDWR);
        /* Reader and writer indicates the same block device.
           We must have independent file descriptors for them. */
        if (!dirtyHashSyncServer(pkt, reader, sizeLb, bulkLb, uuid, hashSeed, false, writer.fd(),
                                 ga.discardType, volSt.stopState, ga.ps, volSt.progressLb,
                                 ga.fsyncIntervalSize)) {
            logger.warn() << "resync-repl-server force-stopped" << volId;
            return false;
        }
    }
    volInfo.setMetaState(metaSt);
    volSt.setLatestMetaState(metaSt);
    dbgVerifyLatestMetaState(volId);
    volInfo.setUuid(uuid);
    volInfo.setArchiveUuid(archiveUuid);
    volSt.updateLastSyncTime();
    volInfo.setState(aArchived);
    ul.lock();
    tran2.commit(aArchived);
    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
    logger.info() << "resync-repl-server done" << volId << metaSt << elapsed;
    return true;
}


bool runReplSyncClient(const std::string &volId, cybozu::Socket &sock, const HostInfoForRepl &hostInfo,
                       bool isSize, uint64_t param, const std::string &dstId, Logger &logger)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(sock);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    cybozu::Uuid archiveUuid = volInfo.getArchiveUuid();
    pkt.write(archiveUuid);
    pkt.write(hostInfo.doResync);
    pkt.flush();

    std::string res;
    pkt.read(res);
    if (res != msgAccept) {
        throw cybozu::Exception(FUNC) << "not accept" << volId << res;
    }

    bool runAtLeastOnce = false;
    int kind;
    pkt.read(kind);
    if (kind == DO_FULL_SYNC) {
        if (!runFullReplClient(volId, volSt, volInfo, dstId, pkt, hostInfo.bulkLb, logger)) {
            return false;
        }
        runAtLeastOnce = true;
    } else if (kind == DO_RESYNC) {
        if (!hostInfo.doResync) {
            throw cybozu::Exception(FUNC)
                << "bad response: resync is not allowed" << volId;
        }
        if (!runResyncReplClient(volId, volSt, volInfo, dstId, pkt, hostInfo.bulkLb, logger)) {
            return false;
        }
        runAtLeastOnce = true;
    } else if (kind != DO_HASH_OR_DIFF_SYNC) {
        throw cybozu::Exception(FUNC) << "bad resonse" << volId << kind;
    }

    MetaState srvLatestState;
    for (;;) {
        pkt.read(srvLatestState);
        const MetaSnap srvLatestSnap = srvLatestState.snapB;
        const MetaSnap cliLatestSnap = volSt.getLatestMetaState().snapB;
        const int repl = volInfo.shouldDoRepl(srvLatestSnap, cliLatestSnap, isSize, param);
        logger.debug() << "srvLatestSnap" << srvLatestSnap << "cliLatestSnap" << cliLatestSnap
                       << repl;
        pkt.write(repl);
        pkt.flush();
        if (repl == ArchiveVolInfo::DONT_REPL) break;
        if (repl == ArchiveVolInfo::DO_HASH_REPL) {
            const MetaState oldestMetaSt = volInfo.getOldestCleanState();
            const MetaSnap cliOldestSnap = oldestMetaSt.snapB;
            if (srvLatestSnap.gidB >= cliOldestSnap.gidB) {
                throw cybozu::Exception(__func__)
                    << "could not execute hash-repl" << srvLatestSnap << cliOldestSnap;
            }
            MetaDiff diff(srvLatestSnap, cliOldestSnap, true, oldestMetaSt.timestamp);
            diff.isCompDiff = true;
            if (!runHashReplClient(volId, volSt, volInfo, dstId, pkt, hostInfo.bulkLb, diff, logger)) {
                return false;
            }
        } else {
            if (hostInfo.dontMerge) {
                if (!runNoMergeDiffReplClient(
                        volId, volSt, volInfo, dstId, pkt, srvLatestSnap, logger)) return false;
            } else {
                if (!runDiffReplClient(
                        volId, volSt, volInfo, dstId, pkt, srvLatestSnap,
                        hostInfo.cmpr, hostInfo.maxWdiffMergeSize, logger)) return false;
            }
        }
        runAtLeastOnce = true;
    }
    packet::Ack(sock).recv();
    if (!runAtLeastOnce) {
        getArchiveGlobal().remoteSnapshotManager.update(volId, dstId, srvLatestState);
    }
    return true;
}


/**
 * ul is locked at the function beginning.
 */
bool runReplSyncServer(const std::string &volId, cybozu::Socket &sock, UniqueLock &ul, Logger &logger)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(sock);
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    cybozu::Uuid archiveUuid;
    pkt.read(archiveUuid);
    bool doResync;
    pkt.read(doResync);

    int kind = DO_HASH_OR_DIFF_SYNC;
    const std::string state = volSt.sm.get();
    if (state == aSyncReady) {
        if (doResync && volInfo.lvExists()) {
            kind = DO_RESYNC;
        } else {
            kind = DO_FULL_SYNC;
        }
    } else {
        // aArchived
        if (volInfo.getArchiveUuid() != archiveUuid || doResync) {
            kind = DO_RESYNC;
        }
    }
    if (!doResync && kind == DO_RESYNC) {
        const char *msg = "resync is required but not allowed";
        pkt.write(msg);
        pkt.flush();
        throw cybozu::Exception(FUNC) << msg << volId;
    }
    pkt.write(msgAccept);
    pkt.write(kind);
    pkt.flush();

    if (kind == DO_FULL_SYNC) {
        if (!runFullReplServer(volId, volSt, volInfo, pkt, archiveUuid, ul, logger)) {
            return false;
        }
    } else if (kind == DO_RESYNC) {
        if (!runResyncReplServer(volId, volSt, volInfo, pkt, ul, logger)) {
            return false;
        }
    } else {
        assert(kind == DO_HASH_OR_DIFF_SYNC);
    }

    for (;;) {
        const MetaState latestMetaSt = volSt.getLatestMetaState();
        pkt.write(latestMetaSt);
        pkt.flush();
        int repl;
        pkt.read(repl);
        if (repl == ArchiveVolInfo::DONT_REPL) break;

        if (repl == ArchiveVolInfo::DO_HASH_REPL) {
            if (!runHashReplServer(volId, volSt, volInfo, pkt, ul, latestMetaSt, logger)) return false;
        } else {
            if (!runDiffReplServer(volId, volSt, volInfo, pkt, ul, latestMetaSt, logger)) return false;
        }
    }
    packet::Ack(sock).sendFin();
    return true;
}


StrVec getAllStatusAsStrVec()
{
    auto fmt = cybozu::util::formatString;
    StrVec v;

    v.push_back("-----ArchiveGlobal-----");
    v.push_back(fmt("nodeId %s", ga.nodeId.c_str()));
    v.push_back(fmt("baseDir %s", ga.baseDirStr.c_str()));
    v.push_back(fmt("volumeGroup %s", ga.volumeGroup.c_str()));
    if (!ga.thinpool.empty()) {
        v.push_back(fmt("thinpool %s", ga.thinpool.c_str()));
    }
    v.push_back(fmt("maxConnections %zu", ga.maxConnections));
    v.push_back(fmt("maxForegroundTasks %zu", ga.maxForegroundTasks));
    v.push_back(fmt("socketTimeout %zu", ga.socketTimeout));
    v.push_back(fmt("keepAlive %s", ga.keepAliveParams.toStr().c_str()));
    v.push_back(fmt("doAutoResize %d", ga.doAutoResize));
    v.push_back(fmt("keepOneColdSnapshot %d", ga.keepOneColdSnapshot));

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
        if (!volSt.lvCache.exists()) {
            s += fmt(" ERROR: lvCache is empty. lv must not exsits.");
            v.push_back(s);
            continue;
        }
        const MetaSnap latestSnap = volSt.getLatestMetaState().snapB;
        s += fmt(" latestSnapshot %s", latestSnap.str().c_str());
        const uint64_t sizeLb = volSt.lvCache.getLv().sizeLb();
        const std::string sizeS = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
        s += fmt(" size %s", sizeS.c_str());

        v.push_back(s);
    }
    return v;
}


StrVec getVolStatusAsStrVec(const std::string &volId)
{
    auto fmt = cybozu::util::formatString;
    StrVec v;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    const std::string &state = volSt.sm.get();
    v.push_back(fmt("hostType archive"));
    v.push_back(fmt("volume %s", volId.c_str()));
    v.push_back(fmt("state %s", state.c_str()));
    if (state == aClear) return v;

    v.push_back(formatActions("action", volSt.ac, allActionVec));
    v.push_back(fmt("stopState %s", stopStateToStr(StopState(volSt.stopState.load()))));
    v.push_back(fmt("lastSyncTime %s"
                    , util::timeToPrintable(volSt.lastSyncTime).c_str()));
    v.push_back(fmt("lastWdiffReceivedTime %s"
                    , util::timeToPrintable(volSt.lastWdiffReceivedTime).c_str()));
    v.push_back(fmt("progressLb %" PRIu64 "", volSt.progressLb.load()));

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    if (!volInfo.lvExists()) {
        v.push_back("baseLv NOT FOUND");
        return v;
    }
    if (!volSt.lvCache.exists()) {
        v.push_back("ERROR: lvCache is empty. lv must not exsits.");
        return v;
    }
    for (std::string& s : volInfo.getStatusAsStrVec()) {
        v.push_back(std::move(s));
    }
    return v;
}


void getApplicableDiffList(protocol::GetCommandParams &p)
{
    const VolIdAndGidParam param = parseVolIdAndGidParam(p.params, 1, false, UINT64_MAX);
    const std::string &volId = param.volId;

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);

    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const MetaState metaSt = volInfo.getMetaState();
    const MetaDiffVec diffV = volSt.diffMgr.getDiffListToApply(metaSt, param.gid);
    StrVec v;
    for (const MetaDiff &diff : diffV) {
        v.push_back(formatMetaDiff("", diff));
    }
    ul.unlock();
    protocol::sendValueAndFin(p, v);
}


void existsDiff(protocol::GetCommandParams &p)
{
    const ExistsDiffParam param = parseExistsDiffParamForGet(p.params);

    ArchiveVolState &volSt = getArchiveVolState(param.volId);
    UniqueLock ul(volSt.mu);
    verifyStateIn(volSt.sm.get(), aActiveOrStopped, __func__);

    const bool s = volSt.diffMgr.exists(param.diff);
    ul.unlock();
    protocol::sendValueAndFin(p, s);
}


void existsBaseImage(protocol::GetCommandParams &p)
{
    const std::string volId = parseVolIdParam(p.params, 1);
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const bool s = volInfo.lvExists();
    ul.unlock();
    protocol::sendValueAndFin(p, s);
}


void getNumAction(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    const NumActionParam param = parseNumActionParamForGet(p.params);
    const std::string &volId = param.volId;
    const std::string &action = param.action;

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


void getAllActions(protocol::GetCommandParams &p)
{
    StrVec v;
    for (const std::string& volId : getVolIdList()) {
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        if (!isStateIn(volSt.sm.get(), aActive)) continue;
        const bool useTime = true;
        v.push_back(formatActions(volId.c_str(), volSt.ac, allActionVec, useTime));
    }
    protocol::sendValueAndFin(p, v);
}


void getSnapshot(protocol::GetCommandParams &p, bool isCold)
{
    const char *const FUNC = __func__;
    const std::string volId = parseVolIdParam(p.params, 1);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    verifyStateIn(volSt.sm.get(), aActiveOrStopped, FUNC);
    const StrVec strV = archive_local::listSnapshot(volId, isCold);
    ul.unlock();
    protocol::sendValueAndFin(p, strV);

    std::string msg = cybozu::util::formatString(
        "get %s succeeded", isCold ? "cold" : "restored");
    p.logger.debug() << msg << volId;
}


void getRestorable(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    const RestorableParam param = parseRestorableParamForGet(p.params);
    const std::string &volId = param.volId;

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    const std::string st = volSt.sm.get();
    StrVec strV;
    if (isStateIn(st, aActive)) {
        strV = archive_local::listRestorable(volId, param.isAll);
    }
    ul.unlock();
    protocol::sendValueAndFin(p, strV);
    p.logger.debug() << "get restorable succeeded" << volId;
}


void getUuidDetail(protocol::GetCommandParams &p, bool isArchive)
{
    const char *const FUNC = __func__;
    const std::string volId = parseVolIdParam(p.params, 1);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (!isStateIn(st, aActive)) {
        throw cybozu::Exception(FUNC) << "bad state" << volId << st;
    }
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const cybozu::Uuid uuid = isArchive ? volInfo.getArchiveUuid() : volInfo.getUuid();
    ul.unlock();
    const std::string uuidStr = uuid.str();
    protocol::sendValueAndFin(p, uuidStr);
    p.logger.debug()
        << (isArchive ? "get archive-uuid succeeded" : "get uuid succeeded")
        << volId << uuidStr;
}


static bool getMetaState(const std::string& volId, MetaState &metaSt, bool throwError, const std::string &errMsg = "")
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (!isStateIn(st, aActive)) {
        if (throwError) {
            throw cybozu::Exception(errMsg) << "bad state" << volId << st;
        }
        return false;
    }
    metaSt  = getArchiveVolInfo(volId).getMetaState();
    return true;
}


void getBase(protocol::GetCommandParams &p)
{
    const std::string volId = parseVolIdParam(p.params, 1);
    MetaState metaSt;
    getMetaState(volId, metaSt, true, __func__);
    const std::string metaStStr = metaSt.strTs();
    protocol::sendValueAndFin(p, metaStStr);
    p.logger.debug() << "get base succeeded" << volId << metaStStr;
}


void getBaseAll(protocol::GetCommandParams &p)
{
    StrVec ret;
    for (const std::string &volId : ga.stMap.getKeyList()) {
        MetaState metaSt;
        if (!getMetaState(volId, metaSt, false)) continue;
        ret.push_back(cybozu::util::formatString(
                          "name:%s\tmetastate:%s"
                          , volId.c_str(), metaSt.strTs().c_str()));
    }
    protocol::sendValueAndFin(p, ret);
    p.logger.debug() << "get base-all succeeded" << ret.size();
}


/**
 * Get block hash to verify block devices.
 * sizeLb: 0 means whole device size.
 */
bool getBlockHash(
    const std::string &volId, uint64_t gid, uint64_t bulkLb, uint64_t sizeLb,
    packet::Packet &pkt, Logger &, cybozu::murmurhash3::Hash &hash)
{
    const char *const FUNC = __func__;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const uint64_t devSizeLb = volSt.lvCache.getLv().sizeLb();
    if (sizeLb == 0) {
        sizeLb = devSizeLb;
    } else if (sizeLb > devSizeLb) {
        throw cybozu::Exception(FUNC) << "Specified device size is too large" << sizeLb << devSizeLb;
    }

    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volSt, volInfo, sizeLb, MetaSnap(gid));

    AlignedArray buf;
    packet::StreamControl ctrl(pkt.sock());
    cybozu::murmurhash3::StreamHasher hasher(0); // seed is 0.
    uint64_t remaining = sizeLb;
    double t0 = cybozu::util::getTime();
    double tx0 = t0;
    while (remaining > 0) {
        if (volSt.stopState == ForceStopping || ga.ps.isForceShutdown()) {
            ctrl.end();
            return false;
        }
        const uint64_t lb = std::min(remaining, bulkLb);
        buf.resize(lb * LOGICAL_BLOCK_SIZE);
        virt.read(buf.data(), buf.size());
        hasher.push(buf.data(), buf.size());
        const double t1 = cybozu::util::getTime();
        if (t1 - t0 > 1.0) { // to avoid timeout.
            ctrl.dummy();
            t0 = t1;
        }
        remaining -= lb;
        const double tx1 = t1;
        if (tx1 - tx0 > PROGRESS_INTERVAL_SEC) {
            LOGs.info() << FUNC << "progress" << sizeLb - remaining;
            tx0 = tx1;
        }
    }
    ctrl.end();
    hash = hasher.get();
    return true;
}


/**
 * Do virtual full scan.
 * sizeLb: 0 means whole device size.
 */
bool virtualFullScanServer(
    const std::string &volId, uint64_t gid, uint64_t bulkLb, uint64_t sizeLb,
    packet::Packet &pkt, Logger &logger)
{
    const char *const FUNC = __func__;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    const uint64_t devSizeLb = volSt.lvCache.getLv().sizeLb();
    if (sizeLb == 0) {
        sizeLb = devSizeLb;
    } else if (sizeLb > devSizeLb) {
        throw cybozu::Exception(FUNC) << "Specified size is too large" << sizeLb << devSizeLb;
    }
    pkt.write(sizeLb);
    pkt.flush();

    VirtualFullScanner virt;
    archive_local::prepareVirtualFullScanner(virt, volSt, volInfo, sizeLb, MetaSnap(gid));

    packet::StreamControl2 ctrl(pkt.sock());
    AlignedArray buf;
    std::string encBuf;
    uint64_t c = 0;
    uint64_t remaining = sizeLb;
    double t0 = cybozu::util::getTime();
    while (remaining > 0) {
        if (volSt.stopState == ForceStopping || ga.ps.isForceShutdown()) {
            ctrl.sendError();
            return false;
        }
        const uint64_t lb = std::min(remaining, bulkLb);
        buf.resize(lb * LOGICAL_BLOCK_SIZE);
        virt.read(buf.data(), buf.size());
        ctrl.sendNext();
        if (cybozu::util::isAllZero(buf.data(), buf.size())) {
            pkt.write(0);
        } else {
            compressSnappy(buf, encBuf);
            pkt.write(encBuf.size());
            pkt.write(encBuf.data(), encBuf.size());
        }
        remaining -= lb;
        c++;
        const double t1 = cybozu::util::getTime();
        if (t1 - t0 > PROGRESS_INTERVAL_SEC) {
            LOGs.info() << FUNC << "progress" << sizeLb - remaining;
            t0 = t1;
        }
    }
    ctrl.sendEnd();
    pkt.flush();
    packet::Ack(pkt.sock()).recv();
    logger.debug() << "number of sent bulks" << c;
    logger.info() << "virt-full-scan sizeLb devSizeLb" << sizeLb << devSizeLb;
    logger.info() << "virt-full-scan-mergeIn " << volId << virt.statIn();
    logger.info() << "virt-full-scan-mergeOut" << volId << virt.statOut();
    logger.info() << "virt-full-scan-mergeMemUsage" << volId << virt.memUsageStr();
    return true;
}


void getVolSize(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    const std::string volId = parseVolIdParam(p.params, 1);

    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
    if (!volInfo.lvExists()) {
        throw cybozu::Exception(FUNC) << "base image does not exist" << volId;
    }
    const uint64_t sizeLb = volSt.lvCache.getLv().sizeLb();
    ul.unlock();
    protocol::sendValueAndFin(p, sizeLb);
    p.logger.debug() << "get vol-size succeeded" << volId << sizeLb;
}


MetaState getMetaStateDetail(const std::string &volId, bool isApplying, uint64_t gid)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    ArchiveVolInfo volInfo = getArchiveVolInfo(volId);

    const MetaState metaSt = volInfo.getMetaState();
    MetaDiffVec diffV;
    if (gid == UINT64_MAX) {
        diffV = volSt.diffMgr.getApplicableDiffList(metaSt.snapB);
    } else {
        diffV = volSt.diffMgr.getApplicableDiffListByGid(metaSt.snapB, gid);
    }
    return isApplying ? beginApplying(metaSt, diffV) : apply(metaSt, diffV);
}


bool getLatestMetaState(const std::string &volId, MetaState &metaSt)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string state = volSt.sm.get();
    if (!isStateIn(state, aActiveOrStopped)) return false;

    metaSt = volSt.getLatestMetaState();
    return true;
}


SnapshotInfo getLatestSnapshotInfo(const std::string &volId)
{
    SnapshotInfo snapInfo;
    snapInfo.init();
    snapInfo.volId = volId;
    MetaState metaSt;
    if (!getLatestMetaState(volId, metaSt)) return snapInfo;
    snapInfo.gid = metaSt.snapB.gidB;
    snapInfo.timestamp = metaSt.timestamp;
    return snapInfo;
}


std::string getLatestSnapForVolume(const std::string& volId)
{
    const SnapshotInfo snapInfo = getLatestSnapshotInfo(volId);
    if (snapInfo.isUnknown()) return "";

    return cybozu::util::formatString(
        "name:%s\t"
        "kind:archive\t"
        "gid:%" PRIu64 "\t"
        "timestamp:%s"
        , snapInfo.volId.c_str()
        , snapInfo.gid
        , cybozu::unixTimeToPrettyStr(snapInfo.timestamp).c_str());
}


void getLatestSnap(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    VolIdOrAllParam param = parseVolIdOrAllParam(p.params, 1);
    StrVec ret;
    if (param.isAll) {
        for (const std::string &volId : ga.stMap.getKeyList()) {
            std::string line = getLatestSnapForVolume(volId);
            if (line.empty()) continue;
            ret.push_back(std::move(line));
        }
    } else {
        std::string line = getLatestSnapForVolume(param.volId);
        if (line.empty()) {
            throw cybozu::Exception(FUNC)
                << "could not get latest snapshot for volume" << param.volId;
        }
        ret.push_back(std::move(line));
    }
    protocol::sendValueAndFin(p, ret);
}


void getTsDelta(protocol::GetCommandParams &p)
{
    StrVec ret;
    const RemoteSnapshotManager::Map map = ga.remoteSnapshotManager.copyMap();
    for (const RemoteSnapshotManager::Map::value_type& pair0 : map) {
        const std::string &volId = pair0.first;
        MetaState srcMetaSt;
        if (!getLatestMetaState(volId, srcMetaSt)) continue;
        for (const RemoteSnapshotManager::InternalMap::value_type &pair1 : pair0.second) {
            const RemoteSnapshotManager::Info &info = pair1.second;
            ret.push_back(generateTsDelta(volId, info.dstId, srcMetaSt, info.metaSt).toStr());
        }
    }
    protocol::sendValueAndFin(p, ret);
    p.logger.debug() << "get ts-delta succeeded";
}

void getHandlerStat(protocol::GetCommandParams &p)
{
    const protocol::HandlerStat stat = getArchiveGlobal().handlerStatMgr.getStatByMove();
    const StrVec ret = prettyPrintHandlerStat(stat);
    protocol::sendValueAndFin(p, ret);
    p.logger.debug() << "get handler-stat succeeded";
}

} // archive_local


void ArchiveVolState::initInner(const std::string& volId)
{
    UniqueLock ul(mu);
    ArchiveVolInfo volInfo(ga.baseDirStr, volId, ga.volumeGroup, ga.thinpool, diffMgr, lvCache);
    if (volInfo.existsVolDir()) {
        const std::string st = volInfo.getState();
        sm.set(st);
        WalbDiffFiles wdiffs(diffMgr, volInfo.volDir.str());
        wdiffs.reload();
        if (isStateIn(st, aActiveOrStopped)) {
            latestMetaSt = volInfo.getLatestState();
        }
    } else {
        sm.set(aClear);
    }
}


void verifyAndRecoverArchiveVol(const std::string& volId)
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

    volInfo.recoverColdToBaseIfNecessary();

    if (!volInfo.lvExists()) {
        throw cybozu::Exception(FUNC) << "base lv must exist" << volId;
    }

    // Check cold snapshots.
    for (VolLvCache::LvMap::value_type& p : volSt.lvCache.getColdMap()) {
        const uint64_t gid = p.first;
        cybozu::lvm::Lv& lv = p.second;
        if (!lv.exists()) {
            throw cybozu::Exception(FUNC)
                << "cold snapshot does not exist" << volId << gid;
        }
        if (!volInfo.existsColdTimestamp(gid)) {
            // It may be possible that the cold snapshot exists but the cold timestamp does not exists.
            // In such cases the cold snapshot is incomplete so it must be deleted.
            LOGs.info() << FUNC << "removing an incomplete cold snapshot" << volId << gid;
            volSt.lvCache.removeCold(gid);
            lv.remove();
        }
    }
}


void gcArchiveVol(const std::string& volId)
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
    const size_t nrTmps = volInfo.gcTmpFiles();
    if (nrDiffs > 0) {
        LOGs.info() << volId << "garbage collected tmp files" << nrTmps;
    }
}


void c2aStatusServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const VolIdOrAllParam param = parseVolIdOrAllParam(protocol::recvStrVec(p.sock, 0, FUNC), 0);
        StrVec strV;
        if (param.isAll) {
            strV = archive_local::getAllStatusAsStrVec();
        } else {
            strV = archive_local::getVolStatusAsStrVec(param.volId);
        }
        protocol::sendValueAndFin(pkt, sendErr, strV);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void c2aInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);

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


void c2aClearVolServer(protocol::ServerParams &p)
{
    const char *FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);
        StateMachine &sm = volSt.sm;
        const std::string &currSt = sm.get(); // aStopped or aSyncReady

        StateMachineTransaction tran(sm, currSt, atClearVol, FUNC);
        ul.unlock();
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        volInfo.clear();
        getArchiveGlobal().remoteSnapshotManager.remove(volId);
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
void c2aStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
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
void c2aStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StopParam param = parseStopParam(protocol::recvStrVec(p.sock, 0, FUNC), false);
        const std::string &volId = param.volId;

        ArchiveVolState &volSt = getArchiveVolState(volId);
        Stopper stopper(volSt.stopState);
        if (!stopper.changeFromNotStopping(param.stopOpt.isForce() ? ForceStopping : Stopping)) {
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
 * Restore command.
 */
void c2aRestoreServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    VolIdAndGidParam param;
    try {
        param = parseVolIdAndGidParam(protocol::recvStrVec(p.sock, 2, FUNC), 0, true, 0);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
        return;
    }
    const std::string &volId = param.volId;
    const uint64_t gid = param.gid;

    ForegroundCounterTransaction foregroundTasksTran;
    ArchiveVolState &volSt = getArchiveVolState(volId);
    UniqueLock ul(volSt.mu);
    try {
        if (volSt.lvCache.hasRestored(gid)) {
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
    logger.info() << "restore started" << volId << gid;
    cybozu::Stopwatch stopwatch;
    if (!archive_local::restore(volId, gid)) {
        logger.warn() << FUNC << "force stopped" << volId << gid;
        return;
    }
    const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
    logger.info() << "restore succeeded" << volId << gid << elapsed;
}


/**
 * !!!CAUSION!!!
 * This is for test and debug.
 */
void c2aReloadMetadataServer(protocol::ServerParams &p)
{
    const char * const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
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


void p2aWdiffTransferServer(protocol::ServerParams &p)
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
        uint32_t maxIoBlocks;
        uint64_t sizeLb;
        MetaDiff diff;

        pkt.read(volId);
        pkt.read(hostType);
        pkt.read(uuid);
        pkt.read(maxIoBlocks); // unused
        pkt.read(sizeLb);
        pkt.read(diff);
        logger.debug() << "recv" << volId << hostType << uuid << sizeLb << diff;

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
                logger.info() << FUNC << "rejected due to" << msg << volId;
                ul.unlock();
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
            logger.info() << FUNC << "rejected due to" << msg << volId;
            ul.unlock();
            pkt.writeFin(msg);
            return;
        }
        if (hostType == proxyHT && volInfo.getUuid() != uuid) {
            const char *msg = msgDifferentUuid;
            logger.info() << FUNC << "rejected due to" << msg << volId;
            ul.unlock();
            pkt.writeFin(msg);
            return;
        }
        archive_local::doAutoResizeIfNecessary(volSt, volInfo, sizeLb);
        const uint64_t selfSizeLb = volSt.lvCache.getLv().sizeLb();
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
        const MetaState latestMetaSt = volSt.getLatestMetaState();
        const MetaSnap& latestSnap = latestMetaSt.snapB;
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
            logger.info() << FUNC << "rejected due to"
                          << msg << volId << latestSnap << diff;
            ul.unlock();
            pkt.writeFin(msg);
            return;
        }
        pkt.write(msgAccept);
        pkt.flush();
        sendErr = false;

        // main procedure
        StateMachineTransaction tran(sm, aArchived, atWdiffRecv, FUNC);
        ul.unlock();
        logger.debug() << "wdiff-transfer started" << volId;
        cybozu::Stopwatch stopwatch;

        const cybozu::FilePath fPath = volInfo.getDiffPath(diff);
        cybozu::TmpFile tmpFile(volInfo.volDir.str());
        cybozu::util::File fileW(tmpFile.fd());
        writeDiffFileHeader(fileW, uuid);
        if (!wdiffTransferServer(pkt, tmpFile.fd(), volSt.stopState, ga.ps, ga.fsyncIntervalSize)) {
            logger.warn() << FUNC << "force stopped" << volId;
            return;
        }
        diff.dataSize = cybozu::FileStat(tmpFile.fd()).size();
        tmpFile.save(fPath.str());

        ul.lock();
        volSt.diffMgr.add(diff);
        volSt.setLatestMetaState(apply(latestMetaSt, diff));
        archive_local::dbgVerifyLatestMetaState(volId);
        tran.commit(aArchived);
        volSt.updateLastWdiffReceivedTime();
        ul.unlock();
        packet::Ack(p.sock).sendFin();
        const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
        logger.debug() << "wdiff-transfer succeeded" << volId << elapsed;
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
void c2aReplicateServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const ReplicateParam param = parseReplicateParam(protocol::recvStrVec(p.sock, 0, FUNC));
        const std::string &volId = param.volId;
        const bool isSize = param.isSize;
        const uint64_t param2 = param.param2;
        const HostInfoForRepl &hostInfo = param.hostInfo;

        ForegroundCounterTransaction foregroundTasksTran;
        ArchiveVolState &volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyActionNotRunning(volSt.ac, aDenyForReplSyncClient, FUNC);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);

        ActionCounterTransaction tran(volSt.ac, aaReplSync);
        ul.unlock();
        cybozu::Socket aSock;
        std::string dstId;
        archive_local::runReplSync1stNegotiation(volId, hostInfo.addrPort, aSock, dstId);
        pkt.writeFin(msgAccept);
        sendErr = false;
        logger.info() << "replication as client started"
                      << volId << param.isSize << param.param2 << hostInfo;
        if (!archive_local::runReplSyncClient(volId, aSock, hostInfo, isSize, param2, dstId, logger)) {
            logger.warn() << FUNC << "replication as client force stopped" << volId << hostInfo;
            return;
        }
        logger.info() << "replication as client succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void a2aReplSyncServer(protocol::ServerParams &p)
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

        pkt.write(msgAccept);
        sendErr = false;

        logger.info() << "replication as server started" << volId;
        cybozu::Stopwatch stopwatch;
        if (!archive_local::runReplSyncServer(volId, p.sock, ul, logger)) {
            logger.warn() << FUNC << "replication as server force stopped" << volId;
            return;
        }
        ul.unlock();
        const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
        logger.info() << "replication as server succeeded" << volId << elapsed;
    } catch (std::exception &e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void c2aApplyServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const VolIdAndGidParam param = parseVolIdAndGidParam(protocol::recvStrVec(p.sock, 2, FUNC), 0, true, 0);
        const std::string &volId = param.volId;
        const uint64_t gid = param.gid;

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
        logger.info() << "apply started" << volId << gid;
        cybozu::Stopwatch stopwatch;
        if (!archive_local::applyDiffsToVolume(volId, gid)) {
            logger.warn() << FUNC << "stopped force" << volId << gid;
            return;
        }
        const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
        logger.info() << "apply succeeded" << volId << gid << elapsed;
    } catch (std::exception& e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void c2aMergeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const MergeParam param = parseMergeParam(protocol::recvStrVec(p.sock, 4, FUNC));
        const std::string& volId = param.volId;
        const uint64_t gidB = param.gidB;
        const bool isSize = param.isSize;
        const char *type = isSize ? "size" : "gid";
        const uint64_t param3 = param.param3; // gidE or maxSizeMb

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
        logger.info() << "merge started" << volId << gidB << type << param3;
        cybozu::Stopwatch stopwatch;
        if (!archive_local::mergeDiffs(volId, gidB, isSize, param3)) {
            logger.warn() << FUNC << "stopped force" << volId << gidB << type << param3;
            return;
        }
        const std::string elapsed = util::getElapsedTimeStr(stopwatch.get());
        logger.info() << "merge succeeded" << volId << gidB << type << param3 << elapsed;
    } catch (std::exception& e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


void c2aResizeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const ResizeParam param = parseResizeParam(protocol::recvStrVec(p.sock, 0, FUNC), true, false);
        const std::string &volId = param.volId;
        const uint64_t newSizeLb = param.newSizeLb;
        const bool doZeroClear = param.doZeroClear;

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
        pkt.write(e.what());
    }
}


void c2aResetVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
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


void changeSnapshot(protocol::ServerParams &p, bool enable)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const ChangeSnapshotParam param = parseChangeSnapshotParam(protocol::recvStrVec(p.sock, 0, FUNC));
        const std::string &volId = param.volId;

        ArchiveVolState& volSt = getArchiveVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        verifyActionNotRunning(volSt.ac, aDenyForChangeSnapshot, FUNC);

        bool failed = false;
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        const std::string msg = (enable ? "enable" : "disable");
        for (const uint64_t gid : param.gidL) {
            MetaDiffVec diffV;
            if (!volSt.diffMgr.changeSnapshot(gid, enable, diffV)) failed = true;
            if (!volInfo.changeSnapshot(diffV, enable)) failed = true;
            logger.info() << msg + " snapshot " + (failed ? "failed" : "succeeded")
                          << volId << gid;
        }
        if (failed) {
#if 0
            // reload metadata.
            WalbDiffFiles wdiffs(volSt.diffMgr, volInfo.volDir.str());
            wdiffs.reload();
#else
            throw cybozu::Exception(FUNC) << "change snapshot failed" << volId << enable;
#endif
        }
        ul.unlock(); // There is no aaChangeSnapshot action so we held lock during the operation.
        pkt.writeFin(msgOk);
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}


void c2aVirtualFullScan(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);
    bool sendErr = true;

    try {
        const VirtualFullScanParam param = parseVirtualFullScanParam(protocol::recvStrVec(p.sock, 0, FUNC));
        const std::string &volId = param.volId;
        const uint64_t gid = param.gid;
        const uint64_t bulkLb = param.bulkLb;
        const uint64_t sizeLb = param.sizeLb;

        ForegroundCounterTransaction foregroundTasksTran;
        verifyMaxForegroundTasks(ga.maxForegroundTasks, FUNC);
        ArchiveVolState &volSt = getArchiveVolState(volId);
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        pkt.write(msgAccept);
        pkt.flush();
        sendErr = false;

        if (!archive_local::virtualFullScanServer(volId, gid, bulkLb, sizeLb, pkt, logger)) {
            throw cybozu::Exception(FUNC) << "force stopped" << volId;
        }
        pkt.writeFin(msgOk);
        logger.debug() << "virtual-full-scan succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * This is synchronous command.
 * The socket timeout is a concern.
 * Currently, this command can be used for test only.
 */
void c2aBlockHashServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);
    bool sendErr = true;

    try {
        const VirtualFullScanParam param = parseVirtualFullScanParam(protocol::recvStrVec(p.sock, 0, FUNC));
        const std::string &volId = param.volId;
        const uint64_t gid = param.gid;
        const uint64_t bulkLb = param.bulkLb;
        const uint64_t sizeLb = param.sizeLb;

        ArchiveVolState &volSt = getArchiveVolState(volId);
        // This does not lock volSt.
        verifyStateIn(volSt.sm.get(), aActive, FUNC);
        pkt.write(msgAccept);
        pkt.flush();

        cybozu::murmurhash3::Hash hash;
        if (!archive_local::getBlockHash(volId, gid, bulkLb, sizeLb, pkt, logger, hash)) {
            throw cybozu::Exception(FUNC) << "force stopped" << volId;
        }
        pkt.write(msgOk);
        sendErr = false;
        pkt.writeFin(hash);
        logger.debug() << "bhash succeeded" << volId << hash;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}


/**
 * This is dangerous. Use for debug purpose.
 */
void c2aSetUuidServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const SetUuidParam param = parseSetUuidParam(protocol::recvStrVec(p.sock, 2, FUNC));
        const std::string &volId = param.volId;
        const cybozu::Uuid &uuid = param.uuid;

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
void c2aSetStateServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const SetStateParam param = parseSetStateParam(protocol::recvStrVec(p.sock, 2, FUNC));
        const std::string &volId = param.volId;
        const std::string &state = param.state;

        verifyStateIn(param.state, aSteadyStates, FUNC);
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
void c2aSetBaseServer(protocol::ServerParams &p)
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


void c2aGarbageCollectDiffServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
        ArchiveVolState &volSt = getArchiveVolState(volId);
        ArchiveVolInfo volInfo = getArchiveVolInfo(volId);
        UniqueLock ul(volSt.mu);
        verifyStateIn(volSt.sm.get(), aActiveOrStopped, FUNC);
        verifyActionNotRunning(volSt.ac, allActionVec, FUNC);

        // Do not unlock volSt due to this command will not change the state.
        const size_t num = volInfo.gcDiffs();

        ul.unlock();
        pkt.writeFin(msgOk);
        logger.info() << "gc-diff succeeded" << volId << num;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}


void s2aGatherLatestSnapServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);
    bool sendErr = true;

    try {
        std::string msg;
        pkt.read(msg);
        if (msg != msgOk) {
            logger.info() << FUNC << "client failed" << msg;
            return;
        }

        const StrVec volIdV = protocol::recvStrVec(p.sock, 0, FUNC);
        std::vector<SnapshotInfo> snapInfoV;

        for (const std::string &volId : volIdV) {
            snapInfoV.push_back(archive_local::getLatestSnapshotInfo(volId));
        }
        pkt.write(msgAccept);
        sendErr = false;
        pkt.write(snapInfoV);
        pkt.writeFin(msgOk);
        logger.debug() << "gather-latest-snap succeeded";
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

#ifndef NDEBUG
void c2aDebugServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    unusedVar(FUNC);
    ProtocolLogger logger(ga.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        pkt.writeFin(msgOk);
        /* debug code from here. */



    } catch (std::exception &e) {
        logger.error() << e.what();
    }
}
#endif

} // namespace walb
