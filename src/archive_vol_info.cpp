#include "archive_vol_info.hpp"
#include "command_param_parser.hpp"

namespace walb {


bool parseBaseLvName(const std::string &name, std::string &volId)
{
    if (!cybozu::util::hasPrefix(name, BASE_VOLUME_PREFIX)) return false;
    if (cybozu::util::hasSuffix(name, TMP_VOLUME_SUFFIX)) return false;
    volId = cybozu::util::removePrefix(name, BASE_VOLUME_PREFIX);
    return isVolIdFormat(volId);
}


bool isBaseLvName(const std::string &name)
{
    std::string volId; // unused
    return parseBaseLvName(name, volId);
}


bool parseVolIdUnderscoreGid(const std::string &s, std::string &volId, uint64_t &gid)
{
    const size_t n = s.rfind('_');
    if (n == std::string::npos) return false;
    volId = s.substr(0, n);
    if (!isVolIdFormat(volId)) return false;
    try {
        gid = cybozu::atoi(&s[n + 1]);
    } catch (std::exception &e) {
        LOGs.error() << __func__ << "atoi failed" << volId << e.what();
        return false;
    }
    return true;
}


bool parseTmpColdToBaseLvName(const std::string &name, std::string &volId, uint64_t &gid)
{
    if (!cybozu::util::hasPrefix(name, BASE_VOLUME_PREFIX)) return false;
    std::string tmp;
    tmp = cybozu::util::removePrefix(name, BASE_VOLUME_PREFIX);
    if (!cybozu::util::hasSuffix(name, TMP_VOLUME_SUFFIX)) return false;
    tmp = cybozu::util::removeSuffix(tmp, TMP_VOLUME_SUFFIX);
    return parseVolIdUnderscoreGid(tmp, volId, gid);
}


bool isTmpColdToBaseLvName(const std::string &name)
{
    std::string volId; // unused
    uint64_t gid; // unused
    return parseTmpColdToBaseLvName(name, volId, gid);
}


bool parseSnapLvName(const std::string &name, std::string &volId, uint64_t &gid, bool isCold, bool isTmp)
{
    if (isCold && isTmp) {
        throw cybozu::Exception("isCold and isTmp must not be true at the same time.");
    }
    const std::string &prefix = isCold ? COLD_VOLUME_PREFIX : RESTORED_VOLUME_PREFIX;
    if (!cybozu::util::hasPrefix(name, prefix)) return false;
    std::string tmp = cybozu::util::removePrefix(name, prefix);
    const bool hasTmpSuffix = cybozu::util::hasSuffix(tmp, TMP_VOLUME_SUFFIX);
    if (isTmp) {
        if (!hasTmpSuffix) return false;
        tmp = cybozu::util::removeSuffix(tmp, TMP_VOLUME_SUFFIX);
    } else {
        if (hasTmpSuffix) return false;
    }
    return parseVolIdUnderscoreGid(tmp, volId, gid);
}


bool isColdLvName(const std::string &name)
{
    std::string volId;
    uint64_t gid;
    return parseSnapLvName(name, volId, gid, true, false);
}


bool isRestoredLvName(const std::string &name)
{
    std::string volId;
    uint64_t gid;
    return parseSnapLvName(name, volId, gid, false, false);
}


bool isTmpRestoredLvName(const std::string &name)
{
    std::string volId;
    uint64_t gid;
    return parseSnapLvName(name, volId, gid, false, true);
}


bool VolLvCache::searchColdNoGreaterThanGid(uint64_t gid, uint64_t &coldGid) const
{
    UniqueLock lk(*muPtr_);
    if (coldMap_.empty()) return false;
    LvMap::const_iterator it = coldMap_.upper_bound(gid);
    if (it == coldMap_.cbegin()) return false;
    --it;
    coldGid = it->first;
    assert(coldGid <= gid);
    return true; // found.
}


VolLvCache::Lv VolLvCache::getSnap(uint64_t gid, bool isCold) const
{
    UniqueLock lk(*muPtr_);
    const LvMap &map = getMap(isCold);
    LvMap::const_iterator it = map.find(gid);
    if (it == map.cend()) {
        throw cybozu::Exception(__func__)
            << "snapshot not found" << lv_ << gid << isCold;
    }
    return it->second;
}

std::vector<uint64_t> VolLvCache::getSnapGidList(bool isCold) const
{
    std::vector<uint64_t> ret;
    {
        UniqueLock lk(*muPtr_);
        for (const LvMap::value_type &p : getMap(isCold)) {
            ret.push_back(p.first);
        }
    }
    /* sorted */
    return ret;
}

void VolLvCache::resizeSnap(uint64_t gid, uint64_t newSizeLb, bool isCold)
{
    UniqueLock lk(*muPtr_);
    LvMap &map = getMap(isCold);
    LvMap::iterator it = map.find(gid);
    if (it == map.end()) {
        throw cybozu::Exception(__func__)
            << "snapshot not found" << lv_ << gid << isCold;
    }
    Lv& lv = it->second;
    lv.setSizeLb(newSizeLb);
}


VolLvCache& insertVolLvCacheMapIfNotFound(VolLvCacheMap &map, const std::string &volId)
{
    VolLvCacheMap::iterator it = map.find(volId);
    if (it == map.end()) {
        bool inserted;
        std::tie(it, inserted) = map.emplace(volId, VolLvCache());
        assert(inserted);
    }
    return it->second;
}


VolLvCacheMap getVolLvCacheMap(
    const cybozu::lvm::LvList &lvL, const std::string &tpName, const StrVec &volIdV)
{
    VolLvCacheMap m1;
    // Base images.
    for (const cybozu::lvm::Lv &lv : lvL) {
        if (tpName.empty() && lv.isTv()) continue;
        if (!tpName.empty() && lv.tpName() != tpName) continue;
        std::string volId;
        if (!parseBaseLvName(lv.name(), volId)) continue;
        LOGs.info() << "FOUND BASE IMAGE" << lv.name();
        VolLvCache &lvC = insertVolLvCacheMapIfNotFound(m1, volId);
        lvC.add(lv);
    }
    // Restored/cold snapshots.
    for (const cybozu::lvm::Lv &lv : lvL) {
        if (tpName.empty() && lv.isTv()) continue;
        if (!tpName.empty() && lv.tpName() != tpName) continue;
        const bool isRestored = isRestoredLvName(lv.name());
        const bool isCold = isColdLvName(lv.name());
        if (!isRestored && !isCold) continue;
        std::string volId;
        uint64_t gid;
        parseSnapLvName(lv.name(), volId, gid, isCold, false);
        VolLvCache &lvC = insertVolLvCacheMapIfNotFound(m1, volId);
        if (isCold) {
            LOGs.info() << "FOUND COLD SNAPSHOT" << lv.name();
            lvC.addCold(gid, lv);
        } else {
            LOGs.info() << "FOUND RESTORED SNAPSHOT" << lv.name();
            lvC.addRestored(gid, lv);
        }
    }
    // TmpColdToBase images.
    LvMap tmpColdToBaseLvMap;
    for (const cybozu::lvm::Lv &lv : lvL) {
        std::string volId;
        uint64_t gid;
        if (parseTmpColdToBaseLvName(lv.name(), volId, gid)) {
            tmpColdToBaseLvMap.emplace(volId, lv);
        }
    }
    // Consider volumes with clear state.
    VolLvCacheMap m2;
    for (const std::string &volId : volIdV) {
        VolLvCacheMap::iterator it = m1.find(volId);
        VolLvCacheMap::iterator it2;
        bool inserted;
        if (it == m1.end()) {
            std::tie(it2, inserted) = m2.emplace(volId, VolLvCache());
        } else {
            VolLvCache &lvC = it->second;
            std::tie(it2, inserted) = m2.emplace(volId, std::move(lvC));
        }
        assert(inserted);
        LvMap::iterator it3 = tmpColdToBaseLvMap.find(volId);
        if (it3 != tmpColdToBaseLvMap.end()) {
            it2->second.setTmpColdToBaseLv(it3->second);
        }
    }
    return m2;
}


void ArchiveVolInfo::init()
{
    util::makeDir(volDir.str(), "ArchiveVolInfo::init", true);
    cybozu::Uuid uuid;
    setUuid(uuid);
    setArchiveUuid(uuid);
    setMetaState(MetaState());
    setState(aSyncReady);
}


void ArchiveVolInfo::clearAllSnapLv()
{
    {
        VolLvCache::LvMap snapM = lvC_.getRestoredMap();
        for (VolLvCache::LvMap::value_type &p : snapM) {
            const uint64_t gid = p.first;
            cybozu::lvm::Lv &snap = p.second;
            snap.remove();
            lvC_.removeRestored(gid);
        }
    }
    {
        VolLvCache::LvMap snapM = lvC_.getColdMap();
        for (VolLvCache::LvMap::value_type &p : snapM) {
            const uint64_t gid = p.first;
            cybozu::lvm::Lv &snap = p.second;
            snap.remove();
            lvC_.removeCold(gid);
        }
    }
    removeColdTimestampFilesBeforeGid(UINT64_MAX); // all
}


void ArchiveVolInfo::clear()
{
    // Delete all related lvm volumes and snapshots.
    if (lvExists()) {
        clearAllSnapLv();
        cybozu::lvm::Lv lv = lvC_.getLv();
        lv.remove();
        lvC_.remove();
    }
    wdiffs_.clearDir();
#if 0 // wdiffs_.clearDir() includes the following operation.
    if (!volDir.rmdirRecursive()) {
        throw cybozu::Exception("ArchiveVolInfo::clear:rmdir recursively failed.");
    }
#endif
}


void ArchiveVolInfo::removeBeforeGid(uint64_t gid)
{
    wdiffs_.removeBeforeGid(gid);
    removeColdTimestampFilesBeforeGid(gid);

    VolLvCache::LvMap coldM = lvC_.getColdMap();
    for (VolLvCache::LvMap::iterator it = coldM.begin(); it != coldM.end(); ++it) {
        uint64_t coldGid = it->first;
        if (coldGid >= gid) break;
        cybozu::lvm::Lv coldLv = it->second;
        try {
            coldLv.remove();
        } catch (std::exception &e) {
            LOGs.error() << __func__ << "remove snapshot failed" << coldLv << e.what();
        }
        lvC_.removeCold(coldGid);
    }
}



MetaState ArchiveVolInfo::getMetaStateForDetail(uint64_t gid, bool &useCold, bool isApply) const
{
    MetaState st = getMetaState();
    useCold = false;
    uint64_t coldGid = 0;
    if (isApply && st.isApplying) return st;
    if (!lvC_.searchColdNoGreaterThanGid(gid, coldGid)) return st;
    if (coldGid <= st.snapB.gidB) return st;
    useCold = true;
    return MetaState(MetaSnap(coldGid), getColdTimestamp(coldGid));
}


void ArchiveVolInfo::setState(const std::string& newState)
{
    const char *tbl[] = {
        aSyncReady, aArchived, aStopped,
    };
    for (const char *p : tbl) {
        if (newState == p) {
            util::saveFile(volDir, "state", newState);
            return;
        }
    }
    throw cybozu::Exception("ArchiveVolInfo::setState:bad state") << newState;
}


uint64_t ArchiveVolInfo::initFullReplResume(
    uint64_t sizeLb, const cybozu::Uuid& archiveUuid, const MetaState& metaSt, FullReplState& fullReplSt)
{
    uint64_t startLb;
    if (getFullReplState(fullReplSt) && archiveUuid == getArchiveUuid()
        && metaSt == fullReplSt.metaSt && fullReplSt.progressLb <= sizeLb) {
        // resume.
        startLb = fullReplSt.progressLb;
    } else {
        // restart.
        startLb = 0;
        fullReplSt.progressLb = 0;
        fullReplSt.metaSt = metaSt;
    }
    fullReplSt.timestamp = ::time(0);
    return startLb;
}


void ArchiveVolInfo::createLv(uint64_t sizeLb)
{
    if (sizeLb == 0) {
        throw cybozu::Exception("ArchiveVolInfo::createLv:sizeLb is zero");
    }
    if (lvExists()) {
        cybozu::lvm::Lv lv;
        if (lvC_.exists()) {
            lv = lvC_.getLv();
        } else {
            lv = cybozu::lvm::locate(getVg().name(), lvName());
            lvC_.add(lv);
        }
        uint64_t curSizeLb = lv.sizeLb();
        if (curSizeLb != sizeLb) {
            throw cybozu::Exception("ArchiveVolInfo::createLv:sizeLb is different") << curSizeLb << sizeLb;
        }
        if (isThinProvisioning()) {
            // Deallocate all the area to execute efficient full backup/replication.
            cybozu::util::File file(lv.path().str(), O_RDWR | O_DIRECT);
            cybozu::util::issueDiscard(file.fd(), 0, curSizeLb);
            file.fdatasync();
        }
        return;
    }
    cybozu::lvm::Lv lv;
    if (isThinProvisioning()) {
        lv = getVg().createTv(thinpool, lvName(), sizeLb);
    } else {
        lv = getVg().createLv(lvName(), sizeLb);
    }
    lvC_.add(lv);
}


void ArchiveVolInfo::growLv(uint64_t newSizeLb, bool doZeroClear)
{
    cybozu::lvm::Lv lv = lvC_.getLv();
    const uint64_t oldSizeLb = lv.sizeLb();
    if (oldSizeLb == newSizeLb) {
        /* no need to grow. */
        return;
    }
    const std::string lvPathStr = lv.path().str();
    if (newSizeLb < oldSizeLb) {
        /* Shrink is not supported. */
        throw cybozu::Exception(
            "You tried to shrink the volume: " + lvPathStr);
    }
    lv.resize(newSizeLb);
    lvC_.resize(lv.sizeLb()); // newSizeLb =< lv.sizeLb().
    device::flushBufferCache(lvPathStr);
    if (doZeroClear) {
        cybozu::util::File f(lvPathStr, O_RDWR | O_DIRECT);
        cybozu::aio::zeroClear(f.fd(), oldSizeLb, newSizeLb - oldSizeLb);
        f.fdatasync();
    }
}

StrVec ArchiveVolInfo::getStatusAsStrVec() const
{
    const char *const FUNC = __func__;
    StrVec v;
    auto &fmt = cybozu::util::formatString;

    if (!existsVolDir()) {
        throw cybozu::Exception(FUNC) << "not found volDir" << volDir.str();
    }
    if (!lvExists()) {
        throw cybozu::Exception(FUNC) << "not found baseLv" << vgName << lvName();
    }

    const uint64_t sizeLb = lvC_.getLv().sizeLb();
    v.push_back(fmt("sizeLb %" PRIu64, sizeLb));
    const std::string sizeS = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
    v.push_back(fmt("size %s", sizeS.c_str()));
    const cybozu::Uuid uuid = getUuid();
    v.push_back(fmt("uuid %s", uuid.str().c_str()));
    const cybozu::Uuid archiveUuid = getArchiveUuid();
    v.push_back(fmt("archiveUuid %s", archiveUuid.str().c_str()));
    const MetaState metaSt = getMetaState();
    v.push_back(fmt("base %s", metaSt.str().c_str()));
    const MetaSnap latest = wdiffs_.getMgr().getLatestSnapshot(metaSt);
    v.push_back(fmt("latest %s", latest.str().c_str()));
    const size_t numCold = lvC_.getNrCold();
    v.push_back(fmt("numCold %zu", numCold));
    const size_t numRestored = lvC_.getNrRestored();
    v.push_back(fmt("numRestored %zu", numRestored));
    const size_t numRestorable = getRestorableSnapshots(false).size();
    v.push_back(fmt("numRestorable %zu", numRestorable));
    const size_t numRestorableAll = getRestorableSnapshots(true).size();
    v.push_back(fmt("numRestorableAll %zu", numRestorableAll));

    MetaDiffVec dv = wdiffs_.getMgr().getAll();
    v.push_back(fmt("numDiff %zu", dv.size()));
    uint64_t totalSize = 0;
    for (const MetaDiff &d : dv) totalSize += d.dataSize;
    v.push_back(fmt("wdiffTotalSize %" PRIu64 "", totalSize));

    FullReplState fullReplSt;
    if (getFullReplState(fullReplSt)) {
        v.push_back(fmt("fullReplState %s", fullReplSt.str().c_str()));
    } else {
        v.push_back(fmt("fullReplState None"));
    }
    return v;
}


bool ArchiveVolInfo::changeSnapshot(const MetaDiffVec& diffV, bool enable)
{
    bool success = true;
    for (MetaDiff diff : diffV) {
        assert(diff.isMergeable != enable);
        cybozu::FilePath to = getDiffPath(diff);
        diff.isMergeable = enable;
        cybozu::FilePath from = getDiffPath(diff);
        if (!from.stat().exists()) {
            LOGs.warn() << "ArchiveVolInfo::changeSnapshot: not found" << from;
            success = false;
            continue;
        }
        if (!from.rename(to)) {
            LOGs.warn() << "ArchiveVolInfo""changeSnapshot: rename failed" << from << to;
            success = false;
        }
    }
    return success;
}


MetaDiffVec ArchiveVolInfo::getDiffListToMergeGid(uint64_t gidB, uint64_t gidE) const
{
    MetaDiffVec v = getDiffMgr().getMergeableDiffList(gidB);
    for (size_t i = 0; i < v.size(); i++) {
        if (v[i].snapB.gidB >= gidE) {
            v.resize(i);
            break;
        }
    }
    return v;
}


int ArchiveVolInfo::shouldDoRepl(const MetaSnap &srvSnap, const MetaSnap &cliSnap, bool isSize, uint64_t param) const
{
    if (srvSnap.gidB >= cliSnap.gidB) return DONT_REPL;
    const MetaDiffVec diffV = getDiffMgr().getDiffListToSync(MetaState(srvSnap, 0), cliSnap);
    if (diffV.empty()) return DO_HASH_REPL;

    if (isSize) {
        const uint64_t minSizeB = param * MEBI;
        if (minSizeB == 0) return DO_DIFF_REPL;
        uint64_t totalB = 0;
        for (const MetaDiff &diff : diffV) {
            totalB += diff.dataSize;
            if (totalB > minSizeB) return DO_DIFF_REPL;
        }
        return DONT_REPL;
    } else {
        const uint64_t gid = param;
        return gid <= diffV[0].snapB.gidB ? DONT_REPL : DO_DIFF_REPL;
    }
}


void ArchiveVolInfo::makeColdToBase(uint64_t gid)
{
    cybozu::lvm::Lv lv = lvC_.getLv();
    cybozu::lvm::Lv coldLv = lvC_.getCold(gid);
    MetaState coldSt(MetaSnap(gid), getColdTimestamp(gid));

    const std::string tmpLvName = coldToBaseLvName(gid);
    if (cybozu::lvm::existsFile(vgName, tmpLvName)) {
        cybozu::lvm::remove(cybozu::lvm::getLvStr(vgName, tmpLvName));
    }
    cybozu::lvm::Lv tmpLv = cybozu::lvm::renameLv(vgName, coldLv.name(), tmpLvName);
    lvC_.removeCold(gid);
    lvC_.setTmpColdToBaseLv(tmpLv);
    cybozu::lvm::setPermission(tmpLv.lvStr(), true);
    if (tmpLv.sizeLb() < lv.sizeLb()) {
        tmpLv.resize(lv.sizeLb());
    }
    lv.remove();
    /* CAN NOT ROLLBACK FROM NOW. */
    setMetaState(coldSt);
    cybozu::lvm::renameLv(vgName, tmpLvName, lv.name());
    removeColdTimestamp(gid);
    lvC_.removeTmpColdToBaseLv();
    removeBeforeGid(gid);
}


void ArchiveVolInfo::recoverColdToBaseIfNecessary()
{
    cybozu::lvm::Lv tmpLv = lvC_.getTmpColdToBaseLv();
    uint64_t coldGid = UINT64_MAX;
    if (tmpLv.exists()) {
        std::string volId2;
        parseTmpColdToBaseLvName(tmpLv.name(), volId2, coldGid);
        assert(volId == volId2);
        assert(coldGid != UINT64_MAX);
    }
    MetaState st0 = getMetaState();
    const uint64_t baseGid = st0.snapB.gidB;

    if (lvExists()) {
        if (!tmpLv.exists()) return;
        if (coldGid <= baseGid) {
            /* Just remove the temporary lv. */
            tmpLv.remove();
            removeColdTimestamp(coldGid);
        } else {
            /* TmpColdToBase image will be renamed
               to the cold snapshot. */
            const std::string coldLvName = coldSnapshotName(coldGid);
            cybozu::lvm::renameLv(vgName, tmpLv.name(), coldLvName);
        }
        lvC_.removeTmpColdToBaseLv();
        return;
    }
    if (!tmpLv.exists()) {
        throw cybozu::Exception("recoverColdToBase") << "not found" << tmpLv.name();
    }
    assert(tmpLv.exists());
    MetaState coldSt(MetaSnap(coldGid), getColdTimestamp(coldGid));
    setMetaState(coldSt);
    cybozu::lvm::Lv baseLv = cybozu::lvm::renameLv(vgName, tmpLv.name(), lvName());
    lvC_.add(baseLv);
    removeColdTimestamp(coldGid);
    lvC_.removeTmpColdToBaseLv();
    removeBeforeGid(coldGid);
}

} // namespace walb
