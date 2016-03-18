#pragma once
/**
 * @file
 * @brief Archive volume information.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <map>
#include <memory>
#include <sstream>
#include "cybozu/file.hpp"
#include "cybozu/atoi.hpp"
#include "lvm.hpp"
#include "fileio.hpp"
#include "aio_util.hpp"
#include "file_path.hpp"
#include "wdiff_data.hpp"
#include "meta.hpp"
#include "walb_diff_merge.hpp"
#include "wdev_util.hpp"
#include "archive_constant.hpp"
#include "random.hpp"
#include "full_repl_state.hpp"

namespace walb {

inline bool parseBaseLvName(const std::string &name, std::string &volId)
{
    if (!cybozu::util::hasPrefix(name, BASE_VOLUME_PREFIX)) return false;
    if (cybozu::util::hasSuffix(name, TMP_VOLUME_SUFFIX)) return false;
    volId = cybozu::util::removePrefix(name, BASE_VOLUME_PREFIX);
    return isVolIdFormat(volId);
}

inline bool isBaseLvName(const std::string &name)
{
    std::string volId; // unused
    return parseBaseLvName(name, volId);
}

/**
 * s: {volId}_{gid} format.
 */
inline bool parseVolIdUnderscoreGid(const std::string &s, std::string &volId, uint64_t &gid)
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

inline bool parseTmpColdToBaseLvName(const std::string &name, std::string &volId, uint64_t &gid)
{
    if (!cybozu::util::hasPrefix(name, BASE_VOLUME_PREFIX)) return false;
    std::string tmp;
    tmp = cybozu::util::removePrefix(name, BASE_VOLUME_PREFIX);
    if (!cybozu::util::hasSuffix(name, TMP_VOLUME_SUFFIX)) return false;
    tmp = cybozu::util::removeSuffix(tmp, TMP_VOLUME_SUFFIX);
    return parseVolIdUnderscoreGid(tmp, volId, gid);
}

inline bool isTmpColdToBaseLvName(const std::string &name)
{
    std::string volId; // unused
    uint64_t gid; // unused
    return parseTmpColdToBaseLvName(name, volId, gid);
}

inline bool parseSnapLvName(const std::string &name, std::string &volId, uint64_t &gid, bool isCold, bool isTmp)
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

inline bool isColdLvName(const std::string &name)
{
    std::string volId;
    uint64_t gid;
    return parseSnapLvName(name, volId, gid, true, false);
}

inline bool isRestoredLvName(const std::string &name)
{
    std::string volId;
    uint64_t gid;
    return parseSnapLvName(name, volId, gid, false, false);
}

inline bool isTmpRestoredLvName(const std::string &name)
{
    std::string volId;
    uint64_t gid;
    return parseSnapLvName(name, volId, gid, false, true);
}

/**
 * This is movable.
 * Almost functions are thread-safe except for constructors and move assigner.
 */
class VolLvCache
{
public:
    using Lv = cybozu::lvm::Lv;
    using LvMap = std::map<uint64_t, Lv>; // key: gid, value: snapshot.
    using MuPtr = std::unique_ptr<std::recursive_mutex>;

private:
    mutable MuPtr muPtr_;
    bool exists_;
    Lv lv_;
    LvMap restoredMap_; // restored volume.
    LvMap coldMap_; // cold volume.

    Lv tmpColdToBaseLv_; /* This is used at startup time only. */

public:
    VolLvCache()
        : muPtr_(new std::recursive_mutex()), exists_(false), lv_()
        , restoredMap_(), coldMap_(), tmpColdToBaseLv_() {
    }
    bool exists() const {
        UniqueLock lk(*muPtr_);
        return exists_;
    }
    Lv getLv() const {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        return lv_;
    }
    void add(const Lv &lv) {
        UniqueLock lk(*muPtr_);
        lv_ = lv;
        exists_ = true;
    }
    void remove() {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        restoredMap_.clear();
        coldMap_.clear();
        lv_ = Lv();
        exists_ = false;
    }
    void resize(uint64_t newSizeLb) {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        lv_.setSizeLb(newSizeLb);
    }
    bool hasRestored(uint64_t gid) const { return hasSnap(gid, false); }
    bool hasCold(uint64_t gid) const { return hasSnap(gid, true); }
    Lv getRestored(uint64_t gid) const { return getSnap(gid, false); }
    Lv getCold(uint64_t gid) const { return getSnap(gid, true); }
    void resizeCold(uint64_t gid, uint64_t newSizeLb) { resizeSnap(gid, newSizeLb, true); }
    size_t getNrRestored() const { return getNrSnap(false); }
    size_t getNrCold() const { return getNrSnap(true); }
    std::vector<uint64_t> getRestoredGidList() const { return getSnapGidList(false); }
    std::vector<uint64_t> getColdGidList() const { return getSnapGidList(true); }
    LvMap getRestoredMap() const { return getSnapMap(false); }
    LvMap getColdMap() const { return getSnapMap(true); }
    void addRestored(uint64_t gid, const Lv &snap) { addSnap(gid, snap, false); }
    void addCold(uint64_t gid, const Lv &snap) { addSnap(gid, snap, true); }
    void removeRestored(uint64_t gid) { removeSnap(gid, false); }
    void removeCold(uint64_t gid) { removeSnap(gid, true); }

    bool searchColdNoGreaterThanGid(uint64_t gid, uint64_t &coldGid) const {
        UniqueLock lk(*muPtr_);
        if (coldMap_.empty()) return false;
        LvMap::const_iterator it = coldMap_.upper_bound(gid);
        if (it == coldMap_.cbegin()) return false;
        --it;
        coldGid = it->first;
        assert(coldGid <= gid);
        return true; // found.
    }
    void setTmpColdToBaseLv(Lv lv) {
        UniqueLock lk(*muPtr_);
        tmpColdToBaseLv_ = lv;
    }
    Lv getTmpColdToBaseLv() const {
        UniqueLock lk(*muPtr_);
        return tmpColdToBaseLv_;
    }
    void removeTmpColdToBaseLv() {
        UniqueLock lk(*muPtr_);
        tmpColdToBaseLv_ = Lv();
    }
private:
    bool hasSnap(uint64_t gid, bool isCold) const {
        UniqueLock lk(*muPtr_);
        const LvMap &map = getMap(isCold);
        return map.find(gid) != map.cend();
    }
    Lv getSnap(uint64_t gid, bool isCold) const {
        UniqueLock lk(*muPtr_);
        const LvMap &map = getMap(isCold);
        LvMap::const_iterator it = map.find(gid);
        if (it == map.cend()) {
            throw cybozu::Exception(__func__)
                << "snapshot not found" << lv_ << gid << isCold;
        }
        return it->second;
    }
    size_t getNrSnap(bool isCold) const {
        UniqueLock lk(*muPtr_);
        return getMap(isCold).size();
    }
    std::vector<uint64_t> getSnapGidList(bool isCold) const {
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
    LvMap getSnapMap(bool isCold) const {
        UniqueLock lk(*muPtr_);
        return getMap(isCold); /* copy */
    }
    void addSnap(uint64_t gid, const Lv &snap, bool isCold) {
        UniqueLock lk(*muPtr_);
        getMap(isCold).emplace(gid, snap);
    }
    void removeSnap(uint64_t gid, bool isCold) {
        UniqueLock lk(*muPtr_);
        getMap(isCold).erase(gid);
    }
    void resizeSnap(uint64_t gid, uint64_t newSizeLb, bool isCold) {
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
    void verifyExistance() const {
        if (!exists_) {
            throw cybozu::Exception("VolLvCache:lv does not existance in memory");
        }
    }
    LvMap& getMap(bool isCold) {
        return isCold ? coldMap_ : restoredMap_;
    }
    const LvMap& getMap(bool isCold) const {
        return isCold ? coldMap_ : restoredMap_;
    }
};

using VolLvCacheMap = std::map<std::string, VolLvCache>;
using LvMap = std::map<std::string, cybozu::lvm::Lv>;

inline VolLvCache& insertVolLvCacheMapIfNotFound(VolLvCacheMap &map, const std::string &volId)
{
    VolLvCacheMap::iterator it = map.find(volId);
    if (it == map.end()) {
        bool inserted;
        std::tie(it, inserted) = map.emplace(volId, VolLvCache());
        assert(inserted);
    }
    return it->second;
}

/**
 * F is bool (*)(const std::string&) type.
 */
template <typename F>
inline size_t removeSnapshotIf(const cybozu::lvm::LvList &lvL, F cond)
{
    size_t nr = 0;
    for (cybozu::lvm::Lv lv : lvL) { // copy
        if (cond(lv.name())) {
            try {
                lv.remove();
                nr++;
            } catch (std::exception &e) {
                LOGs.error() << __func__ << "remove snapshot failed" << lv << e.what();
            }
        }
    }
    return nr;
}

inline size_t removeTemporaryRestoredSnapshots(const cybozu::lvm::LvList &lvL)
{
    return removeSnapshotIf(lvL, isTmpRestoredLvName);
}

inline size_t removeTemporaryColdToBaseSnapshots(const cybozu::lvm::LvList &lvL)
{
    return removeSnapshotIf(lvL, isTmpColdToBaseLvName);
}

inline VolLvCacheMap getVolLvCacheMap(
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

/**
 * Data manager for a volume in a server.
 * This is not thread-safe.
 */
class ArchiveVolInfo
{
public:
    const cybozu::FilePath volDir;
    const std::string volId;
    const std::string vgName;
    const std::string thinpool;
private:
    WalbDiffFiles wdiffs_;
    VolLvCache &lvC_;

public:
    /**
     * @baseDirStr base directory path string.
     * @volId volume identifier.
     * @vgName volume group name.
     */
    ArchiveVolInfo(const std::string &baseDirStr, const std::string &volId,
                   const std::string &vgName, const std::string &thinpool,
                   MetaDiffManager &diffMgr, VolLvCache &lvC)
        : volDir(cybozu::FilePath(baseDirStr) + volId)
        , volId(volId)
        , vgName(vgName)
        , thinpool(thinpool)
        , wdiffs_(diffMgr, volDir.str())
        , lvC_(lvC) {
        // Check of baseDirStr and vgName must have been done at startup time.
        if (volId.empty()) {
            throw cybozu::Exception("ArchiveVolInfo:volId is empty");
        }
    }
    void init() {
        util::makeDir(volDir.str(), "ArchiveVolInfo::init", true);
        cybozu::Uuid uuid;
        setUuid(uuid);
        setArchiveUuid(uuid);
        setMetaState(MetaState());
        setState(aSyncReady);
    }
    void clearAllSnapLv() {
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
    void clearAllWdiffs() {
        wdiffs_.clear();
    }
    /**
     * CAUSION:
     *   The volume will be removed if exists.
     *   All data inside the directory will be removed.
     */
    void clear() {
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
    void removeDiffs(const MetaDiffVec& diffV)
    {
        wdiffs_.removeDiffs(diffV);
    }
    void removeBeforeGid(uint64_t gid) {
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
    cybozu::Uuid getUuid() const {
        cybozu::Uuid uuid;
        util::loadFile(volDir, "uuid", uuid);
        return uuid;
    }
    void setUuid(const cybozu::Uuid &uuid) {
        util::saveFile(volDir, "uuid", uuid);
    }
    cybozu::Uuid getArchiveUuid() const {
        cybozu::Uuid uuid;
        util::loadFile(volDir, "archive_uuid", uuid);
        return uuid;
    }
    void setArchiveUuid(const cybozu::Uuid &uuid) {
        util::saveFile(volDir, "archive_uuid", uuid);
    }
    cybozu::Uuid generateArchiveUuid() {
        cybozu::Uuid uuid;
        cybozu::util::Random<size_t> rand;
        uuid.setRand(rand);
        setArchiveUuid(uuid);
        return uuid;
    }
    void verifyArchiveUuid(const cybozu::Uuid &uuid) const {
        const cybozu::Uuid archiveUuid = getArchiveUuid();
        if (uuid == archiveUuid) return;
        throw cybozu::Exception("ArchiveVolInfo::verifyArchiveUuid:invalid archive uuid")
            << volId << uuid << archiveUuid;
    }
    void setMetaState(const MetaState &st) {
        util::saveFile(volDir, "base", st);
    }
    MetaState getMetaState() const {
        MetaState st;
        util::loadFile(volDir, "base", st);
        return st;
    }
    std::string coldTimestampFileName(uint64_t gid) const {
        return cybozu::util::formatString("%" PRIu64 ".cold", gid);
    }
    void setColdTimestamp(uint64_t gid, uint64_t timestamp) {
        util::saveFile(volDir, coldTimestampFileName(gid), timestamp);
    }
    uint64_t getColdTimestamp(uint64_t gid) const {
        uint64_t timestamp;
        util::loadFile(volDir, coldTimestampFileName(gid), timestamp);
        return timestamp;
    }
    bool existsColdTimestamp(uint64_t gid) const {
        cybozu::FilePath p = volDir + coldTimestampFileName(gid);
        return p.stat().isFile();
    }
    void removeColdTimestamp(uint64_t gid) {
        cybozu::FilePath p = volDir + coldTimestampFileName(gid);
        removeFile(p);
    }
    void removeColdTimestampFilesBeforeGid(uint64_t maxGid) {
        for (std::string &fname : util::getFileNameList(volDir.str(), "cold")) {
            const uint64_t gid = cybozu::atoi(cybozu::util::removeSuffix(fname, ".cold"));
            if (gid >= maxGid) continue;
            cybozu::FilePath p = volDir + fname;
            removeFile(p);
        }
    }
    MetaState getMetaStateForRestore(uint64_t gid, bool &useCold) const {
        return getMetaStateForDetail(gid, useCold, false);
    }
    MetaState getMetaStateForApply(uint64_t gid, bool &useCold) const {
        return getMetaStateForDetail(gid, useCold, true);
    }
    MetaState getMetaStateForDetail(uint64_t gid, bool &useCold, bool isApply) const {
        MetaState st = getMetaState();
        useCold = false;
        uint64_t coldGid;
        if (isApply && st.isApplying) return st;
        if (!lvC_.searchColdNoGreaterThanGid(gid, coldGid)) return st;
        if (coldGid <= st.snapB.gidB) return st;
        useCold = true;
        return MetaState(MetaSnap(coldGid), getColdTimestamp(coldGid));
    }
    void setState(const std::string& newState)
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
    std::string getState() const {
        std::string st;
        util::loadFile(volDir, "state", st);
        return st;
    }
    const char *getFullReplStateFileName() const {
        static const char name[] = "full_repl_state";
        return name;
    }
    cybozu::FilePath getFullReplStateFilePath() const {
        return volDir + cybozu::FilePath(getFullReplStateFileName());
    }
    bool getFullReplState(FullReplState& fullReplSt) const {
        const cybozu::FilePath path = getFullReplStateFilePath();
        if (!path.stat().exists()) return false;
        util::loadFile(volDir, getFullReplStateFileName(), fullReplSt);
        return true;
    }
    void removeFullReplState() {
        const cybozu::FilePath path = getFullReplStateFilePath();
        if (!path.stat().exists()) return;
        if (!path.unlink()) {
            throw cybozu::Exception("ArchiveVolInfo::removeFullReplState:unlink failed")
                << cybozu::ErrorNo();
        }
    }
    void setFullReplState(const FullReplState& fullReplSt) {
        util::saveFile(volDir, getFullReplStateFileName(), fullReplSt);
    }
    uint64_t initFullReplResume(uint64_t sizeLb, const cybozu::Uuid& archiveUuid, const MetaState& metaSt, FullReplState& fullReplSt) {
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
    bool existsVolDir() const {
        return volDir.stat().isDirectory();
    }
    bool lvExists() const {
        return cybozu::lvm::existsFile(vgName, lvName());
    }
    /**
     * Create a volume.
     * @sizeLb volume size [logical block].
     */
    void createLv(uint64_t sizeLb) {
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
    /**
     * Grow the volume.
     * @newSizeLb [logical block].
     * @doZeroClar zero-clear the extended area if true.
     */
    void growLv(uint64_t newSizeLb, bool doZeroClear = false) {
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
    StrVec getStatusAsStrVec() const {
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
    std::string restoredSnapshotName(uint64_t gid) const {
        return restoredSnapshotNamePrefix() + cybozu::itoa(gid);
    }
    std::string tmpRestoredSnapshotName(uint64_t gid) const {
        return restoredSnapshotName(gid) + TMP_VOLUME_SUFFIX;
    }
    std::string coldSnapshotName(uint64_t gid) const {
        return coldSnapshotNamePrefix() + cybozu::itoa(gid);
    }
    /**
     * Get restorable snapshots.
     * RETURN:
     *   MetaState list.
     *   st.snapB.gidB and st.timestamp have meaning.
     */
    std::vector<MetaState> getRestorableSnapshots(bool isAll = false) const {
        return getDiffMgr().getRestorableList(getMetaState(), isAll);
    }
    /**
     * Full path of the wdiff file of a corresponding meta diff.
     */
    cybozu::FilePath getDiffPath(const MetaDiff &diff) const {
        return volDir + cybozu::FilePath(createDiffFileName(diff));
    }
    const MetaDiffManager &getDiffMgr() const {
        return wdiffs_.getMgr();
    }
    MetaSnap getLatestSnapshot() const {
        const MetaState metaSt = getMetaState();
        return getDiffMgr().getLatestSnapshot(metaSt);
    }
    uint64_t getOldestCleanSnapshot() const {
        const MetaState metaSt = getMetaState();
        return getDiffMgr().getOldestCleanSnapshot(metaSt);
    }
    MetaState getOldestMetaState() const {
        MetaState metaSt = getMetaState();
        const MetaDiffVec diffV = getDiffMgr().getMinimumApplicableDiffList(metaSt);
        if (!diffV.empty()) metaSt = apply(metaSt, diffV);
        assert(!metaSt.isApplying);
        return metaSt;
    }
    /**
     * @diffV: diff vector to be.
     */
    bool changeSnapshot(const MetaDiffVec& diffV, bool enable) {
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
    /**
     * @snap base snapshot.
     * @size maximum total size [byte].
     * @nr   maximum number of wdiff files.
     * RETURN:
     *   MetaDiff list that can be merged and applicable to the snapshot.
     */
    MetaDiffVec getDiffListToSend(const MetaSnap &snap, uint64_t size, size_t nr) const {
        return wdiffs_.getDiffListToSend(snap, size, nr);
    }
    MetaDiffVec getDiffListToMerge(uint64_t gid, uint64_t size) const {
        return wdiffs_.getDiffListToMerge(gid, size);
    }
    MetaDiffVec getDiffListToMergeGid(uint64_t gidB, uint64_t gidE) const {
        MetaDiffVec v = getDiffMgr().getMergeableDiffList(gidB);
        for (size_t i = 0; i < v.size(); i++) {
            if (v[i].snapB.gidB >= gidE) {
                v.resize(i);
                break;
            }
        }
        return v;
    }
    /**
     * @srvSnap latest snapshot of the remote server.
     * @cliSnap latest snapshot of the client server (self).
     * @minSizeB if total wdiff size is less than this size, diff repl is not required.
     */
    enum {
        DONT_REPL = 0,
        DO_HASH_REPL = 1,
        DO_DIFF_REPL = 2
    };
    int shouldDoRepl(const MetaSnap &srvSnap, const MetaSnap &cliSnap, bool isSize, uint64_t param) const {
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
    /**
     * Remove garbage wdiff files.
     */
    size_t gcDiffs() {
        return wdiffs_.gc(getMetaState().snapB);
    }
    size_t gcDiffsRange(uint64_t gidB, uint64_t gidE) {
        return wdiffs_.gcRange(gidB, gidE);
    }
    size_t gcTmpFiles() {
        return cybozu::removeAllTmpFiles(volDir.str());
    }
    bool isThinProvisioning() const {
        return !thinpool.empty();
    }
    /*
     * Use cold image as the new base image.
     * This make apply command efficient.
     */
    void makeColdToBase(uint64_t gid) {
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
    /**
     * This is required after failure of makeColdToBase() execution.
     * If the base lv does not exist, temporary ColdToBase image will be used.
     * Otherwise, ColdToBase image will be go back to the cold snapshot.
     */
    void recoverColdToBaseIfNecessary() {
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
    std::string coldToBaseLvName(uint64_t gid) const {
        return lvName() + "_" + cybozu::itoa(gid) + "_tmp";
    }
private:
    cybozu::lvm::Vg getVg() const {
        return cybozu::lvm::getVg(vgName);
    }
    std::string lvName() const {
        return BASE_VOLUME_PREFIX + volId;
    }
    std::string restoredSnapshotNamePrefix() const {
        return RESTORED_VOLUME_PREFIX + volId + "_";
    }
    std::string coldSnapshotNamePrefix() const {
        return COLD_VOLUME_PREFIX + volId + "_";
    }
    void removeFile(const cybozu::FilePath &path) {
        if (!path.stat().isFile()) return;
        if (!path.unlink()) {
            LOGs.error() << "remove file failed" << path.str();
        }
    }
};

} //namespace walb
