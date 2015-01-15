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

namespace walb {

inline bool isArchiveLvName(const std::string &name)
{
    return cybozu::util::hasPrefix(name, VOLUME_PREFIX);
}

inline bool isArchiveSnapName(const std::string &name)
{
    return cybozu::util::hasPrefix(name, RESTORE_PREFIX) && !cybozu::util::hasSuffix(name, RESTORE_TMP_SUFFIX);
}

inline bool isArchiveTmpSnapName(const std::string &name)
{
    return cybozu::util::hasPrefix(name, RESTORE_PREFIX) && cybozu::util::hasSuffix(name, RESTORE_TMP_SUFFIX);
}

inline std::string getVolIdFromArchiveLvName(const std::string &name)
{
    assert(isArchiveLvName(name));
    return cybozu::util::removePrefix(name, VOLUME_PREFIX);
}

inline void getVolIdFromArchiveSnapName(const std::string &name, std::string &volId, uint64_t &gid)
{
    assert(isArchiveSnapName(name));
    size_t n = name.rfind('_');
    if (n == std::string::npos) {
        throw cybozu::Exception(__func__) << "bad archive snapshot name" << name;
    }
    size_t p = RESTORE_PREFIX.size();
    volId = name.substr(p, n - p);
    gid = cybozu::atoi(&name[n + 1]);
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
    LvMap snapMap_;

public:
    VolLvCache() : muPtr_(new std::recursive_mutex()), exists_(false), lv_(), snapMap_() {
    }
    Lv getLv() const {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        return lv_;
    }
    bool hasSnap(uint64_t gid) const {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        return snapMap_.find(gid) != snapMap_.cend();
    }
    Lv getSnap(uint64_t gid) const {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        LvMap::const_iterator it = snapMap_.find(gid);
        if (it == snapMap_.cend()) {
            throw cybozu::Exception(__func__)
                << "snapshot not found" << lv_ << gid;
        }
        return it->second;
    }
    size_t getSnapNr() const {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        return snapMap_.size();
    }
    std::vector<uint64_t> getSnapGidList() const {
        std::vector<uint64_t> ret;
        {
            UniqueLock lk(*muPtr_);
            verifyExistance();
            for (const LvMap::value_type &p : snapMap_) {
                ret.push_back(p.first);
            }
        }
        /* sorted */
        return ret;
    }
    LvMap getSnapMap() const {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        return snapMap_; /* copy */
    }
    void add(const Lv &lv) {
        UniqueLock lk(*muPtr_);
        snapMap_.clear();
        lv_ = lv;
        exists_ = true;
    }
    void remove() {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        snapMap_.clear();
        lv_ = Lv();
        exists_ = false;
    }
    void addSnap(uint64_t gid, const Lv &snap) {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        snapMap_.emplace(gid, snap);
    }
    void removeSnap(uint64_t gid) {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        snapMap_.erase(gid);
    }
    void resize(uint64_t newSizeLb) {
        UniqueLock lk(*muPtr_);
        verifyExistance();
        lv_.setSizeLb(newSizeLb);
    }
private:
    void verifyExistance() const {
        if (!exists_) {
            throw cybozu::Exception("VolLvCache:lv does not existance in memory");
        }
    }
};

using VolLvCacheMap = std::map<std::string, VolLvCache>;

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

inline VolLvCacheMap getVolLvCacheMap(
    const std::string &vgName, const std::string &tpName, const StrVec &volIdV)
{
    VolLvCacheMap m1;
    for (cybozu::lvm::Lv &lv : cybozu::lvm::listLv(vgName)) {
        if (tpName.empty() == lv.isTv()) {
            continue;
        }
        if (lv.isSnap()) {
            if (isArchiveSnapName(lv.name())) {
                std::string volId;
                uint64_t gid;
                getVolIdFromArchiveSnapName(lv.name(), volId, gid);
                VolLvCache &lvC = insertVolLvCacheMapIfNotFound(m1, volId);
                lvC.addSnap(gid, lv);
            }
        } else {
            if (isArchiveLvName(lv.name())) {
                const std::string volId = getVolIdFromArchiveLvName(lv.name());
                VolLvCache &lvC = insertVolLvCacheMapIfNotFound(m1, volId);
                lvC.add(lv);
            }
        }
    }
    VolLvCacheMap m2;
    for (const std::string &volId : volIdV) {
        VolLvCacheMap::iterator it = m1.find(volId);
        if (it == m1.end()) {
            m2.emplace(volId, VolLvCache());
        } else {
            VolLvCache &lvC = it->second;
            m2.emplace(volId, std::move(lvC));
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
        setUuid(cybozu::Uuid());
        setMetaState(MetaState());
        setState(aSyncReady);
    }
    /**
     * CAUSION:
     *   The volume will be removed if exists.
     *   All data inside the directory will be removed.
     */
    void clear() {
        // Delete all related lvm volumes and snapshots.
        if (lvExists()) {
            VolLvCache::LvMap snapM = lvC_.getSnapMap();
            for (VolLvCache::LvMap::value_type &p : snapM) {
                const uint64_t gid = p.first;
                cybozu::lvm::Lv &snap = p.second;
                snap.remove();
                lvC_.removeSnap(gid);
            }
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
    }
    cybozu::Uuid getUuid() const {
        cybozu::Uuid uuid;
        util::loadFile(volDir, "uuid", uuid);
        return uuid;
    }
    void setUuid(const cybozu::Uuid &uuid) {
        util::saveFile(volDir, "uuid", uuid);
    }
    void setMetaState(const MetaState &st) {
        util::saveFile(volDir, "base", st);
    }
    MetaState getMetaState() const {
        MetaState st;
        util::loadFile(volDir, "base", st);
        return st;
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
            cybozu::lvm::Lv lv = lvC_.getLv();
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
        const MetaState metaSt = getMetaState();
        v.push_back(fmt("base %s", metaSt.str().c_str()));
        const MetaSnap latest = wdiffs_.getMgr().getLatestSnapshot(metaSt);
        v.push_back(fmt("latest %s", latest.str().c_str()));
        const size_t numRestored = lvC_.getSnapNr();
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
        return v;
    }
    std::string restoredSnapshotName(uint64_t gid) const {
        return restoredSnapshotNamePrefix() + cybozu::itoa(gid);
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
    void changeSnapshot(const MetaDiffVec& diffV, bool enable) {
        for (MetaDiff diff : diffV) {
            assert(diff.isMergeable == enable);
            cybozu::FilePath to = getDiffPath(diff);
            diff.isMergeable = !enable;
            cybozu::FilePath from = getDiffPath(diff);
            if (!from.rename(to)) {
                throw cybozu::Exception("ArchiveVolInfo")
                    << (enable ? "enableSnapshot" : "disableSnapshot")
                    << from << to;
            }
        }
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
        return wdiffs_.gc();
    }
    /**
     * Remove temporarily created volumes for restore.
     */
    size_t gcVolumes() {
        const std::string prefix = restoredSnapshotNamePrefix();
        size_t nr = 0;
        cybozu::lvm::Lv lv = lvC_.getLv();
        for (cybozu::lvm::Lv &snap : lv.getSnapList()) {
            if (isArchiveTmpSnapName(snap.snapName())) {
                try {
                    snap.remove();
                    nr++;
                } catch (std::exception &e) {
                    LOGs.error() << __func__ << "remove failed" << snap << e.what();
                }
            }
        }
        return nr;
    }
    bool isThinProvisioning() const {
        return !thinpool.empty();
    }
private:
    cybozu::lvm::Vg getVg() const {
        return cybozu::lvm::getVg(vgName);
    }
    std::string lvName() const {
        return VOLUME_PREFIX + volId;
    }
    std::string restoredSnapshotNamePrefix() const {
        return RESTORE_PREFIX + volId + "_";
    }
};

} //namespace walb
