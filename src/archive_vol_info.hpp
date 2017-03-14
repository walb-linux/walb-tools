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

bool parseBaseLvName(const std::string &name, std::string &volId);
bool isBaseLvName(const std::string &name);


/**
 * s: {volId}_{gid} format.
 */
bool parseVolIdUnderscoreGid(const std::string &s, std::string &volId, uint64_t &gid);


bool parseTmpColdToBaseLvName(const std::string &name, std::string &volId, uint64_t &gid);
bool isTmpColdToBaseLvName(const std::string &name);
bool parseSnapLvName(const std::string &name, std::string &volId, uint64_t &gid, bool isCold, bool isTmp);
bool isColdLvName(const std::string &name);
bool isRestoredLvName(const std::string &name);
bool isTmpRestoredLvName(const std::string &name);



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

    bool searchColdNoGreaterThanGid(uint64_t gid, uint64_t &coldGid) const;

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
    Lv getSnap(uint64_t gid, bool isCold) const;
    size_t getNrSnap(bool isCold) const {
        UniqueLock lk(*muPtr_);
        return getMap(isCold).size();
    }
    std::vector<uint64_t> getSnapGidList(bool isCold) const;
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
    void resizeSnap(uint64_t gid, uint64_t newSizeLb, bool isCold);
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

VolLvCache& insertVolLvCacheMapIfNotFound(VolLvCacheMap &map, const std::string &volId);


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

VolLvCacheMap getVolLvCacheMap(
    const cybozu::lvm::LvList &lvL, const std::string &tpName, const StrVec &volIdV);


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
    void init();
    void clearAllSnapLv();
    void clearAllWdiffs() {
        wdiffs_.clear();
    }
    /**
     * CAUSION:
     *   The volume will be removed if exists.
     *   All data inside the directory will be removed.
     */
    void clear();
    void removeDiffs(const MetaDiffVec& diffV) {
        wdiffs_.removeDiffs(diffV);
    }
    void removeBeforeGid(uint64_t gid);
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
    MetaState getMetaStateForDetail(uint64_t gid, bool &useCold, bool isApply) const;
    void setState(const std::string& newState);
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
    uint64_t initFullReplResume(uint64_t sizeLb, const cybozu::Uuid& archiveUuid,
                                const MetaState& metaSt, FullReplState& fullReplSt);
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
    void createLv(uint64_t sizeLb);
    /**
     * Grow the volume.
     * @newSizeLb [logical block].
     * @doZeroClar zero-clear the extended area if true.
     */
    void growLv(uint64_t newSizeLb, bool doZeroClear = false);
    StrVec getStatusAsStrVec() const;
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
     *   st.isExplicit has also meaning.
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
    MetaState getLatestState() const {
        const MetaState metaSt = getMetaState();
        return getDiffMgr().getLatestState(metaSt);
    }
    MetaState getOldestCleanState() const {
        const MetaState metaSt = getMetaState();
        return getDiffMgr().getOldestCleanState(metaSt);
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
    bool changeSnapshot(const MetaDiffVec& diffV, bool enable);
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
    MetaDiffVec getDiffListToMergeGid(uint64_t gidB, uint64_t gidE) const;
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
    int shouldDoRepl(const MetaSnap &srvSnap, const MetaSnap &cliSnap, bool isSize, uint64_t param) const;
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
    void makeColdToBase(uint64_t gid);
    /**
     * This is required after failure of makeColdToBase() execution.
     * If the base lv does not exist, temporary ColdToBase image will be used.
     * Otherwise, ColdToBase image will be go back to the cold snapshot.
     */
    void recoverColdToBaseIfNecessary();
    std::string coldToBaseLvName(uint64_t gid) const {
        return lvName() + "_" + cybozu::itoa(gid) + "_tmp";
    }
    VolLvCache& lvCache() { return lvC_; }
    const VolLvCache& lvCache() const { return lvC_; }
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
