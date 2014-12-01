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

public:
    /**
     * @baseDirStr base directory path string.
     * @volId volume identifier.
     * @vgName volume group name.
     */
    ArchiveVolInfo(const std::string &baseDirStr, const std::string &volId,
                   const std::string &vgName, const std::string &thinpool,
                   MetaDiffManager &diffMgr)
        : volDir(cybozu::FilePath(baseDirStr) + volId)
        , volId(volId)
        , vgName(vgName)
        , thinpool(thinpool)
        , wdiffs_(diffMgr, volDir.str()) {
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
            getLv().remove();
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
        return cybozu::lvm::fileExists(vgName, lvName());
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
            cybozu::lvm::Lv lv = getLv();
            uint64_t curSizeLb = lv.sizeLb();
            if (curSizeLb != sizeLb) {
                throw cybozu::Exception("ArchiveVolInfo::createLv:sizeLb is different") << curSizeLb << sizeLb;
            }
            if (isThinProvisioning()) {
                // Deallocate all the area to execute efficient full backup/replication.
                cybozu::util::File file(lv.path().str(), O_RDWR | O_DIRECT);
                cybozu::util::issueDiscard(file.fd(), 0, curSizeLb);
            }
            return;
        }
        if (isThinProvisioning()) {
            getVg().createThin(thinpool, lvName(), sizeLb);
        } else {
            getVg().create(lvName(), sizeLb);
        }
    }
    /**
     * Get volume data.
     */
    cybozu::lvm::Lv getLv() const {
        cybozu::lvm::Lv lv = cybozu::lvm::locate(vgName, lvName());
        if (lv.isSnapshot()) {
            throw cybozu::Exception(
                "The target must not be snapshot: " + lv.path().str());
        }
        return lv;
    }
    /**
     * Grow the volume.
     * @newSizeLb [logical block].
     * @doZeroClar zero-clear the extended area if true.
     */
    void growLv(uint64_t newSizeLb, bool doZeroClear = false) {
        cybozu::lvm::Lv lv = getLv();
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
        device::flushBufferCache(lvPathStr);
        if (doZeroClear) {
            cybozu::util::File f(lv.path().str(), O_RDWR | O_DIRECT);
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

        const uint64_t sizeLb = getLv().sizeLb();
        v.push_back(fmt("sizeLb %" PRIu64, sizeLb));
        const std::string sizeS = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
        v.push_back(fmt("size %s", sizeS.c_str()));
        const cybozu::Uuid uuid = getUuid();
        v.push_back(fmt("uuid %s", uuid.str().c_str()));
        const MetaState metaSt = getMetaState();
        v.push_back(fmt("base %s", metaSt.str().c_str()));
        const MetaSnap latest = wdiffs_.getMgr().getLatestSnapshot(metaSt);
        v.push_back(fmt("latest %s", latest.str().c_str()));
        const size_t numRestored = getRestoredSnapshots().size();
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
     * Get restored snapshots.
     *
     * RETURN:
     *   gid list (sorted).
     */
    std::vector<uint64_t> getRestoredSnapshots() const {
        std::vector<uint64_t> v;
        const std::string prefix = restoredSnapshotNamePrefix();
        for (cybozu::lvm::Lv &lv : getLv().snapshotList()) {
            if (cybozu::util::hasPrefix(lv.snapName(), prefix)) {
                const std::string gidStr = cybozu::util::removePrefix(lv.snapName(), prefix);
                if (cybozu::util::isAllDigit(gidStr)) {
                    v.push_back(cybozu::atoi(gidStr));
                }
            }
        }
        std::sort(v.begin(), v.end());
        return v;
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
    /**
     * @snap base snapshot.
     * @size maximum total size [byte].
     * RETURN:
     *   MetaDiff list that can be merged and applicable to the snapshot.
     */
    MetaDiffVec getDiffListToSend(const MetaSnap &snap, uint64_t size) const {
        return wdiffs_.getDiffListToSend(snap, size);
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
        size_t nr = 0;
        const std::string prefix = restoredSnapshotNamePrefix();
        for (cybozu::lvm::Lv &lv : getLv().snapshotList()) {
            if (cybozu::util::hasPrefix(lv.snapName(), prefix) &&
                cybozu::util::hasSuffix(lv.snapName(), RESTORE_TMP_SUFFIX)) {
                lv.remove();
                nr++;
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
