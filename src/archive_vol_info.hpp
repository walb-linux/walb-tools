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
#include "file_path.hpp"
#include "wdiff_data.hpp"
#include "meta.hpp"
#include "walb_diff_merge.hpp"

namespace walb {

const std::string VOLUME_PREFIX = "i_";
const std::string RESTORE_PREFIX = "r_";

const char *const aClear = "Clear";
const char *const aSyncReady = "SyncReady";
const char *const aArchived = "Archived";
const char *const aStopped = "Stopped";

// temporary state
const char *const atInitVol = "InitVol";
const char *const atClearVol = "ClearVol";
const char *const atResetVol = "ResetVol";
const char *const atFullSync = "FullSync";
const char *const atHashSync = "HashSync";
const char *const atWdiffRecv = "WdiffRecv";
const char *const atReplSync = "ReplSyncAsServer";
const char *const atStop = "Stop";
const char *const atStart = "Start";

/**
 * Data manager for a volume in a server.
 * This is not thread-safe.
 */
class ArchiveVolInfo
{
public:
    const cybozu::FilePath volDir;
    const std::string volId;
private:
    const std::string vgName_;
    WalbDiffFiles wdiffs_;

public:
    /**
     * @baseDirStr base directory path string.
     * @volId volume identifier.
     * @vgName volume group name.
     */
    ArchiveVolInfo(const std::string &baseDirStr, const std::string &volId,
                   const std::string &vgName, MetaDiffManager &diffMgr)
        : volDir(cybozu::FilePath(baseDirStr) + volId)
        , volId(volId)
        , vgName_(vgName)
        , wdiffs_(diffMgr, volDir.str()) {
        cybozu::FilePath baseDir(baseDirStr);
        if (!baseDir.stat().isDirectory()) {
            throw cybozu::Exception("ArchiveVolInfo:Directory not found: " + baseDirStr);
        }
        if (!cybozu::lvm::vgExists(vgName_)) {
            throw cybozu::Exception("ArchiveVolInfo:Vg does not exist: " + vgName_);
        }
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
    void removeDiffs(const std::vector<MetaDiff>& diffV)
    {
        wdiffs_.removeDiffs(diffV);
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
        return cybozu::lvm::exists(vgName_, lvName());
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
            uint64_t curSizeLb = getLv().sizeLb();
            if (curSizeLb != sizeLb) {
                throw cybozu::Exception("ArchiveVolInfo::createLv:sizeLb is different") << curSizeLb << sizeLb;
            }
            return;
        }
        getVg().create(lvName(), sizeLb);
    }
    /**
     * Get volume data.
     */
    cybozu::lvm::Lv getLv() const {
        cybozu::lvm::Lv lv = cybozu::lvm::locate(vgName_, lvName());
        if (lv.isSnapshot()) {
            throw cybozu::Exception(
                "The target must not be snapshot: " + lv.path().str());
        }
        return lv;
    }
    /**
     * Grow the volume.
     * @newSizeLb [logical block].
     */
    void growLv(uint64_t newSizeLb) {
        cybozu::lvm::Lv lv = getLv();
        if (lv.sizeLb() == newSizeLb) {
            /* no need to grow. */
            return;
        }
        if (newSizeLb < lv.sizeLb()) {
            /* Shrink is not supported. */
            throw cybozu::Exception(
                "You tried to shrink the volume: " + lv.path().str());
        }
        lv.resize(newSizeLb);
    }
    std::vector<std::string> getStatusAsStrVec() const {
        const char *const FUNC = __func__;
        std::vector<std::string> v;
        auto &fmt = cybozu::util::formatString;

        if (!existsVolDir()) {
            throw cybozu::Exception(FUNC) << "not found volDir" << volDir.str();
        }
        const bool existsLv = lvExists();

        v.push_back(fmt("volId %s", volId.c_str()));
        uint64_t sizeLb = 0;
        if (existsLv) sizeLb = getLv().sizeLb();
        const std::string sizeStr = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
        v.push_back(fmt("size %" PRIu64 " %s", sizeLb, sizeStr.c_str()));
        const cybozu::Uuid uuid = getUuid();
        v.push_back(fmt("uuid %s", uuid.str().c_str()));
        std::string st = getState();
        v.push_back(fmt("state %s", st.c_str()));
        std::vector<std::string> actionV; // TODO
        std::string actions;
        for (const std::string &s : actionV) {
            actions += std::string(" ") + s;
        }
        v.push_back(fmt("actions %s", actions.c_str()));
        const MetaState metaSt = getMetaState();
        v.push_back(fmt("base %s", metaSt.str().c_str()));
        const MetaSnap latest = wdiffs_.getMgr().getLatestSnapshot(metaSt);
        v.push_back(fmt("latest %s", latest.str().c_str()));
        size_t numRestored = 0; // TODO
        v.push_back(fmt("numRestored %zu", numRestored));
        for (size_t i = 0; i < numRestored; i++) {
            uint64_t gid = 0; // TODO
            v.push_back(fmt("restored %" PRIu64 "", gid));
        }
        std::vector<MetaSnap> rv; // TODO
        v.push_back(fmt("numRestoreble %zu", rv.size()));
        for (size_t i = 0; i < rv.size(); i++) {
            MetaSnap snap; // TODO
            v.push_back(fmt("snapshot %" PRIu64 "", snap.gidB));
        }
        std::vector<MetaDiff> dv = wdiffs_.getMgr().getApplicableDiffList(metaSt.snapB);
        v.push_back(fmt("numWdiff %zu", dv.size()));
        for (const MetaDiff &d : dv) {
            size_t size = wdiffs_.getDiffFileSize(d);
            v.push_back(fmt("wdiff %s %d %" PRIu64 " %s"
                            , d.str().c_str(), d.isMergeable ? 1 : 0, size
                            , cybozu::unixTimeToStr(d.timestamp).c_str()));
        }
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
    std::vector<MetaDiff> getDiffListToSend(const MetaSnap &snap, uint64_t size) const {
        return wdiffs_.getDiffListToSend(snap, size);
    }
    std::vector<MetaDiff> getDiffListToMerge(uint64_t gid, uint64_t size) const {
        return wdiffs_.getDiffListToMerge(gid, size);
    }
    std::vector<MetaDiff> getDiffListToMergeGid(uint64_t gidB, uint64_t gidE) const {
        std::vector<MetaDiff> v = getDiffMgr().getMergeableDiffList(gidB);
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
     *
     * RETURN:
     *   doHashRepl, doDiffRepl
     */
    std::pair<bool, bool> shouldDoRepl(const MetaSnap &srvSnap, const MetaSnap &cliSnap, uint64_t minSizeB) const {
        if (srvSnap.gidB >= cliSnap.gidB) return {false, false};
        const std::vector<MetaDiff> diffV = getDiffMgr().getDiffListToSync(MetaState(srvSnap, 0), cliSnap);
        if (diffV.empty()) return {true, false};
        if (minSizeB == 0) return {false, true};
        uint64_t totalB = 0;
        for (const MetaDiff &diff : diffV) {
            const uint64_t diffSizeB = wdiffs_.getDiffFileSize(diff);
            totalB += diffSizeB;
            if (totalB > minSizeB) return {false, true};
        }
        return {false, false};
    }
private:
    cybozu::lvm::Vg getVg() const {
        return cybozu::lvm::getVg(vgName_);
    }
    std::string lvName() const {
        return VOLUME_PREFIX + volId;
    }
    std::string restoredSnapshotNamePrefix() const {
        return RESTORE_PREFIX + volId + "_";
    }
#if 0 // XXX
    template <typename OutputStream>
    void print(OutputStream &os) const {

        MetaSnap oldest = baseRecord_;
        MetaSnap latest = wdiffsP_->latest();
        std::string oldestState = oldest.isDirty() ? "dirty" : "clean";
        std::string latestState = latest.isDirty() ? "dirty" : "clean";

        os << "vg: " << vgName_ << std::endl;
        os << "name: " << volId_ << std::endl;
        os << "sizeLb: " << getLv().sizeLb() << std::endl;
        os << "oldest: (" << oldest.gid0() << ", " << oldest.gid1() << ") "
           << oldestState << std::endl;
        os << "latest: (" << latest.gid0() << ", " << latest.gid1() << ") "
           << latestState << std::endl;

        os << "----------restored snapshots----------" << std::endl;
        for (auto &pair : getRestores()) {
            cybozu::lvm::Lv &lv = pair.second;
            lv.print(os);
        }

        os << "----------diff files----------" << std::endl;
        std::vector<std::string> v = wdiffsP_->listName();
        for (std::string &fileName : v) {
            os << fileName << std::endl;
        }
        os << "----------end----------" << std::endl;
    }
    void print(::FILE *fp = ::stdout) const {
        std::stringstream ss;
        print(ss);
        std::string s(ss.str());
        if (::fwrite(&s[0], 1, s.size(), fp) < s.size()) {
            throw cybozu::Exception("fwrite failed.");
        }
        ::fflush(fp);
    }
    /**
     * Update the base record to be dirty to start wdiffs application.
     */
    void startToApply(const MetaDiff &diff) {
        baseRecord_ = walb::startToApply(baseRecord_, diff);
        saveBaseRecord();
    }
    /**
     * Update the base record to be clean to finish wdiffs application.
     * And remove old wdiffs.
     */
    void finishToApply(const MetaDiff &diff) {
        baseRecord_ = walb::finishToApply(baseRecord_, diff);
        saveBaseRecord();
    }
    /**
     * Remove old diffs which have been applied to the base lv already.
     */
    void removeOldDiffs() {
        wdiffsP_->removeBeforeGid(baseRecord_.gid0());
    }
    /**
     * You must prepare wdiff file before calling this.
     * Add a wdiff.
     */
    void add(const MetaDiff &diff) {
        if (!wdiffsP_->add(diff)) throw RT_ERR("diff add failed.");
    }
private:
    cybozu::FilePath baseRecordPath() const {
        return getDir() + cybozu::FilePath("base");
    }
    bool loadBaseRecord() {
        if (!baseRecordPath().stat().exists()) return false;
        cybozu::util::FileReader reader(baseRecordPath().str(), O_RDONLY);
        cybozu::load(baseRecord_, reader);
        checkBaseRecord();
        return true;
    }
    void saveBaseRecord() const {
        checkBaseRecord();
        cybozu::TmpFile tmpFile(getDir().str());
        cybozu::save(tmpFile, baseRecord_);
        tmpFile.save(baseRecordPath().str());
    }
    void checkBaseRecord() const {
        if (!baseRecord_.isValid()) {
            throw cybozu::Exception("baseRecord is not valid.");
        }
    }
#endif // XXX
};

} //namespace walb
