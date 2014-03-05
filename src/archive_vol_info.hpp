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
const char *const atFullSync = "FullSync";
const char *const atHashSync = "HashSync";
const char *const atWdiffRecv = "WdiffRecv";
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
private:
    const std::string vgName_;
    const std::string volId_;
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
        , vgName_(vgName)
        , volId_(volId)
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
     * Restore a snapshot.
     * (1) create lvm snapshot of base lv. (with temporal lv name)
     * (2) apply appropriate wdiff files.
     * (3) rename the lvm snapshot.
     */
    bool restore(uint64_t gid) {
        using namespace walb::diff;
        cybozu::lvm::Lv lv = getLv();
        const std::string targetName = restoredSnapshotName(gid);
        const std::string tmpLvName = targetName + "_tmp";
        if (lv.hasSnapshot(tmpLvName)) {
            lv.getSnapshot(tmpLvName).remove();
        }
        if (lv.hasSnapshot(targetName)) {
            // Already restored.
            return false;
        }
        cybozu::lvm::Lv lvSnap = lv.takeSnapshot(tmpLvName, true);
        const MetaDiffManager& mgr = wdiffs_.getMgr();
        const int maxRetryNum = 10;
        int retryNum = 0;
        std::vector<cybozu::util::FileOpener> ops;
    retry:
        {
            const std::vector<MetaDiff> metaDiffList = mgr.getDiffListToRestore(getMetaState(), gid);
            if (metaDiffList.empty()) {
                return false;
            }
            // apply wdiff files indicated by metaDiffList to lvSnap.
            for (const MetaDiff& diff : metaDiffList) {
                cybozu::util::FileOpener op;
                if (!op.open(getDiffPath(diff).str(), O_RDONLY)) {
                    retryNum++;
                    if (retryNum == maxRetryNum) throw cybozu::Exception("ArchiveVolInfo::restore:exceed max retry");
                    ops.clear();
                    goto retry;
                }
                ops.push_back(std::move(op));
            }
        }
        std::vector<int> fds;
        for (cybozu::util::FileOpener& op : ops) {
            fds.push_back(op.fd());
        }
        diff::Merger merger;
        merger.addWdiffs(fds);
        diff::RecIo recIo;
        cybozu::util::BlockDevice bd(lvSnap.path().str(), O_RDWR);
        std::vector<char> zero;
        while (merger.pop(recIo)) {
            const walb_diff_record& rec = recIo.record().record();
            assert(!isCompressedRec(rec));
            const uint64_t ioAddress = rec.io_address;
            const uint64_t ioBlocks = rec.io_blocks;;
            LOGd_("ioAddr %" PRIu64 " ioBlks %" PRIu64 "", ioAddress, ioBlocks);
            const uint64_t ioAddrB = ioAddress * LOGICAL_BLOCK_SIZE;
            const uint64_t ioSizeB = ioBlocks * LOGICAL_BLOCK_SIZE;

            // TODO: check the IO is out of range or not.

            const char *data;
            // Curently a discard IO is converted to an all-zero IO.
            if (isAllZeroRec(rec) || isDiscardRec(rec)) {
                if (zero.size() < ioSizeB) zero.resize(ioSizeB, 0);
                data = &zero[0];
            } else {
                data = recIo.io().rawData();
            }
            bd.write(ioAddrB, ioSizeB, data);
        }
        bd.fdatasync();
        bd.close();
        cybozu::lvm::renameLv(lv.vgName(), tmpLvName, targetName);
        return true;
    }
    /**
     * QQQ
     */
    bool apply(uint64_t timestamp) {
        MetaState st0 = getMetaState();
        const MetaDiffManager &mgr = wdiffs_.getMgr();
        std::vector<MetaDiff> metaDiffList = mgr.getDiffListToApply(st0, timestamp);
        if (metaDiffList.empty()) {
            // There is nothing to apply.
            return false;
        }
        MetaState st1 = applying(st0, metaDiffList);
        setMetaState(st1);

        // QQQ
        // apply wdiff files indicated by metaDiffList to lv.

        MetaState st2 = walb::apply(st0, metaDiffList);
        setMetaState(st2);

        // QQQ
        // Remove applied wdiff files.

        return true;
    }
    std::vector<std::string> getStatusAsStrVec() const {
        std::vector<std::string> v;
        auto &fmt = cybozu::util::formatString;

        v.push_back(fmt("volId %s", volId_.c_str()));
        uint64_t sizeLb = 0; // TODO
        v.push_back(fmt("size %" PRIu64 "", sizeLb));
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
                            , d.str().c_str(), d.canMerge ? 1 : 0, size
                            , cybozu::unixTimeToStr(d.timestamp).c_str()));
        }
        return v;
    }
private:
    cybozu::lvm::Vg getVg() const {
        return cybozu::lvm::getVg(vgName_);
    }
    std::string lvName() const {
        return VOLUME_PREFIX + volId_;
    }
    std::string restoredSnapshotName(uint64_t gid) const {
        return RESTORE_PREFIX + volId_ + "_" + cybozu::itoa(gid);
    }
    /**
     * Full path of the wdiff file of a corresponding meta diff.
     */
    cybozu::FilePath getDiffPath(const MetaDiff &diff) const {
        return volDir + cybozu::FilePath(createDiffFileName(diff));
    }
#if 0 // XXX
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
    /**
     * Get restored snapshots.
     */
    std::map<uint64_t, cybozu::lvm::Lv> getRestores() const {
        std::map<uint64_t, cybozu::lvm::Lv> map;
        std::string prefix = RESTORE_PREFIX + volId_ + "_";
        for (cybozu::lvm::Lv &lv : getLv().snapshotList()) {
            if (cybozu::util::hasPrefix(lv.snapName(), prefix)) {
                std::string gidStr
                    = cybozu::util::removePrefix(lv.snapName(), prefix);
                uint64_t gid = cybozu::atoi(gidStr);
                map.emplace(gid, lv);
            }
        }
        return map;
    }
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
     * Drop a restored snapshot.
     */
    bool drop(uint64_t gid) {
        std::string snapName = restoredSnapshotName(gid);
        if (!getLv().hasSnapshot(snapName)) {
            return false;
        }
        getLv().getSnapshot(snapName).remove();
        return true;
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
