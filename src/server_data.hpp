/**
 * @file
 * @brief Server data.
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

#ifndef WALB_TOOLS_SERVER_DATA_HPP
#define WALB_TOOLS_SERVER_DATA_HPP

namespace walb {

const std::string VG_NAME = "vg";
const std::string VOLUME_PREFIX = "i_";
const std::string RESTORE_PREFIX = "r_";

/**
 * Data manager for a volume in a server.
 * This is not thread-safe.
 */
class ServerData
{
private:
    const cybozu::FilePath baseDir_;
    const std::string vgName_;
    const std::string name_;
    std::shared_ptr<WalbDiffFiles> wdiffsP_;
    MetaSnap baseRecord_; /* snapshot information of the volume. */

public:
    /**
     * @baseDirStr base directory path string.
     * @name volume identifier.
     * @vgName volume group name.
     */
    ServerData(const std::string &baseDirStr, const std::string &name,
               const std::string &vgName = VG_NAME)
        : baseDir_(baseDirStr)
        , vgName_(vgName)
        , name_(name)
        , wdiffsP_()
        , baseRecord_() {
        if (!baseDir_.stat().isDirectory()) {
            throw std::runtime_error("Directory not found: " + baseDir_.str());
        }
        wdiffsP_ = std::make_shared<WalbDiffFiles>(getDir().str(), true);
        if (!loadBaseRecord()) {
            /* TODO: gid can be specified. */
            reset(0);
        }
    }
    bool lvExists() const {
        return cybozu::lvm::exists(vgName_, lvName());
    }
    /**
     * Create a volume.
     * @sizeLb volume size [logical block].
     */
    void createLv(size_t sizeLb) {
        assert(0 < sizeLb);
        if (lvExists()) return;
        getVg().create(lvName(), sizeLb);
    }
    /**
     * Grow the volume.
     * @newSizeLb [logical block].
     */
    void growLv(size_t newSizeLb) {
        cybozu::lvm::Lv lv = getLv();
        if (lv.sizeLb() == newSizeLb) {
            /* no need to grow. */
            return;
        }
        if (newSizeLb < lv.sizeLb()) {
            /* Shrink is not supported. */
            throw std::runtime_error(
                "You tried to shrink the volume: " + lv.path().str());
        }
        lv.resize(newSizeLb);
    }
    /**
     * Get volume data.
     */
    cybozu::lvm::Lv getLv() const {
        cybozu::lvm::Lv lv = cybozu::lvm::locate(vgName_, lvName());
        if (lv.isSnapshot()) {
            throw std::runtime_error(
                "The target must not be snapshot: " + lv.path().str());
        }
        return lv;
    }
    /**
     * CAUSION:
     *   All data inside the directory will be removed.
     *   The volume will be removed if exists.
     */
    void reset(uint64_t gid) {
        baseRecord_.init();
        baseRecord_.raw().gid0 = gid;
        baseRecord_.raw().gid1 = gid;
        baseRecord_.raw().timestamp = ::time(0);
        saveBaseRecord();

        wdiffsP_->reset(gid);

        if (lvExists()) {
            getLv().remove();
        }
    }
    bool initialized() const {
        /* now editing */
        return false;
    }
    const WalbDiffFiles &diffs() const {
        assert(wdiffsP_);
        return *wdiffsP_;
    }
    /**
     * Get restored snapshots.
     */
    std::map<uint64_t, cybozu::lvm::Lv> restores() const {
        std::map<uint64_t, cybozu::lvm::Lv> map;
        std::string prefix = RESTORE_PREFIX + name_ + "_";
        for (cybozu::lvm::Lv &lv : getLv().snapshotList()) {
            if (cybozu::util::hasPrefix(lv.snapName(), prefix)) {
                std::string gidStr
                    = cybozu::util::removePrefix(lv.snapName(), prefix);
                uint64_t gid = cybozu::atoi(gidStr);
                map.insert(std::make_pair(gid, lv));
            }
        }
        return std::move(map);
    }
    /**
     * Get the latest clean snapshot.
     *
     * RETURN:
     *   gid that means the latest clean snapshot.
     */
    uint64_t getLatestCleanSnapshot() const {
        /* now editing */
        return uint64_t(-1);
    }
    template <typename OutputStream>
    void print(OutputStream &os) const {

        MetaSnap oldest = baseRecord_;
        MetaSnap latest = wdiffsP_->latest();
        std::string oldestState = oldest.isDirty() ? "dirty" : "clean";
        std::string latestState = latest.isDirty() ? "dirty" : "clean";

        os << "vg: " << vgName_ << std::endl;
        os << "name: " << name_ << std::endl;
        os << "sizeLb: " << getLv().sizeLb() << std::endl;
        os << "oldest: (" << oldest.gid0() << ", " << oldest.gid1() << ") "
           << oldestState << std::endl;
        os << "latest: (" << latest.gid0() << ", " << latest.gid1() << ") "
           << latestState << std::endl;

        os << "----------restored snapshots----------" << std::endl;
        for (auto &pair : restores()) {
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
    void print(::FILE *fp) const {
        std::stringstream ss;
        print(ss);
        std::string s(ss.str());
        if (::fwrite(&s[0], 1, s.size(), fp) < s.size()) {
            throw std::runtime_error("fwrite failed.");
        }
        ::fflush(fp);
    }
    void print() const { print(::stdout); }
    /**
     * Create a restore volume as a snapshot.
     * @gid restored snapshot will indicates the gid.
     */
    bool restore(uint64_t gid = uint64_t(-1)) {
        if (gid == uint64_t(-1)) {
            gid = getLatestCleanSnapshot();
        } else {
            if (canRestore(gid)) {

            }
        }
        std::string suffix
            = cybozu::util::formatString("_%" PRIu64 "", gid);
        std::string snapName = RESTORE_PREFIX + name_ + suffix;
        if (getLv().hasSnapshot(snapName)) return false;
        cybozu::lvm::Lv snap = getLv().takeSnapshot(snapName);
        return snap.exists();
    }
    /**
     * Whether a specified gid can be restored.
     * That means wdiffs has the clean snaphsot for the gid.
     */
    bool canRestore(uint64_t gid) const {
        for (const MetaDiff &diff : wdiffsP_->listDiff()) {
            if (diff.gid1() == gid && !diff.isDirty()) {
                return true;
            }
        }
        return false;
    }
    /**
     * Drop a restored snapshot.
     */
    bool drop(const std::string &name) {
        std::string snapName = RESTORE_PREFIX + name;
        if (!getLv().hasSnapshot(snapName)) {
            return false;
        }
        getLv().getSnapshot(snapName).remove();
        return true;
    }
    /**
     * This is what to do for application.
     * DO NOT CALL THIS really.
     *
     * Apply all diffs before gid into the original lv.
     * Applied diff will be deleted after the application completed successfully.
     */
    void apply(uint64_t gid) {
        throw std::runtime_error("Do not call this.");

        std::vector<MetaDiff> diffV = diffsToApply(gid);
        if (diffV.empty()) return;
        MetaDiff diff = diffV[0];
        for (size_t i = 1; i < diffV.size(); i++) {
            if (!diff.canMerge(diffV[i])) {
                throw std::runtime_error("could not merge.");
            }
            diff = diff.merge(diffV[i]);
        }
        std::vector<cybozu::FilePath> pathV;
        for (MetaDiff &diff : diffV) {
            pathV.push_back(getDiffPath(diff));
        }
        /*
         * Try to open all files in the pathV.
         */
        startToApply(diff);
        /*
         * Apply the wdiff file indicated by the pathV.
         */
        finishToApply(diff);
    }
    /**
     * Full path of the wdiff file of a corresponding meta diff.
     */
    cybozu::FilePath getDiffPath(const MetaDiff &diff) const {
        return getDir() + cybozu::FilePath(createDiffFileName(diff));
    }
    /**
     * @gid must be a valid separator of wdiff file and file.
     *
     * RETURN:
     *   a list of meta diff to apply.
     */
    std::vector<MetaDiff> diffsToApply(uint64_t gid) {
        throw RT_ERR("%s: not yet implemented.", __func__);
        /* now editing */
        return {};
    }
    /**
     * Update the base record to be dirty to start wdiffs application.
     */
    void startToApply(const MetaDiff &diff) {
        baseRecord_ = baseRecord_.startToApply(diff);
        saveBaseRecord();
    }
    /**
     * Update the base record to be clean to finish wdiffs application.
     * And remove old wdiffs.
     */
    void finishToApply(const MetaDiff &diff) {
        baseRecord_ = baseRecord_.finishToApply(diff);
        saveBaseRecord();
    }
    /**
     * You must prepare wdiff file before calling this.
     * Add a wdiff.
     */
    void add(const MetaDiff &diff) {
        throw RT_ERR("%s: not yet implemented.", __func__);
        /* now editing */
    }
    /**
     * Get wdiff candidates for consolidation.
     * You must consolidate them by yourself.
     * After consolidation completed,
     * you must call finishToConsolidate(diff) where
     * the diff is consolidate(returned diff list).
     *
     * @ts0, @ts1: timestamp range [ts0, ts1).
     *
     * RETURN:
     *   Consolidate candidates whose average file size is the smallest.
     *   canConsolidate(returned) must be true.
     */
    std::vector<MetaDiff> candidatesToConsolidate(uint64_t ts0, uint64_t ts1) const {
        std::vector<MetaDiff> v;
        std::vector<std::vector<MetaDiff> > vv;
        std::vector<std::pair<uint64_t, uint64_t> > stat;
        uint64_t total = 0;

        auto insert = [&]() {
            assert(!v.empty());
            stat.push_back(std::make_pair(total, v.size()));
            assert(canConsolidate(v));
            vv.push_back(std::move(v));
            v.clear();
            total = 0;
        };

        /*
         * Split wdiff file list where each list can be consolidated.
         */
        for (MetaDiff &diff : wdiffsP_->listDiffInTimeRange(ts0, ts1)) {
            if (!v.empty() && !v.back().canMerge(diff)) insert();
            v.push_back(diff);
            total += wdiffsP_->getDiffFileSize(diff);
        }
        if (!v.empty()) insert();
        if (vv.empty()) return {};
        assert(vv.size() == stat.size());
        std::vector<double> avgV;
        for (auto &pair : stat) {
            avgV.push_back(double(pair.first) / double(pair.second));
        }
        assert(vv.size() == avgV.size());

        /*
         * Choose the wdiff list whose average size is the smallest.
         */
        size_t idx = 0;
        double min = avgV[0];
        for (size_t i = 1; i < avgV.size(); i++) {
            if (avgV[i] < min) {
                idx = i;
                min = avgV[i];
            }
        }
        return vv[idx];
    }
    /**
     * Finish to consolidate.
     * (1) insert the corresponding meta diff.
     * (2) delete dangling diffs.
     */
    void finishToConsolidate(const MetaDiff &diff) {
        if (!wdiffsP_->consolidate(diff.gid0(), diff.gid1())) {
            throw std::runtime_error("Consolidate failed.");
        }
        wdiffsP_->cleanup();
    }
private:
    cybozu::lvm::Vg getVg() const {
        return cybozu::lvm::getVg(vgName_);
    }
    std::string lvName() const {
        return VOLUME_PREFIX + name_;
    }
    cybozu::FilePath getDir() const {
        return baseDir_ + cybozu::FilePath(name_);
    }
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
            throw std::runtime_error("baseRecord is not valid.");
        }
    }
};

} //namespace walb

#endif /* WALB_TOOLS_SERVER_DATA_HPP */
