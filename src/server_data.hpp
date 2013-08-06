/**
 * @file
 * @brief Server data.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <stdexcept>
#include <string>
#include <map>
#include <memory>
#include "cybozu/file.hpp"
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

        if (!cybozu::lvm::exists(vgName_, VOLUME_PREFIX + name_)) {
            /* There does not exist the logical volume. */
            /* TODO. */
        }

        /* initialize base record. */
        if (!loadBaseRecord()) {
            /* TODO: gid can be specified. */
            reset(0);
        }
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
        saveBaseRecord();

        wdiffsP_->reset(gid);

        /* TODO: remove the volume. */
    }
    bool initialized() const {
        /* now editing */
        return false;
    }
    /**
     * Get volume data.
     */
    cybozu::lvm::Lv volume() const {
        std::vector<cybozu::lvm::Lv> v =
            cybozu::lvm::findLv(vgName_, VOLUME_PREFIX + name_);
        if (v.empty()) {
            throw std::runtime_error(
                "Volume does not exist: " + vgName_ + "/" + name_);
        }
        return v[0];
    }
    const WalbDiffFiles &diffs() const {
        assert(wdiffsP_);
        return *wdiffsP_;
    }
    /**
     * Get restored snapshots.
     */
    std::map<std::string, cybozu::lvm::Lv> restores() const {
        std::map<std::string, cybozu::lvm::Lv> map;
        for (cybozu::lvm::Lv &lv : volume().snapshotList()) {
            if (cybozu::util::hasPrefix(lv.snapName(), RESTORE_PREFIX)) {
                std::string name
                    = cybozu::util::removePrefix(lv.snapName(), RESTORE_PREFIX);
                map.insert(std::make_pair(name, lv));
            }
        }
        return std::move(map);
    }
    void info(FILE *fp) const {
        /* now editing */
    }
    /**
     * Merge all diffs before gid into the original lv.
     */
    void merge(uint64_t gid) {
        /* now editing */
    }
    /**
     * Create a restore volume as a snapshot.
     */
    bool restore(const std::string &name) {
        cybozu::lvm::Lv snap = volume().takeSnapshot(RESTORE_PREFIX + name);
        return snap.exists();
    }
    /**
     * Drop a restored snapshot.
     */
    bool drop(const std::string &name) {
        if (!volume().hasSnapshot(RESTORE_PREFIX + name)) {
            return false;
        }
        volume().getSnapshot(RESTORE_PREFIX + name).remove();
        return true;
    }
    /**
     * Delete dangling diffs.
     */
    void cleanup() {
        wdiffsP_->cleanup();
    }
private:
    cybozu::FilePath getDir() const {
        return baseDir_ + cybozu::FilePath(name_);
    }
    cybozu::FilePath baseRecordPath() const {
        getDir() + cybozu::FilePath("base");
    }
    bool loadBaseRecord() {
        if (!baseRecordPath().stat().isFile()) return false;
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
