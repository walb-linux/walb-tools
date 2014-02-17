#pragma once
/**
 * @file
 * @brief Walb diff files management.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <map>
#include <vector>
#include <deque>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "cybozu/file.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "meta.hpp"
#include "time.hpp"
#include "walb_logger.hpp"
#include "walb_util.hpp"
#include "fileio_serializer.hpp"

namespace walb {

/**
 * Manager for walb diff files.
 * This is not thread-safe.
 *
 * Wdiff files:
 *   See meta.hpp for wdiff file name format.
 *
 * LatestRecord file:
 *   This indicates the latest snapshot gid(s).
 *   The file contents is a serialized MetaSnap object.
 *   gid0, gid1, and timestamp are meaningfull.
 */
class WalbDiffFiles
{
private:
    cybozu::FilePath dir_;
    MetaDiffManager mgr_;

public:
    /**
     * @dirStr a directory that contains wdiff files and latest records.
     * @isContiguous:
     *   true for server data. must be contiguous.
     *   false for proxy data. must not be contiguous but newer.
     */
    explicit WalbDiffFiles(const std::string &dirStr, bool doMakeDir = true)
        : dir_(dirStr), mgr_() {
        if (doMakeDir) walb::util::makeDir(dir_.str(), "WalbDiffFiles");
    }
    DISABLE_COPY_AND_ASSIGN(WalbDiffFiles);
    DISABLE_MOVE(WalbDiffFiles);

    /**
     * CAUSION:
     *   All data inside the directory will be removed.
     */
    void clean() {
        /* remove all wdiff files. */
        removeBeforeGid(uint64_t(-1));
    }
    /**
     * CAUSION:
     *   Whole directory will be removed.
     */
    void clearDir() {
        if (!dir_.rmdirRecursive()) {
            throw cybozu::Exception("WalbDiffFiles::eraseCompletely:rmdirRecursive failed");
        }
    }
    /**
     * Add a diff and update the "latest" record file.
     * Before calling this, you must settle the diff file.
     */
    void add(const MetaDiff &diff) {
        mgr_.add(diff);
    }
    /**
     * To call const member functions.
     */
    const MetaDiffManager &getMgr() const {
        return mgr_;
    }
    /**
     * Get transfer diff list.
     * @size max total size [byte].
     */
    std::vector<MetaDiff> getTransferDiffList(uint64_t size) {
        std::vector<MetaDiff> v = mgr_.getMergeableDiffList(0);
        if (v.empty()) return {};
        std::vector<MetaDiff> u;
        uint64_t total = 0;
        for (const MetaDiff &diff : v) {
            uint64_t size0 = getDiffFileSize(diff);
            if (size < total + size0) break;
            u.push_back(diff);
            total += size0;
        }
        if (u.empty()) {
            u.push_back(v[0]);
        }
        return u;
    }
    /**
     * Garbage collect.
     */
    void gc() {
        removeDiffFiles(mgr_.gc());
    }
    /**
     * Remove wdiffs before a specified gid.
     */
    void removeBeforeGid(uint64_t gid) {
        removeDiffFiles(mgr_.eraseBeforeGid(gid));
    }
    /**
     * List of wdiff name in order of gid.
     * RETURN:
     *   list of wdiff name.
     *   the list does not contain overlapped ones.
     */
    std::vector<std::string> listName(uint64_t gid0 = 0, uint64_t gid1 = -1) const {
        return convertDiffToName(listDiff(gid0, gid1));
    }
    /**
     * RETURN:
     *   Diff list.
     */
    std::vector<MetaDiff> listDiff(uint64_t gid0 = 0, uint64_t gid1 = -1) const {
        return mgr_.getAll(gid0, gid1);
    }
    /**
     * Reload metadata by scanning directory entries.
     * searching "*.wdiff" files.
     */
    void reloadMetadata() {
        mgr_.clear();
        std::vector<cybozu::FileInfo> list;
        if (!cybozu::GetFileList(list, dir_.str(), "wdiff")) {
            throw std::runtime_error("GetFileList failed.");
        }
        for (cybozu::FileInfo &info : list) {
            if (info.name == "." || info.name == ".." || !info.isFile)
                continue;
            MetaDiff diff = parseDiffFileName(info.name);
            mgr_.add(diff);
        }
    }
    const cybozu::FilePath &dirPath() const {
        return dir_;
    }
    /**
     * Get size of a wdiff files.
     * RETURN:
     *   size [byte].
     *   0 if the file does not exist.
     */
    size_t getDiffFileSize(const MetaDiff &diff) const {
        cybozu::FilePath fPath(createDiffFileName(diff));
        return (dir_ + fPath).stat().size();
    }
private:
    /**
     * Remove diff files.
     * @pathStrV file name list (not full paths).
     */
    void removeDiffFiles(const std::vector<MetaDiff> &v) const {
        for (const MetaDiff &d : v) {
            cybozu::FilePath p = dir_ + createDiffFileName(d);
            if (!p.stat().isFile()) continue;
            if (!p.unlink()) {
                /* TODO: put log. */
            }
        }
    }
    /**
     * Convert diff list to name list.
     */
    static std::vector<std::string> convertDiffToName(const std::vector<MetaDiff> &diffV) {
        std::vector<std::string> v;
        for (const MetaDiff &diff : diffV) {
            v.push_back(createDiffFileName(diff));
        }
        return v;
    }
};

} //namespace walb
