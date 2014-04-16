#pragma once
#include <cassert>
#include <map>
#include <vector>
#include <mutex>
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
#include "fileio.hpp"
#include "fileio_serializer.hpp"

namespace walb {

inline std::vector<MetaDiff> loadWdiffMetadata(const std::string &dirStr)
{
    std::vector<MetaDiff> ret;
    for (const std::string &fname : util::getFileNameList(dirStr, "wdiff")) {
        ret.push_back(parseDiffFileName(fname));
    }
    return ret;
}

/**
 * Manager for walb diff files.
 *
 * Wdiff files:
 *   See meta.hpp for wdiff file name format.
 */
class WalbDiffFiles
{
private:
    MetaDiffManager &mgr_; // thread-safe.
    const cybozu::FilePath dir_;

public:
    WalbDiffFiles(MetaDiffManager &mgr, const std::string &dirStr, bool doesMakeDir = false)
        : mgr_(mgr), dir_(dirStr) {
        if (doesMakeDir) walb::util::makeDir(dir_.str(), "WalbDiffFiles");
    }
    DISABLE_COPY_AND_ASSIGN(WalbDiffFiles);
    DISABLE_MOVE(WalbDiffFiles);

    /**
     * CAUSION:
     *   All data inside the directory will be removed.
     */
    void clear() {
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
        mgr_.clear();
    }
    /**
     * Add a diff file.
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
     * Get transfer diff list for proxy WdiffTransfer.
     * @size max total file size [byte].
     */
    std::vector<MetaDiff> getDiffListToSend(uint64_t size) const {
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
     * Remove wdiffs from memory and storage.
     */
    void removeDiffs(const std::vector<MetaDiff> &v) {
        mgr_.erase(v);
        removeDiffFiles(v);
    }
    /**
     * Remove diff files.
     * @diffV diff list.
     */
    void removeDiffFiles(const std::vector<MetaDiff> &v) {
        for (const MetaDiff &d : v) {
            cybozu::FilePath p = dir_ + createDiffFileName(d);
            if (!p.stat().isFile()) continue;
            if (!p.unlink()) {
                LOGs.error() << "removeDiffFiles:unlink failed" << p.str();
            }
        }
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
     * It's heavy operation.
     */
    void reload() {
        mgr_.reset(loadWdiffMetadata(dir_.str()));
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
