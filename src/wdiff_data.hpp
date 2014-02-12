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
    const bool isContiguous_;
    using Mmap = std::multimap<uint64_t, MetaDiff>;
    Mmap mmap_; // key is diff.snapB.gidB

public:
    /**
     * @dirStr a directory that contains wdiff files and latest records.
     * @isContiguous:
     *   true for server data. must be contiguous.
     *   false for proxy data. must not be contiguous but newer.
     */
    WalbDiffFiles(const std::string &dirStr, bool isContiguous, bool doesReset = false)
        : dir_(dirStr), isContiguous_(isContiguous), mmap_() {
        if (!dir_.stat().exists()) {
            if (!dir_.mkdir()) {
                throw std::runtime_error("mkdir failed: " + dir_.str());
            }
            reset();
            return;
        }
        if (!dir_.stat().isDirectory()) {
            throw std::runtime_error("It's not a directory.");
        }
        if (doesReset) {
            if (!dir_.rmdirRecursive()) {
                throw std::runtime_error("Reset (rmdirRecursive) failed.");
            }
            if (!dir_.mkdir()) {
                throw std::runtime_error("Reset (mkdir) failed.");
            }
            reset();
            return;
        }
        reloadMetadata();
    }
    ~WalbDiffFiles() noexcept {
    }
    DISABLE_COPY_AND_ASSIGN(WalbDiffFiles);
    DISABLE_MOVE(WalbDiffFiles);

    /**
     * CAUSION:
     *   All data inside the directory will be removed.
     */
    void reset() {
        /* remove all wdiff files. */
        removeBeforeGid(uint64_t(-1));
    }
    /**
     * Add a diff and update the "latest" record file.
     * Before calling this, you must settle the diff file.
     */
    bool add(const MetaDiff &diff) {
        // if (isContiguous_) {
        // } else {
        // }
        mmap_.emplace(diff.snapB.gidB, diff);
        return true;
    }
    /**
     * Consolidate gid range [gid0, gid1) into a wdiff.
     *
     * You must call this after created the merged diff file.
     * Old wdiff files will not be removed.
     * You must call cleanup() explicitly.
     *
     * There must be diffs which gid0/gid1 is the given gid0/gid1.
     * and there is no diff which can_merge are false between them.
     * Also, they are all contiguous.
     *
     * RETURN:
     *   true if successfuly merged.
     *   or false.
     */
    bool consolidate(uint64_t, uint64_t) {
#if 1
        return false;
#else
        assert(isContiguous_);
        Mmap::const_iterator it = mmap_.lower_bound(gid0);
        if (it == mmap_.cend()) return false;
        MetaDiff merged, diff;
        it = nextKey(it, diff);
        merged = diff;
        if (diff.snap0().gid0() != gid0) return false;
        it = skipOverlapped(it, diff.snap1().gid0());
        while (it != mmap_.cend()) {
            it = nextKey(it, diff);
            if (gid1 < diff.snap1().gid0()) break;
            if (!canMerge(merged, diff)) return false;
            merged = merge(merged, diff);
            merged.setTimestamp(diff.timestamp());
            it = skipOverlapped(it, diff.snap1().gid0());
        }
        assert(merged.snap0().gid0() == gid0);
        assert(merged.snap1().gid0() == gid1);
        mmap_.emplace(merged.snap0().gid0(), merged);
        return true;
#endif
    }
    /**
     * Search diffs which can be merged.
     * @gid lower gid.
     * RETURN:
     *   A list of gid ranges each of which can be merged.
     */
    std::vector<MetaDiff> getMergingCandidates(uint64_t gid = 0) const {
        std::vector<MetaDiff> v = getFirstDiffs(gid);
        if (v.empty()) return {};
        MetaDiff diff = chooseFromCandidates(v);
        v = { diff };
        MetaDiff mDiff = diff;
        while (true) {
            std::vector<MetaDiff> u = getMergableDiffs(mDiff);
            if (u.empty()) break;
            diff = chooseFromCandidates(u);
            v.push_back(diff);
            mDiff = merge(mDiff, diff);
        }
        return v;
    }
    /**
     * Search mergable diffs.
     * This is simple but O(N) algorithm.
     */
    std::vector<MetaDiff> getMergableDiffs(const MetaDiff &diff) const {
        std::vector<MetaDiff> v;
        Mmap::const_iterator it = mmap_.begin();
        while (it != mmap_.cend()) {
            MetaDiff c = it->second;
            if (diff.snapE.gidE < c.snapB.gidB) {
                // There is no candidates after this.
                break;
            }
            if (diff != c && canMerge(diff, c)) {
                v.push_back(c);
            }
            ++it;
        }
        return v;
    }
    /**
     * Choose one diff from candidates with the maximum snapE.gidB.
     */
    static MetaDiff chooseFromCandidates(const std::vector<MetaDiff> &v) {
        if (v.empty()) throw cybozu::Exception("chooseFromCandidates:empty");
        MetaDiff diff = v[0];
        for (size_t i = 1; i < v.size(); i++) {
            if (diff.snapE.gidB < v[i].snapE.gidB) {
                diff = v[i];
            }
        }
        return diff;
    }
    /**
     * Get transfer candidates.
     * This is for proxy.
     * @gid start gid.
     * @size max total size [byte].
     */
    std::vector<MetaDiff> getTransferCandidates(uint64_t size) {
        std::vector<MetaDiff> v = getMergingCandidates();
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
    std::map<std::pair<uint64_t, uint64_t>, std::vector<MetaDiff> > groupBySnapB() {
        std::map<std::pair<uint64_t, uint64_t>, std::vector<MetaDiff> > m;
        for (auto &p : mmap_) {
            MetaDiff &diff = p.second;
            auto key = std::make_pair(diff.snapB.gidB, diff.snapB.gidE);
            auto it = m.find(key);
            if (it == m.end()) {
                auto pair = m.emplace(key, std::vector<MetaDiff>());
                pair.first->second.push_back(diff);
            } else {
                it->second.push_back(diff);
            }
        }
        return m;
    }
    void gc() {
        std::vector<std::string> u;
        auto m = groupBySnapB();
        for (auto &p : m) {
            std::vector<MetaDiff> &v = p.second;
            assert(!v.empty());
            MetaDiff maxDiff = v[0];
            for (size_t i = 1; i < v.size(); i++) {
                if (maxDiff.snapE.gidB < v[i].snapE.gidB) {
                    maxDiff = v[i];
                }
            }
            std::cout << "maxDiff " << maxDiff << std::endl; // debug
            if (maxDiff.isClean()) {
                auto v1 = getIncludes(maxDiff);
                for (auto &d : v1) {
                    u.push_back(createDiffFileName(d));
                    std::cout << "gc " << d << std::endl; // debug
                    auto it = search(d);
                    mmap_.erase(it);
                }
            }
        }
        removeFiles(u);
    }
    Mmap::iterator search(const MetaDiff &diff) {
        auto it = mmap_.lower_bound(diff.snapB.gidB);
        while (it != mmap_.end()) {
            const MetaDiff &d = it->second;
            if (d.snapB.gidB != diff.snapB.gidB) break;
            if (d == diff) return it;
            ++it;
        }
        return mmap_.end();
    }
    std::vector<MetaDiff> getIncludes(const MetaDiff &diffC) const {
        if (diffC.isDirty()) return {};
        auto it = mmap_.lower_bound(diffC.snapB.gidB);
        std::vector<MetaDiff> v;
        while (it != mmap_.cend()) {
            const MetaDiff &d = it->second;
            if (diffC.snapE.gidE <= d.snapB.gidB) break;
            if (d != diffC && d.isClean() && includes(diffC, d)) {
                v.push_back(d);
            }
            ++it;
        }
        return v;
    }
    /**
     * Remove wdiffs before a specified gid.
     */
    void removeBeforeGid(uint64_t gid) {
        std::vector<std::string> v;
        Mmap::iterator it = mmap_.begin();
        while (it != mmap_.end()) {
            MetaDiff &diff = it->second;
            if (diff.snapE.gidE <= gid) {
                it = mmap_.erase(it);
                v.push_back(createDiffFileName(diff));
            } else {
                ++it;
            }
        }
        removeFiles(v);
    }
    /**
     * List of wdiff name in order of gid.
     * RETURN:
     *   list of wdiff name.
     *   the list does not contain overlapped ones.
     */
    std::vector<std::string> listName() const {
        return convertDiffToName(listDiff());
    }
    std::vector<std::string> listName(uint64_t gid0, uint64_t gid1) const {
        return convertDiffToName(listDiff(gid0, gid1));
    }
    /**
     * RETURN:
     *   list of diff.
     */
    std::vector<MetaDiff> listDiff(uint64_t gid0, uint64_t gid1) const {
        assert(gid0 < gid1);
        Mmap::const_iterator it = mmap_.lower_bound(gid0);
        if (it == mmap_.cend()) return {};
        std::vector<MetaDiff> v;
        while (it != mmap_.cend()) {
            MetaDiff diff = it->second;
            if (gid1 <= diff.snapB.gidB) {
                break;
            }
            if (diff.snapE.gidE <= gid1) {
                v.push_back(diff);
            }
            ++it;
        }
        return v;
    }
    std::vector<MetaDiff> listDiff() const {
        return listDiff(0, uint64_t(-1));
    }
    /**
     * Reload metadata by scanning directory entries.
     * searching "*.wdiff" files.
     */
    void reloadMetadata() {
        mmap_.clear();
        std::vector<cybozu::FileInfo> list;
        if (!cybozu::GetFileList(list, dir_.str(), "wdiff")) {
            throw std::runtime_error("GetFileList failed.");
        }
        for (cybozu::FileInfo &info : list) {
            if (info.name == "." || info.name == ".." || !info.isFile)
                continue;
            MetaDiff diff = parseDiffFileName(info.name);
            mmap_.emplace(diff.snapB.gidB, diff);
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
     * Get first diff.
     * @gid start position.
     * Diffs that has smallest diff.snapB.gidB and they are
     * the same snapB.
     */
    std::vector<MetaDiff> getFirstDiffs(uint64_t gid = 0) const {
        std::vector<MetaDiff> v;
        auto it = mmap_.lower_bound(gid);
        if (it == mmap_.cend()) return {};
        MetaDiff diff = it->second;
        MetaSnap snapB = diff.snapB;
        v.push_back(diff);
        ++it;
        while (it != mmap_.cend()) {
            MetaDiff diff = it->second;
            if (snapB.gidB != diff.snapB.gidB) {
                break;
            }
            if (snapB == diff.snapB) {
                v.push_back(diff);
            }
            ++it;
        }
        return v;
    }
    /**
     * Check MetaDiffs are contiguous.
     */
    static bool isContigous(std::vector<MetaDiff> &) {
#if 1
        return false;
#else
        if (diffV.empty()) return true;
        auto curr = diffV.cbegin();
        auto prev = curr;
        while (curr != diffV.cend()) {
            if (prev->snap1().gid0() != curr->snap0().gid0()) return false;
            prev = curr;
            ++curr;
        }
        return true;
#endif
    }
    /**
     * Remove diff files.
     * @pathStrV file name list (not full paths).
     */
    void removeFiles(const std::vector<std::string> &pathStrV) const {
        for (const std::string &pathStr : pathStrV) {
            cybozu::FilePath p = dir_ + cybozu::FilePath(pathStr);
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
