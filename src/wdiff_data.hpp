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
#include "stdout_logger.hpp"
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
    MetaSnap latestRecord_;
    const bool isContiguous_;
    using Mmap = std::multimap<uint64_t, MetaDiff>;
    Mmap mmap_;

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
            reset(0);
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
            reset(0);
            return;
        }
        reloadMetadata();
    }
    ~WalbDiffFiles() noexcept {
        try {
            cleanup();
        } catch (...) {
        }
    }
    DISABLE_COPY_AND_ASSIGN(WalbDiffFiles);
    DISABLE_MOVE(WalbDiffFiles);

    /**
     * CAUSION:
     *   All data inside the directory will be removed.
     */
    void reset(uint64_t gid) {
        /* remove all wdiff files. */
        removeBeforeGid(uint64_t(-1));

        latestRecord_.init();
        latestRecord_.raw.gid0 = gid;
        latestRecord_.raw.gid1 = gid;
        saveLatestRecord();
    }
    /**
     * Add a diff and update the "latest" record file.
     * Before calling this, you must settle the diff file.
     */
    bool add(const MetaDiff &diff) {
        if (isContiguous_) {
            if (!latestRecord_.canApply(diff)) return false;
            latestRecord_ = latestRecord_.apply(diff);
        } else {
            if (diff.gid0() < latestRecord_.gid0()) return false;
            latestRecord_ = getSnapFromDiff(diff);
        }
        latestRecord_.raw.timestamp = diff.raw.timestamp;
        saveLatestRecord();
        mmap_.insert(std::make_pair(diff.gid0(), diff));
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
    bool consolidate(uint64_t gid0, uint64_t gid1) {
        assert(isContiguous_);
        Mmap::const_iterator it = mmap_.lower_bound(gid0);
        if (it == mmap_.cend()) return false;
        MetaDiff merged, diff;
        it = nextKey(it, diff);
        merged = diff;
        if (diff.gid0() != gid0) return false;
        it = skipOverlapped(it, diff.gid1());
        while (it != mmap_.cend()) {
            it = nextKey(it, diff);
            if (gid1 < diff.gid1()) break;
            if (!merged.canMerge(diff)) return false;
            merged = merged.merge(diff);
            merged.raw.timestamp = diff.raw.timestamp;
            it = skipOverlapped(it, diff.gid1());
        }
        assert(merged.gid0() == gid0);
        assert(merged.gid1() == gid1);
        mmap_.insert(std::make_pair(merged.gid0(), merged));
        return true;
    }
    /**
     * Search diffs which can be merged.
     * RETURN:
     *   A list of gid ranges each of which can be merged.
     */
    std::vector<std::pair<uint64_t, uint64_t> > getMergingCandidates() const {
        std::deque<MetaDiff> q;
        std::vector<std::pair<uint64_t, uint64_t> > v;
        auto insert = [&]() {
            uint64_t bgn = 0, end = 0;
            assert(!q.empty());
            bgn = q.front().gid0();
            while (!q.empty()) {
                end = q.front().gid1();
                q.pop_front();
            }
            v.push_back(std::make_pair(bgn, end));
        };
        for (const MetaDiff &diff : listDiff()) {
            bool canMerge = q.empty() ||
                (diff.raw.can_merge != 0 && q.back().canMerge(diff));
            if (canMerge) {
                q.push_back(diff);
            } else {
                insert();
                q.push_back(diff);
            }
        }
        if (!q.empty()) insert();
        return v;
    }
    /**
     * Get transfer candidates.
     * @size max total size [byte].
     */
    std::vector<MetaDiff> getTransferCandidates(uint64_t size) {
        std::vector<std::pair<uint64_t, uint64_t> > pV
            = getMergingCandidates();
        if (pV.empty()) return {};
        uint64_t gid0 = pV.front().first;
        uint64_t gid1 = pV.front().second;
        uint64_t total = 0;
        std::vector<MetaDiff> diffV;
        for (MetaDiff &diff : listDiff(gid0, gid1)) {
            uint64_t size0 = getDiffFileSize(diff);
            if (size < total + size0) break;
            diffV.push_back(diff);
            total += size0;
        }
        return diffV;
    }
    /**
     * Remove wdiffs before a specified gid.
     */
    void removeBeforeGid(uint64_t gid1) {
        std::vector<std::string> v;
        Mmap::iterator it = mmap_.begin();
        while (it != mmap_.end()) {
            MetaDiff &diff = it->second;
            if (diff.gid1() <= gid1) {
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
     *   the list does not contain overlapped ones.
     */
    std::vector<MetaDiff> listDiff(uint64_t gid0, uint64_t gid1) const {
        assert(gid0 < gid1);
        Mmap::const_iterator it = mmap_.lower_bound(gid0);
        if (it == mmap_.cend()) return {};
        std::vector<MetaDiff> v;
        MetaDiff diff;
        while (it != mmap_.cend()) {
            it = nextKey(it, diff);
            if (gid1 < diff.gid1()) break;
            v.push_back(diff);
            it = skipOverlapped(it, diff.gid1());
        }
        return v;
    }
    std::vector<MetaDiff> listDiff() const {
        return listDiff(0, uint64_t(-1));
    }
    /**
     * Get diff list of timestamp range.
     *
     * @ts0, @ts1: timestamp range [ts0, ts1).
     */
    std::vector<MetaDiff> listDiffInTimeRange(uint64_t ts0, uint64_t ts1) const {
        assert(ts0 < ts1);
        uint64_t gid0 = getMinGidAfterTime(ts0);
        uint64_t gid1 = getMaxGidBeforeTime(ts1);
        if (gid1 <= gid0) return {};
        return listDiff(gid0, gid1);
    }
    /**
     * Remove wdiff files which have been already merged.
     */
    void cleanup() {
        Mmap::iterator it;
        it = mmap_.begin();
        while (it != mmap_.end()) {
            MetaDiff diff;
            nextKey(it, diff);
            it = removeOverlapped(it, diff);
        }
    }
    MetaSnap latest() const {
        return latestRecord_;
    }
    /**
     * Get the latest gid in the directory.
     * You can add only a diff which gid0 is latestGid().
     */
    uint64_t latestGid() const {
        return latestRecord_.gid0();
    }
    /**
     * Get the oldest gid in the directory.
     */
    uint64_t oldestGid() const {
        if (mmap_.empty()) {
            return latestRecord_.gid0();
        } else {
            MetaDiff diff;
            nextKey(mmap_.begin(), diff);
            return diff.gid0();
        }
    }
    /**
     * Get the latest snapshot record.
     */
    MetaSnap latestSnap() const {
        return latestRecord_;
    }
    /**
     * Reload metadata by scanning directory entries.
     * searching "*.wdiff" files.
     */
    /**
     * See reloadMetadata();
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
            MetaDiff diff;
            if (!parseDiffFileName(info.name, diff)) continue;
            mmap_.insert(std::make_pair(diff.gid0(), diff));
        }
        check();
        if (mmap_.empty()) {
            loadLatestRecord();
        } else {
            MetaDiff diff;
            prevKey(mmap_.rbegin(), diff);
            latestRecord_ = getSnapFromDiff(diff);
            saveLatestRecord();
        }
    }
    const cybozu::FilePath &dirPath() const {
        return dir_;
    }
    /**
     * RETURN:
     *   maximum gid1 among wdiffs which timestmap are all before a given timestamp.
     */
    uint64_t getMaxGidBeforeTime(uint64_t timestamp) const {
        uint64_t gid1 = oldestGid();
        Mmap::const_iterator it = mmap_.cbegin();
        while (it != mmap_.cend()) {
            const MetaDiff &diff = it->second;
            if (diff.raw.timestamp <= timestamp && gid1 < diff.gid1()) {
                gid1 = diff.gid1();
            }
            ++it;
        }
        return gid1;
    }
    /**
     * RETURN:
     *   minimum gid0 among wdiffs which timestamp are all after a given timestamp.
     */
    uint64_t getMinGidAfterTime(uint64_t timestamp) const {
        uint64_t gid0 = latestGid();
        Mmap::const_iterator it = mmap_.cbegin();
        while (it != mmap_.cend()) {
            const MetaDiff &diff = it->second;
            if (timestamp <= diff.raw.timestamp && diff.gid0() < gid0) {
                gid0 = diff.gid0();
            }
            ++it;
        }
        return gid0;
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
     * Check MetaDiffs are contiguous.
     */
    static bool isContigous(std::vector<MetaDiff> &diffV) {
        if (diffV.empty()) return true;
        auto curr = diffV.cbegin();
        auto prev = curr;
        while (curr != diffV.cend()) {
            if (prev->gid1() != curr->gid0()) return false;
            prev = curr;
            ++curr;
        }
        return true;
    }
    /**
     * Check whether whole data of oldestGid() <= gid < latestGid() exist.
     * this is meaningfull only when isContiguous_ is true.
     */
    bool check() const {
        if (!isContiguous_ || mmap_.empty()) return true;
        Mmap::const_iterator it = mmap_.cbegin();
        MetaDiff diff;
        it = nextKey(it, diff);
        it = skipOverlapped(it, diff.gid1());
        while (it != mmap_.end()) {
            assert(diff.gid0() < it->first);
            if (diff.gid1() < it->first) {
                LOGw("There are a hole between gid %" PRIu64 " and %" PRIu64 ".", diff.gid1(), it->first);
                return false;
            }
            it = nextKey(it, diff);
            it = skipOverlapped(it, diff.gid1());
        }
        return true;
    }
    /**
     * @it must not indicate mmap_.end().
     * @diff the one with the gid0 and the maximum gid1 will be set.
     * RETURN:
     *   iterator which key are greather than the given.
     */
    template <typename Iterator>
    Iterator nextKey(Iterator it, MetaDiff &diff) const {
        assert(it != mmap_.end());
        diff = it->second;
        ++it;
        while (it != mmap_.end() && diff.gid0() == it->first) {
            if (diff.gid1() < it->second.gid1()) {
                diff = it->second;
            }
            ++it;
        }
        return it;
    }
    /**
     * get previous key using reverse iterator.
     */
    template <typename ReverseIterator>
    ReverseIterator prevKey(ReverseIterator it, MetaDiff &diff) const {
        assert(it != mmap_.rend());
        diff = it->second;
        ++it;
        while (it != mmap_.rend() && diff.gid0() == it->first) {
            if (diff.gid1() < it->second.gid1()) {
                diff = it->second;
            }
            ++it;
        }
        return it;
    }
    /**
     * Skip overlapped diffs which gid1 <= a given gid1.
     */
    template <typename Iterator>
    Iterator skipOverlapped(Iterator it, uint64_t gid1) const {
        while (it != mmap_.end() && it->second.gid1() <= gid1) {
            ++it;
        }
        return it;
    }
    /**
     * Latest record path.
     */
    cybozu::FilePath latestRecordPath() const {
        return dir_ + cybozu::FilePath("latest");
    }
    /**
     * Load latest snapshot record from the file.
     * RETURN:
     *   false if not found.
     */
    bool loadLatestRecord() {
        if (!latestRecordPath().stat().isFile()) return false;
        cybozu::util::FileReader reader(latestRecordPath().str(), O_RDONLY);
        cybozu::load(latestRecord_, reader);
        if (!latestRecord_.isValid()) {
            throw std::runtime_error("latestRecord is not valid.");
        }
        return true;
    }
    /**
     * Save latest snapshot record to the file.
     */
    void saveLatestRecord() const {
        cybozu::TmpFile tmpFile(dir_.str());
        cybozu::save(tmpFile, latestRecord_);
        tmpFile.save(latestRecordPath().str());
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
     * Remove overlapped diff files.
     *
     * @it iterator that indicates the first element of the target range.
     * @diff the function will remove diffs that are overwritten by it.
     *     the diff itself will not be removed.
     *
     * RETURN:
     *   An iterator follows of the next key.
     *   Its gid0 must be the gid1 of the given diff if valid
     *   (only when isContiguous_ is true).
     */
    Mmap::iterator removeOverlapped(Mmap::iterator it, const MetaDiff &diff) {
        std::vector<std::string> v;
        while (it != mmap_.end()) {
            MetaDiff &diff0 = it->second;
            assert(diff.gid0() <= diff0.gid0());
            if (diff.gid1() < diff0.gid1()) {
                break;
            }
            if (diff0 == diff) {
                /* Skip the given diff. */
                ++it;
                continue;
            }
            v.push_back(createDiffFileName(diff0));
            it = mmap_.erase(it);
        }
#ifdef DEBUG
        if (it != mmap_.end()) {
            MetaDiff &diff0 = it->second;
            if (isContiguous_) {
                assert(diff.gid1() == diff0.gid0());
            } else {
                assert(diff.gid1() <= diff0.gid0());
            }
        }
#endif
        removeFiles(v);
        return it;
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
