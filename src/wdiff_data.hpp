/**
 * @file
 * @brief Walb diff files management.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <map>
#include <mutex>
#include <vector>
#include <queue>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "cybozu/file.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "meta.hpp"
#include "time.hpp"
#include "logger.hpp"

#ifndef WALB_TOOLS_WDIFF_DATA_HPP
#define WALB_TOOLS_WDIFF_DATA_HPP

namespace walb {

/**
 * Manager for walb diff files.
 *
 * Wdiff file name format:
 *   [timestamp]-[can_merge]-[gid0]-[gid1].wdiff (clean diff)
 *   [timestamp]-[can_merge]-[gid0]-[gid1]-[gid2].wdiff (dirty diff)
 *   timestamp: YYYYMMDDhhmmss format.
 *   can_merge: 0 or 1.
 *   gid0, gid1, gid2: positive integer (hex without prefix "0x").
 *   gid0 < gid1. gid1 <= gid2.
 *
 * Merge rule:
 *   ts0-c-gid0-gid1.wdiff + ts1-0-gid1-gid2.wdiff --> ts1-c-gid0-gid2.wdiff
 *   ts0 <= ts1.
 *   See MetaDiff::canMerge() for detail.
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
    using Mmap = std::multimap<uint64_t, MetaDiff>;
    Mmap mmap_;
    mutable std::mutex mutex_; /* for mmap_. */

public:
    WalbDiffFiles(const std::string &dirStr, bool doesReset = false)
        : dir_(dirStr), mmap_(), mutex_() {
        if (!dir_.stat().exists()) {
            if (!dir_.mkdir()) {
                throw std::runtime_error("Reset (mkdir) failed.");
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
    /**
     * CAUSION:
     *   All data inside the directory will be removed.
     */
    void reset(uint64_t gid) {
        /* remove all wdiff files. */
        removeBeforeGid(uint64_t(-1));

        latestRecord_.init();
        latestRecord_.raw().gid0 = gid;
        latestRecord_.raw().gid1 = gid;
        saveLatestRecord();
    }
    /**
     * Add a diff and update the "latest" record file.
     * Before calling this, you must settle the diff file.
     */
    bool add(const MetaDiff &diff) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (!latestRecord_.canApply(diff)) return false;
        mmap_.insert(std::make_pair(diff.gid0(), diff));
        latestRecord_ = latestRecord_.apply(diff);
        latestRecord_.raw().timestamp = diff.raw().timestamp;
        saveLatestRecord();
        return true;
    }
    /**
     * Merge gid range [gid0, gid1) into a wdiff.
     *
     * You must call this after created the merged diff file.
     * Old wdiff files will not be removed.
     * You must call cleanup() explicitly.
     *
     * There must be diffs which gid0/gid1 is the given gid0/gid1.
     * and there is no diff which can_merge are false between them.
     *
     * RETURN:
     *   true if successfuly merged.
     *   or false.
     */
    bool merge(uint64_t gid0, uint64_t gid1) {
        std::lock_guard<std::mutex> lk(mutex_);
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
            merged.raw().timestamp = diff.raw().timestamp;
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
     *   A list of gid ranges.
     */
    std::vector<std::pair<uint64_t, uint64_t> > getMergeCandidates() const {
        std::lock_guard<std::mutex> lk(mutex_);
        std::queue<MetaDiff> q;
        std::vector<std::pair<uint64_t, uint64_t> > v;
        auto insert = [&]() {
            uint64_t bgn = 0, end = 0;
            assert(!q.empty());
            if (q.size() == 1) {
                q.pop();
                return;
            }
            bgn = q.front().gid0();
            while (!q.empty()) {
                end = q.front().gid1();
                q.pop();
            }
            v.push_back(std::make_pair(bgn, end));
        };
        for (const MetaDiff &diff : listDiffNolock()) {
            if (q.empty() || diff.raw().can_merge) {
                q.push(diff);
            } else {
                insert();
                q.push(diff);
            }
        }
        insert();
        return std::move(v);
    }
    /**
     * Remove wdiffs before a specified gid.
     */
    void removeBeforeGid(uint64_t gid) {
        std::lock_guard<std::mutex> lk(mutex_);
        removeBeforeGidNolock(gid);
    }
    /**
     * Remove wdiffs before a specified timestamp.
     */
    void removeBeforeTime(uint64_t timestamp) {
        std::lock_guard<std::mutex> lk(mutex_);
        uint64_t gid1 = getMaxGidBeforeTime(timestamp);
        removeBeforeGidNolock(gid1);
    }
    /**
     * List wdiff name before a given timestamp.
     */
    std::vector<std::string> listBeforeTime(uint64_t timestamp) const {
        std::lock_guard<std::mutex> lk(mutex_);
        return convertDiffToName(listDiffNolock(0, getMaxGidBeforeTime(timestamp)));
    }
    /**
     * List of wdiff name in order of gid.
     * RETURN:
     *   list of wdiff name.
     *   the list does not contain overlapped ones.
     */
    std::vector<std::string> listName() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return convertDiffToName(listDiffNolock());
    }
    std::vector<std::string> listName(uint64_t gid0, uint64_t gid1) const {
        std::lock_guard<std::mutex> lk(mutex_);
        return convertDiffToName(listDiffNolock(gid0, gid1));
    }
    /**
     * RETURN:
     *   list of diff.
     *   the list does not contain overlapped ones.
     */
    std::vector<MetaDiff> listDiff() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return listDiffNolock();
    }
    std::vector<MetaDiff> listDiff(uint64_t gid0, uint64_t gid1) const {
        std::lock_guard<std::mutex> lk(mutex_);
        return listDiffNolock(gid0, gid1);
    }
    /**
     * Remove wdiff files which have been already merged.
     */
    void cleanup() {
        std::lock_guard<std::mutex> lk(mutex_);
        Mmap::iterator it;
        it = mmap_.begin();
        while (it != mmap_.end()) {
            MetaDiff diff;
            nextKey(it, diff);
            it = removeOverlapped(it, diff);
        }
    }
    /**
     * Get the latest gid in the directory.
     * You can add only a diff which gid0 is latestGid().
     */
    uint64_t latestGid() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return latestRecord_.gid0();
    }
    /**
     * Get the oldest gid in the directory.
     */
    uint64_t oldestGid() const {
        std::lock_guard<std::mutex> lk(mutex_);
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
        std::lock_guard<std::mutex> lk(mutex_);
        return latestRecord_;
    }
    /**
     * Reload metadata by scanning directory entries.
     * searching "*.wdiff" files.
     */
    void reloadMetadata() {
        std::lock_guard<std::mutex> lk(mutex_);
        reloadMetadataNolock();
    }
    const cybozu::FilePath &dirPath() const {
        return dir_;
    }
    /**
     * Create a diff file name.
     */
    static std::string createDiffFileName(const MetaDiff &diff) {
        assert(diff.isValid());
        std::string s;
        s += cybozu::unixTimeToStr(diff.raw().timestamp);
        s.push_back('-');
        s.push_back(diff.raw().can_merge ? '1' : '0');
        s.push_back('-');
        s += cybozu::util::intToHexStr(diff.gid0());
        s.push_back('-');
        s += cybozu::util::intToHexStr(diff.gid1());
        if (diff.isDirty()) {
            s.push_back('-');
            s += cybozu::util::intToHexStr(diff.gid2());
        }
        s += ".wdiff";
        return std::move(s);
    }
    /**
     * @name input file name.
     * @diff will be set.
     * RETURN:
     *   false if parsing failed. diff may be updated partially.
     */
    static bool parseDiffFileName(const std::string &name, MetaDiff &diff) {
        diff.init();
        const std::string minName("YYYYMMDDhhmmss-0-0-1.wdiff");
        std::string s = name;
        if (s.size() < minName.size()) {
            return false;
        }
        /* timestamp */
        std::string ts = s.substr(0, 14);
        diff.raw().timestamp = cybozu::strToUnixTime(ts);
        if (s[14] != '-') return false;
        /* can_merge */
        if (s[15] == '0') {
            diff.raw().can_merge = 0;
        } else if (s[15] == '1') {
            diff.raw().can_merge = 1;
        } else {
            return false;
        }
        if (s[16] != '-') return false;
        s = s.substr(17);
        /* gid0, gid1, (gid2). */
        std::vector<uint64_t> gidV;
        for (int i = 0; i < 3; i++) {
            size_t n = s.find("-");
            if (n == std::string::npos) break;
            uint64_t gid;
            if (!cybozu::util::hexStrToInt<uint64_t>(s.substr(0, n), gid)) {
                return false;
            }
            gidV.push_back(gid);
            s = s.substr(n + 1);
        }
        size_t n = s.find(".wdiff");
        if (n == std::string::npos) return false;
        uint64_t gid;
        if (!cybozu::util::hexStrToInt<uint64_t>(s.substr(0, n), gid)) {
            return false;
        }
        gidV.push_back(gid);
        switch (gidV.size()) {
        case 2:
            diff.raw().gid0 = gidV[0];
            diff.raw().gid1 = gidV[1];
            diff.raw().gid2 = gidV[1];
            break;
        case 3:
            diff.raw().gid0 = gidV[0];
            diff.raw().gid1 = gidV[1];
            diff.raw().gid2 = gidV[2];
            break;
        default:
            return false;
        }
        return diff.isValid();
    }
private:
    /**
     * Check whether whole data of oldestGid() <= gid < latestGid() exist.
     */
    bool checkNolock() const {
        if (mmap_.empty()) return true;
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
        while (it != mmap_.rend() && diff.raw().gid0 == it->first) {
            if (diff.raw().gid1 < it->second.gid1()) {
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
     * See reloadMetadata();
     */
    void reloadMetadataNolock() {
        mmap_.clear();
        std::vector<cybozu::FileInfo> list;
        if (!cybozu::GetFileList(list, dir_.str(), "wdiff")) {
            throw std::runtime_error("GetFileList failed.");
        }
        for (cybozu::FileInfo &info : list) {
            MetaDiff diff;
            if (!parseDiffFileName(info.name, diff)) continue;
            mmap_.insert(std::make_pair(diff.gid0(), diff));
        }
        checkNolock();
        if (mmap_.empty()) {
            loadLatestRecord();
        } else {
            MetaDiff diff;
            prevKey(mmap_.rbegin(), diff);
            latestRecord_ = getSnapFromDiff(diff);
            saveLatestRecord();
        }
    }
    /**
     * Remove diff files.
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
     * See removeBeforeGid().
     */
    void removeBeforeGidNolock(uint64_t gid1) {
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
     * CAUSION:
     *   the lock must be held.
     * RETURN:
     *   maximum gid1 among wdiffs which timestmap are all before a given timestamp.
     */
    uint64_t getMaxGidBeforeTime(uint64_t timestamp) const {
        uint64_t gid1 = 0;
        Mmap::const_iterator it = mmap_.begin();
        while (it != mmap_.cend()) {
            const MetaDiff &diff = it->second;
            if (diff.raw().timestamp < timestamp && gid1 < diff.gid1()) {
                gid1 = diff.gid1();
            }
            ++it;
        }
        return gid1;
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
     *   Its gid0 must be the gid1 of the given diff if valid.
     */
    Mmap::iterator removeOverlapped(Mmap::iterator it, const MetaDiff &diff) {
        std::vector<std::string> v;
        while (it != mmap_.end() && it->second.gid1() <= diff.gid1()) {
            MetaDiff &diff0 = it->second;
            assert(diff.gid0() <= diff0.gid0());
            if (diff0 == diff) {
                ++it;
                continue;
            }
            v.push_back(createDiffFileName(diff0));
            it = mmap_.erase(it);
        }
#ifdef DEBUG
        if (it != mmap_.end()) {
            MetaDiff &diff0 = it->second;
            assert(diff.gid1() == diff0.gid0());
        }
#endif
        removeFiles(v);
        return it;
    }
    /**
     * See listDiff().
     */
    std::vector<MetaDiff> listDiffNolock(uint64_t gid0, uint64_t gid1) const {
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
        return std::move(v);
    }
    std::vector<MetaDiff> listDiffNolock() const {
        return listDiffNolock(0, uint64_t(-1));
    }
    /**
     * Convert diff list to name list.
     */
    static std::vector<std::string> convertDiffToName(const std::vector<MetaDiff> &diffV) {
        std::vector<std::string> v;
        for (const MetaDiff &diff : diffV) {
            v.push_back(createDiffFileName(diff));
        }
        return std::move(v);
    }
};

} //namespace walb

#endif /* WALB_TOOLS_WDIFF_DATA_HPP */
