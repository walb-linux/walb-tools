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
    WalbDiffFiles(const std::string &dirStr)
        : dir_(dirStr), mmap_(), mutex_() {
        if (!dir_.stat().isDirectory()) {
            throw std::runtime_error("Directory does not exist.");
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
     * Merge gid range [gid0, gid1) into a wdiff file.
     *
     * Old wdiff files will not be removed.
     * You must call cleanup() explicitly.
     */
    void merge(uint64_t gid0, uint64_t gid1) {
        /* now editing */
    }
    /**
     * Add a diff and update the "latest" record file.
     */
    void add(const MetaDiff &diff) {
        /* now editing */
    }
    /**
     * Remove wdiffs before a specified gid.
     */
    void removeBeforeGid(uint64_t gid) {
        /* now editing */
    }
    /**
     * Remove wdiffs before a specified timestamp.
     */
    void removeBeforeTime(uint64_t ts) {
        /* now editing */
    }
    /**
     * List of wdiff name in order of gid.
     */
    std::vector<std::string> list() const {
        /* now editing */
        return {};
    }
    /**
     * Remove wdiff files which have been already merged.
     */
    void cleanup() {
        /* now editing */
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
     * Reload metadata by scanning directory entries.
     * searching "*.wdiff" files.
     */
    void reloadMetadata() {
        std::lock_guard<std::mutex> lk(mutex_);
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
        }
    }
private:
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
};

} //namespace walb

#endif /* WALB_TOOLS_WDIFF_DATA_HPP */
