/**
 * @file
 * @brief Walb diff files management.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <map>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "cybozu/file.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "meta.hpp"

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
    std::multimap<uint64_t, MetaDiff> mmap_;

public:
    WalbDiffFiles(const std::string &dirStr, const std::string &name)
        : dir_(dirStr), name_(name), mmap_() {
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
     *
     */
    void add(const SnapshotRecord &rec) {
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
     */
    uint64_t latestGid() const {
        /* now editing */
        return 0;
    }
    /**
     * Get the oldest gid in the directory.
     */
    uint64_t oldestGid() const {
        /* now editing */
        return 0;
    }
    /**
     * Check whether whole data of oldestGid() <= gid < latestGid() exist.
     */
    bool check() const {
        /* now editing */
        return false;
    }
    /**
     * Reload metadata by scanning directory entries.
     * searching "*.wdiff" files.
     */
    void reloadMetadata() {
        /* now editing */
    }
};

} //namespace walb

#endif /* WALB_TOOLS_WDIFF_DATA_HPP */
