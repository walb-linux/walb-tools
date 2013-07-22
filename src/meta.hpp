/**
 * @file
 * @brief Meta snapshot and diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <stdexcept>

#ifndef WALB_TOOLS_META_HPP
#define WALB_TOOLS_META_HPP

namespace walb {

/**
 * Diff identifier.
 *
 * The diff has
 *   (1) all data for gid0 <= gid < gid1.
 *   (2) partial data for gid1 <= gid < gid2.
 */
struct MetaDiff
{
    const uint64_t gid0;
    const uint64_t gid1;
    const uint64_t gid2;
    /**
     * Clean diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1)
        : gid0(gid0)
        , gid1(gid1)
        , gid2(gid1) { check(); }
    /**
     * Dirty diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1, uint64_t gid2)
        : gid0(gid0)
        , gid1(gid1)
        , gid2(gid2) { check(); }
    bool operator==(const MetaDiff &rhs) const {
        return gid0 == rhs.gid0
            && gid1 == rhs.gid1
            && gid2 == rhs.gid2;
    }
    bool operator!=(const MetaDiff &rhs) const {
        return !(*this == rhs);
    }
    bool isDirty() const { return gid1 != gid2; }
    /**
     * This is a bit strict check.
     */
    bool canMerge(const MetaDiff &diff) const {
        return gid1 == diff.gid0;
    }
    MetaDiff merge(const MetaDiff &diff) const {
        assert(canMerge(diff));
        return MetaDiff(gid0, diff.gid1, std::max(gid2, diff.gid2));
    }
    void check() const {
        if (!(gid0 <= gid1 && gid1 <= gid2)) {
            throw std::runtime_error("invalid MetaDiff.");
        }
    }
};

/**
 * Snapshot identifier.
 *
 * The snapshot has
 *   (1) all data at gid0.
 *   (2) partial data for gid0 <= gid < gid1.
 */
struct MetaSnap
{
    const uint64_t gid0;
    const uint64_t gid1;
    /**
     * Clean snapshot.
     */
    explicit MetaSnap(uint64_t gid)
        : gid0(gid), gid1(gid) { check(); }
    /**
     * Dirty snapshot.
     */
    MetaSnap(uint64_t gid0a, uint64_t gid1a)
        : gid0(gid0a), gid1(gid1a) { check(); }
    bool operator==(const MetaSnap &rhs) const {
        return gid0 == rhs.gid0
            && gid1 == rhs.gid1;
    }
    bool operator!=(const MetaSnap &rhs) const {
        return !(*this == rhs);
    }
    bool isDirty() const { return gid0 != gid1; }
    /**
     * Check a diff can be applied to the snapshot.
     */
    bool canApply(const MetaDiff &diff) const {
        return diff.gid0 <= gid0 && gid0 < diff.gid1;
    }
    MetaSnap apply(const MetaDiff &diff) const {
        assert(canApply(diff));
        return MetaSnap(diff.gid1, std::max(gid1, diff.gid2));
    }
    bool isTooNew(const MetaDiff &diff) const {
        return gid0 < diff.gid0;
    }
    bool isTooOld(const MetaDiff &diff) const {
        return diff.gid1 <= gid0;
    }
    void check() const {
        if (!(gid0 <= gid1)) {
            throw std::runtime_error("invalid MetaSnap.");
        }
    }
};

} //namespace walb

#endif /* WALB_TOOLS_META_HPP */
