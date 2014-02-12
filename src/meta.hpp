#pragma once
/**
 * @file
 * @brief Meta snapshot and diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <cassert>
#include <stdexcept>
#include <iostream>
#include <cinttypes>
#include <string>
#include <vector>
#include "cybozu/serializer.hpp"
#include "util.hpp"
#include "time.hpp"

/**
 * Wdiff file name format:
 *   [timestamp]-[can_merge]-[s0.gid0]-[s1.gid0].wdiff (clean diff)
 *   [timestamp]-[can_merge]-[s0.gid0]-[s0.gid1]-[s1.gid0]-[s1.gid1].wdiff (dirty diff)
 *   timestamp: YYYYMMDDhhmmss format.
 *   can_merge: 0 or 1.
 *   gid: non-negative integer (hex string without prefix "0x").
 *   s0 or s1 (generally s) indicates a snapshot. (s0, s1) indicates a diff.
 *
 *
 * TODO: rewrite the following constraints.
 * >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 * Constraints:
 *   s.gid0 <= s.gid1 must be satisfied.
 *   s0.gid1 < s1.gid0 must be satisfied.
 *   If s.gid0 == s.gid1 then the snapshot s is clean, else dirty.
 *   A diff (s0, s1) is called clean only if both s0 and s1 are clean.
 *
 * Apply rule:
 *   We can apply a diff (s1, s2) to a snapshot s0 where
 *   if s1 is dirty then
 *     s0 == s1 is required (strictly) or
 *     (s1.gid0() == s0.gid0() and s1.gid1() <= s0.gid1()).
 *     The latter condition is required for multiple calls of startToApply().
 *   else:
 *     s1.gid0 <= s0.gid0 is required.
 *     s1.gid0 == s0.gid0 is required for more strict setting.
 *   After applying, you will get the snapshot s3 where
 *     s3.gid0 = s2.gid0.
 *     s3.gid1 = max(s0.gid1, s2.gid1).
 *
 * Merge rule:
 *   ts0-c-s0-s1.wdiff + ts1-0-s2-s3.wdiff --> ts1-c-s3.wdiff
 *   ts0 <= ts1 must be satisfied.
 *   s1 and (s2, s3) must satisfy the apply rule.
 *   See MetaDiff::canMerge() for detail.
 *
 * Use createDiffFileName() and parseDiffFileName()
 * to convert from/to a MetaDiff to/from its filename.
 *
 * Use canConsolidate() and consolidate() to consolidate diff list.
 * <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
 */
namespace walb {

const uint16_t META_SNAP_PREAMBLE    = 0x0311;
const uint16_t META_DIFF_PREAMBLE    = 0x0312;
const uint16_t META_STATE_PREAMBLE   = 0x0313;
const uint16_t META_LSIDGID_PREAMBLE = 0x0314;

/**
 * Snapshot record.
 */
struct MetaSnap
{
    uint16_t preamble;
    uint64_t gidB, gidE;

    MetaSnap()
        : MetaSnap(-1) {}
    explicit MetaSnap(uint64_t gid)
        : MetaSnap(gid, gid) {}
    MetaSnap(uint64_t gidB, uint64_t gidE)
        : preamble(META_SNAP_PREAMBLE)
        , gidB(gidB), gidE(gidE) {}
    bool isClean() const {
        return gidB == gidE;
    }
    bool isDirty() const {
        return gidB != gidE;
    }
    bool operator==(const MetaSnap &rhs) const {
        return gidB == rhs.gidB && gidE == rhs.gidE;
    }
    bool operator!=(const MetaSnap &rhs) const {
        return gidB != rhs.gidB || gidE != rhs.gidE;
    }
    void check() const {
        if (preamble != META_SNAP_PREAMBLE) {
            throw cybozu::Exception("MetaSnap::check:wrong preamble") << preamble;
        }
        if (gidB > gidE) {
            throw cybozu::Exception("MetaSnap::check:must be gidB <= gidE") << gidB << gidE;
        }
    }
    std::string str() const {
        if (gidB == gidE) {
            return cybozu::util::formatString("|%" PRIu64 "|", gidB);
        } else {
            return cybozu::util::formatString("|%" PRIu64 ",%" PRIu64 "|", gidB, gidE);
        }
    }
    friend inline std::ostream &operator<<(std::ostream &os, const MetaSnap &snap) {
        os << snap.str();
        return os;
    }
    /**
     * For cybozu serializer.
     */
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(preamble, is);
        cybozu::load(gidB, is);
        cybozu::load(gidE, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, preamble);
        cybozu::save(os, gidB);
        cybozu::save(os, gidE);
    }
};

/**
 * Diff record.
 */
struct MetaDiff
{
    uint16_t preamble;
    bool canMerge;
    uint64_t timestamp;
    MetaSnap snapB, snapE;

    MetaDiff()
        : MetaDiff(MetaSnap(), MetaSnap()) {}
    MetaDiff(uint64_t snapBgid, uint64_t snapEgid)
        : MetaDiff(snapBgid, snapBgid, snapEgid, snapEgid) {}
    MetaDiff(uint64_t snapBgidB, uint64_t snapBgidE, uint64_t snapEgidB, uint64_t snapEgidE)
        : preamble(META_DIFF_PREAMBLE)
        , canMerge(false), timestamp(0)
        , snapB(snapBgidB, snapBgidE)
        , snapE(snapEgidB, snapEgidE) {}
    MetaDiff(const MetaSnap &snapB, const MetaSnap &snapE)
        : preamble(META_DIFF_PREAMBLE)
        , canMerge(false), timestamp(0)
        , snapB(snapB), snapE(snapE) {}
    bool operator==(const MetaDiff &rhs) const {
        return snapB == rhs.snapB && snapE == rhs.snapE;
    }
    bool operator!=(const MetaDiff &rhs) const {
        return snapB != rhs.snapB || snapE != rhs.snapE;
    }
    bool isClean() const {
        return snapB.isClean() && snapE.isClean();
    }
    bool isDirty() const {
        return snapB.isDirty() || snapE.isDirty();
    }
    void check() const {
        if (preamble != META_DIFF_PREAMBLE) {
            throw cybozu::Exception("MetaDiff::check:wrong preamble") << preamble;
        }
        snapB.check();
        snapE.check();
        if (snapB.gidB >= snapE.gidB) {
            throw cybozu::Exception("MetaDiff::broken progress constraint")
                << snapB.str() << snapE.str();
        }
    }
    std::string str() const {
        std::string b = snapB.str();
        std::string e = snapE.str();
        return b + "-->" + e;
    }
    friend inline std::ostream &operator<<(std::ostream &os, const MetaDiff &diff) {
        os << diff.str();
        return os;
    }
    /**
     * For cybozu serializer.
     */
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(preamble, is);
        cybozu::load(canMerge, is);
        cybozu::load(timestamp, is);
        cybozu::load(snapB, is);
        cybozu::load(snapE, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, preamble);
        cybozu::save(os, canMerge);
        cybozu::save(os, timestamp);
        cybozu::save(os, snapB);
        cybozu::save(os, snapE);
    }
};

/**
 * Base lv state record.
 */
struct MetaState
{
    uint16_t preamble;
    bool isApplying;
    MetaSnap snapB, snapE; /* snapE is meaningful when isApplying is true */

    MetaState()
        : preamble(META_STATE_PREAMBLE)
        , isApplying(false), snapB(), snapE() {
    }
    explicit MetaState(const MetaSnap &snap)
        : preamble(META_STATE_PREAMBLE)
        , isApplying(false)
        , snapB(snap), snapE(snap) {}
    MetaState(const MetaSnap &snapB, const MetaSnap &snapE)
        : preamble(META_STATE_PREAMBLE)
        , isApplying(true)
        , snapB(snapB), snapE(snapE) {}
    bool operator==(const MetaState &rhs) const {
        if (isApplying != rhs.isApplying) {
            return false;
        }
        if (snapB != rhs.snapB) {
            return false;
        }
        if (isApplying && snapE != rhs.snapE) {
            return false;
        }
        return true;
    }
    bool operator!=(const MetaState &rhs) const {
        return !operator==(rhs);
    }
    void check() const {
        if (preamble != META_STATE_PREAMBLE) {
            throw cybozu::Exception("MetaState::check:wrong preamble") << preamble;
        }
        snapB.check();
        snapE.check();
        if (isApplying && snapB.gidB >= snapE.gidB) {
            throw cybozu::Exception("MetaState::broken progress constraint")
                << snapB.str() << snapE.str();
        }
    }
    std::string str() const {
        std::string b = snapB.str();
        std::string e = snapE.str();
        if (isApplying) {
            return cybozu::util::formatString("<%s-->%s>", b.c_str(), e.c_str());
        } else {
            return cybozu::util::formatString("<%s>", b.c_str());
        }
    }
    friend inline std::ostream &operator<<(std::ostream &os, const MetaState &st) {
        os << st.str();
        return os;
    }
    /**
     * For cybozu serializer.
     */
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(preamble, is);
        cybozu::load(isApplying, is);
        cybozu::load(snapB, is);
        cybozu::load(snapE, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, preamble);
        cybozu::save(os, isApplying);
        cybozu::save(os, snapB);
        cybozu::save(os, snapE);
    }
};

/**
 * LsidGid record.
 */
struct MetaLsidGid
{
    uint16_t preamble;
    bool canMerge;
    uint64_t timestamp; /* unix time */
    uint64_t lsid; /* log sequence id. */
    uint64_t gid; /* generation id. */

    MetaLsidGid()
        : preamble(META_LSIDGID_PREAMBLE)
        , canMerge(false), timestamp(0), lsid(-1), gid(-1) {
    }
    void check() const {
        if (preamble != META_LSIDGID_PREAMBLE) {
            throw cybozu::Exception("MetaLsidGid::check:wrong preamble") << preamble;
        }
    }
    std::string str() const {
        std::string ts = cybozu::unixTimeToStr(timestamp);
        return cybozu::util::formatString(
            "LsidGid timestamp %s canMerge %d lsid %" PRIu64 " gid %" PRIu64 ""
            , ts.c_str(), canMerge, lsid, gid);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const MetaLsidGid &lg) {
        os << lg.str();
        return os;
    }
    /**
     * For cybozu serializer.
     */
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(preamble, is);
        cybozu::load(canMerge, is);
        cybozu::load(timestamp, is);
        cybozu::load(lsid, is);
        cybozu::load(gid, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, preamble);
        cybozu::save(os, canMerge);
        cybozu::save(os, timestamp);
        cybozu::save(os, lsid);
        cybozu::save(os, gid);
    }
};

enum class Relation
{
    TOO_OLD_DIFF, TOO_NEW_DIFF, APPLICABLE_DIFF, NOT_APPLICABLE_DIFF,
};

inline Relation getRelation(const MetaSnap &snap, const MetaDiff &diff)
{
    if (diff.isClean()) {
        if (diff.snapE.gidB <= snap.gidB) {
            return Relation::TOO_OLD_DIFF;
        } else if (snap.gidB < diff.snapB.gidB) {
            return Relation::TOO_NEW_DIFF;
        }
        return Relation::APPLICABLE_DIFF;
    }
    if (snap.gidB == diff.snapB.gidB && snap.gidE == diff.snapB.gidE) {
        return Relation::APPLICABLE_DIFF;
    }
    return Relation::NOT_APPLICABLE_DIFF;
}

inline bool canApply(const MetaSnap &snap, const MetaDiff &diff)
{
    return getRelation(snap, diff) == Relation::APPLICABLE_DIFF;
}

inline bool isTooOld(const MetaSnap &snap, const MetaDiff &diff)
{
    return getRelation(snap, diff) == Relation::TOO_OLD_DIFF;
}

inline bool isTooNew(const MetaSnap &snap, const MetaDiff &diff)
{
    return getRelation(snap, diff) == Relation::TOO_NEW_DIFF;
}

inline MetaSnap apply(const MetaSnap &snap, const MetaDiff &diff)
{
    MetaSnap ret;
    assert(canApply(snap, diff));
    if (diff.isClean()) {
        ret.gidB = diff.snapE.gidB;
        ret.gidE = std::max(diff.snapE.gidB, snap.gidE);
    } else {
        ret = diff.snapE;
    }
    return ret;
}

inline bool canMerge(const MetaDiff &diff0, const MetaDiff &diff1)
{
    return diff1.canMerge && canApply(diff0.snapE, diff1);
}

inline MetaDiff merge(const MetaDiff &diff0, const MetaDiff &diff1)
{
    MetaDiff ret;
    assert(canMerge(diff0, diff1));
    ret.snapB = diff0.snapB;
    ret.snapE = apply(diff0.snapE, diff1);
    ret.canMerge = diff0.canMerge;
    ret.timestamp = diff1.timestamp;
    return ret;
}

inline MetaDiff merge(const std::vector<MetaDiff> &diffV)
{
    if (diffV.empty()) {
        throw cybozu::Exception("merge:empty vector.");
    }
    MetaDiff mdiff = diffV[0];
    for (size_t i = 1; i < diffV.size(); i++) {
        if (!canMerge(mdiff, diffV[i])) {
            throw cybozu::Exception("merge:can not merge") << mdiff << diffV[i];
        }
        mdiff = merge(mdiff, diffV[i]);
    }
    return mdiff;
}

inline Relation getRelation(const MetaState &st, const MetaDiff &diff)
{
    if (st.isApplying) {
        if (diff.isClean()) {
            if (diff.snapB.gidB <= st.snapB.gidB &&
                st.snapE.gidB <= diff.snapE.gidB) {
                return Relation::APPLICABLE_DIFF;
            }
            return Relation::NOT_APPLICABLE_DIFF;
        }
        if (st.snapB == diff.snapB && st.snapE.gidB <= diff.snapE.gidB) {
            return Relation::APPLICABLE_DIFF;
        }
        return Relation::NOT_APPLICABLE_DIFF;
    }
    if (diff.isClean()) {
        if (st.snapB.gidB < diff.snapB.gidB) {
            return Relation::TOO_NEW_DIFF;
        } else if (diff.snapE.gidB <= st.snapB.gidB) {
            return Relation::TOO_OLD_DIFF;
        }
        return Relation::APPLICABLE_DIFF;
    }
    if (st.snapB == diff.snapB && st.snapB.gidB < diff.snapE.gidB) {
        return Relation::APPLICABLE_DIFF;
    }
    return Relation::NOT_APPLICABLE_DIFF;
}

inline bool canApply(const MetaState &st, const MetaDiff &diff)
{
    return getRelation(st, diff) == Relation::APPLICABLE_DIFF;
}

inline MetaState applying(const MetaState &st, const MetaDiff &diff)
{
    assert(canApply(st, diff));
    MetaState ret;
    ret.isApplying = true;
    ret.snapB = st.snapB;
    if (diff.isClean()) {
        ret.snapE.gidB = diff.snapE.gidB;
        if (st.isApplying) {
            ret.snapE.gidE = std::max(diff.snapE.gidB, st.snapE.gidE);
            return ret;
        }
        ret.snapE.gidE = std::max(diff.snapE.gidB, st.snapB.gidE);
        return ret;
    }
    ret.snapE = diff.snapE;
    return ret;
}

inline MetaState apply(const MetaState &st, const MetaDiff &diff)
{
    assert(canApply(st, diff));
    MetaState ret;
    ret = applying(st, diff);
    ret.isApplying = false;
    ret.snapB = ret.snapE;
    return ret;
}

/**
 * Check whether a diff is already applied or not.
 * If true, the diff can be deleted safely.
 * This is sufficient condition.
 */
inline bool isAlreadyApplied(const MetaSnap &snap, const MetaDiff &diff)
{
    return diff.snapE.gidE <= snap.gidB;
}

/**
 * Check whether diffC contains diff or not.
 * If true, the diff can be deleted safely.
 * This is sufficient condition.
 */
inline bool includes(const MetaDiff &diffC, const MetaDiff &diff)
{
    return diffC.snapB.gidE <= diff.snapB.gidB && diff.snapE.gidE <= diffC.snapE.gidB;
}

// QQQ


#if 0 // XXX

/**
 * Snapshot identifier.
 *
 * The snapshot has
 *   (1) all data at gid0.
 *   (2) partial data between gid0 <= gid < gid1.
 */
class MetaSnap
{
public:
    struct meta_snap raw;
    MetaSnap() { init(); }
    /**
     * Dirty snapshot.
     */
    MetaSnap(uint64_t gid0, uint64_t gid1) {
        init();
        setSnap(gid0, gid1);
        check();
    }
    /**
     * Clean snapshot.
     */
    explicit MetaSnap(uint64_t gid) : MetaSnap(gid, gid) {}
    MetaSnap(const MetaSnap &rhs) : raw(rhs.raw) {}
    MetaSnap &operator=(const MetaSnap &rhs) {
        raw = rhs.raw;
        return *this;
    }
    void init() {
        ::memset(&raw, 0, sizeof(raw));
        raw.preamble = META_SNAP_PREAMBLE;
    }
    uint64_t gid0() const { return raw.snap[0]; }
    uint64_t gid1() const { return raw.snap[1]; }
    bool canMerge() const { return raw.can_merge != 0; }
    uint64_t timestamp() const { return raw.timestamp; }
    uint64_t lsid() const { return raw.lsid; }
    bool operator==(const MetaSnap &rhs) const {
        return gid0() == rhs.gid0()
            && gid1() == rhs.gid1();
    }
    bool operator!=(const MetaSnap &rhs) const {
        return !(*this == rhs);
    }
    void check() const {
        if (raw.preamble != META_SNAP_PREAMBLE) {
            throw RT_ERR("invalid preample.");
        }
        if (!(gid0() <= gid1())) {
            throw RT_ERR("invalid MetaSnap (%" PRIu64 ", %" PRIu64 ")."
                         , gid0(), gid1());
        }
    }
    bool isValid() const {
        try {
            check();
            return true;
        } catch (...) {
            return false;
        }
    }
    bool isClean() const { return gid0() == gid1(); }
    bool isDirty() const { return gid0() != gid1(); }
    friend inline std::ostream &operator<<(std::ostream& os, const MetaSnap &s0) {
        os << "(" << s0.gid0() << ", " << s0.gid1() << ")" << std::endl;
        return os;
    }
    void print(FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s\n", str().c_str());
    }
    std::string str() const {
        return cybozu::util::formatString(
            "MetaSnap ts %" PRIu64 " gid %" PRIu64 " %" PRIu64 " "
            "lsid %" PRIu64 " can_merge %d"
            , timestamp(), gid0(), gid1(), lsid(), canMerge());
    }

    void setSnap(uint64_t gid0, uint64_t gid1) {
        raw.snap[0] = gid0;
        raw.snap[1] = gid1;
    }
    void setSnap(uint64_t gid0) { setSnap(gid0, gid0); }
    void setCanMerge(bool b) { raw.can_merge = b; }
    void setTimestamp(uint64_t ts) { raw.timestamp = ts; }
    void setLsid(uint64_t lsid) { raw.lsid = lsid; }

    const void *rawData() const { return &raw; }
    void *rawData() { return &raw; }
    size_t rawSize() const { return sizeof(raw); }

    /**
     * For cybozu serializer.
     */
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::loadPod(raw, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::savePod(os, raw);
    }
};

/**
 * Diff identifier.
 *
 * The diff has
 *   (1) all data for gid0 <= gid < gid1.
 *   (2) partial data for gid1 <= gid < gid2.
 */
class MetaDiff
{
private:
    struct meta_diff raw;
public:
    /**
     * Default constructor.
     */
    MetaDiff() { init(); }
    /**
     * Create a dirty diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1, uint64_t gid2, uint64_t gid3) {
        init();
        setSnap0(gid0, gid1);
        setSnap1(gid2, gid3);
        check();
    }
    /**
     * Clean a clean diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1) : MetaDiff(gid0, gid0, gid1, gid1) {}
    /**
     * Constract from two MetaSnap objects.
     */
    MetaDiff(const MetaSnap &s0, const MetaSnap &s1)
        : MetaDiff(s0.gid0(), s0.gid1(), s1.gid0(), s1.gid1()) {}
    /**
     * Copy constructor.
     */
    MetaDiff(const MetaDiff &rhs) : raw(rhs.raw) {}
    MetaDiff &operator=(const MetaDiff &rhs) {
        raw = rhs.raw;
        return *this;
    }
    void init() {
        ::memset(&raw, 0, sizeof(raw));
        raw.preamble = META_DIFF_PREAMBLE;
    }
    bool operator==(const MetaDiff &rhs) const {
        return snap0() == rhs.snap0() &&
            snap1() == rhs.snap1();
    }
    bool operator!=(const MetaDiff &rhs) const {
        return !(*this == rhs);
    }
    bool isValid() const {
        try {
            check();
            return true;
        } catch (...) {
            return false;
        }
    }
    MetaSnap snap0() const { return snapDetail(raw.snap0); }
    MetaSnap snap1() const { return snapDetail(raw.snap1); }

    bool isClean() const { return snap0().isClean() && snap1().isClean(); }
    bool isDirty() const { return !isClean(); }
    bool canMerge() const { return raw.can_merge != 0; }
    uint64_t timestamp() const { return raw.timestamp; }

    friend inline std::ostream &operator<<(std::ostream& os, const MetaDiff &d0) {
        os << "(" << d0.snap0() << ", " << d0.snap1() << ")" << std::endl;
        return os;
    }
    void print(FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s\n", str().c_str());
    }
    std::string str() const {
        return cybozu::util::formatString(
            "MetaDiff ts %" PRIu64 " gid %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " "
            "can_merge %d"
            , timestamp()
            , snap0().gid0(), snap0().gid1()
            , snap1().gid0(), snap1().gid1()
            , canMerge());
    }
    void check() const {
        if (raw.preamble != META_DIFF_PREAMBLE) {
            throw RT_ERR("invalid preamble.");
        }
        snap0().check();
        snap1().check();
        if (!(snap0().gid1() < snap1().gid0())) {
            throw RT_ERR("invalid metadiff: must be snap0.gid1 < snap1.gid0.");
        }
        if (snap0().isDirty() && !canMerge()) {
            throw RT_ERR("snap0 is dirty then canMerge must be true.");
        }
    }

    void setSnap0(uint64_t gid0, uint64_t gid1) {
        setSnapDetail(raw.snap0, gid0, gid1);
        if (snap0().isDirty()) setCanMerge(true);
    }
    void setSnap0(uint64_t gid0) { setSnap0(gid0, gid0); }
    void setSnap1(uint64_t gid0, uint64_t gid1) {
        setSnapDetail(raw.snap1, gid0, gid1);
    }
    void setSnap1(uint64_t gid0) { setSnap1(gid0, gid0); }
    void setTimestamp(uint64_t ts) { raw.timestamp = ts; }
    void setCanMerge(bool b) { raw.can_merge = b; }

    const void *rawData() const { return &raw; }
    void *rawData() { return &raw; }
    size_t rawSize() const { return sizeof(raw); }

    /**
     * For cybozu serializer.
     */
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::loadPod(raw, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::savePod(os, raw);
    }
private:
    MetaSnap snapDetail(const uint64_t snap[2]) const {
        MetaSnap s(snap[0], snap[1]);
        s.setTimestamp(timestamp());
        return s;
    }
    void setSnapDetail(uint64_t snap[2], uint64_t gid0, uint64_t gid1) {
        snap[0] = gid0;
        snap[1] = gid1;
    }
};

/**
 * Check a diff can be applied to the snapshot.
 */
static inline bool canApply(const MetaSnap &snap, const MetaDiff &diff)
{
    if (!snap.isValid()) return false;
    if (!diff.isValid()) return false;
    const MetaSnap &s0 = snap;
    const MetaSnap s1 = diff.snap0();
    UNUSED const MetaSnap s2 = diff.snap1();
    if (s1.isDirty()) {
        return s1.gid0() == s0.gid0() && s1.gid1() <= s0.gid1();
    }
#if 0
    /* 1st condition means there is no lack.
       2nd condition means there must progress. */
    return s1.gid0() <= s0.gid0() && s0.gid0() < s2.gid0();
#else
    /* More strict but simpler. */
    return s1.gid0() == s0.gid0();
#endif
}

/**
 * startToApply() and finishToApply() will do the same thing
 * as apply(). This is for consistency.
 */
static inline MetaSnap apply(const MetaSnap &snap, const MetaDiff &diff)
{
    assert(canApply(snap, diff));
    uint64_t gid0 = diff.snap1().gid0();
    uint64_t gid1 = std::max(snap.gid1(), diff.snap1().gid1());
    MetaSnap ret(gid0, gid1);
    ret.setTimestamp(diff.timestamp);
    return ret;
}

/**
 * Check two diffs can be merged.
 * @ignoreFlag specify true to ignore can_merge flag of diff1.
 */
static inline bool canMerge(const MetaDiff &diff0, const MetaDiff &diff1,
                            bool ignoreFlag = false)
{
    if (!diff0.isValid()) return false;
    if (!diff1.isValid()) return false;
    if (!ignoreFlag && !diff1.canMerge()) return false;
    return canApply(diff0.snap1(), diff1);
}

/**
 * Merge two diffs.
 */
static inline MetaDiff merge(const MetaDiff &diff0, const MetaDiff &diff1,
                             UNUSED bool ignoreFlag = false)
{
    assert(canMerge(diff0, diff1, ignoreFlag));
    MetaDiff ret(diff0.snap0(), apply(diff0.snap1(), diff1));
    ret.setCanMerge(diff0.canMerge());
    ret.setTimestamp(diff1.timestamp);
    return ret;
}

/**
 * If this function returns true on an archive,
 * the archive will accept the diff in the future.
 *
 * RETURN:
 *   true if the diff is too new to apply to the snap.
 */
static inline bool isTooNew(const MetaSnap &snap, const MetaDiff &diff)
{
    assert(!canApply(snap, diff));
    return snap.gid0() < diff.snap0().gid0(); /* There exists lack. */
}

/**
 * If the function returns true on an archive,
 * the archive never accept the diff
 * because it has received corresponding data already.
 *
 * RETURN:
 *   true if the diff is too old to apply to the snap.
 */
static inline bool isTooOld(const MetaSnap &snap, const MetaDiff &diff)
{
    assert(!canApply(snap, diff));
    return diff.snap1().gid0() <= snap.gid0(); /* There is no progress. */
}

/**
 * RETURN:
 *   MetaSnap as a result of application of the given MetaDiff.
 */
static inline MetaSnap getSnapFromDiff(const MetaDiff &diff)
{
    MetaSnap s = diff.snap1();
    s.setTimestamp(diff.timestamp());
    return s;
}

#endif // XXX

/**
 * @name input file name.
 * RETURN:
 *   parsed diff.
 */
inline MetaDiff parseDiffFileName(const std::string &name)
{
    MetaDiff diff;
    const std::string minName("YYYYMMDDhhmmss-0-0-1.wdiff");
    std::string s = name;
    if (s.size() < minName.size()) {
        throw cybozu::Exception("parseDiffFileName:too short name") << name;
    }
    /* timestamp */
    std::string ts = s.substr(0, 14);
    diff.timestamp = cybozu::strToUnixTime(ts);
    if (s[14] != '-') {
        throw cybozu::Exception("parseDiffFileName:parse failure1") << name;
    }
    /* can_merge */
    diff.canMerge = s[15] != '0';
    if (s[16] != '-') {
        throw cybozu::Exception("parseDiffFileName:parse failure2") << name;
    }
    s = s.substr(17);
    /* gid0, gid1(, gid2, gid3). */
    std::vector<uint64_t> gidV;
    for (int i = 0; i < 4; i++) {
        size_t n = s.find("-");
        if (n == std::string::npos) break;
        uint64_t gid;
        if (!cybozu::util::hexStrToInt<uint64_t>(s.substr(0, n), gid)) {
            throw cybozu::Exception("parseDiffFileName:hexStrToInt failure1") << name;
        }
        gidV.push_back(gid);
        s = s.substr(n + 1);
    }
    size_t n = s.find(".wdiff");
    if (n == std::string::npos) {
        throw cybozu::Exception("parseDiffFileName:wrong suffix") << name;
    }
    uint64_t gid;
    if (!cybozu::util::hexStrToInt<uint64_t>(s.substr(0, n), gid)) {
        throw cybozu::Exception("parseDiffFileName:hexStrToInt failure2") << name;
    }
    gidV.push_back(gid);
    switch (gidV.size()) {
    case 2:
        diff.snapB.gidB = gidV[0];
        diff.snapB.gidE = gidV[0];
        diff.snapE.gidB = gidV[1];
        diff.snapE.gidE = gidV[1];
        break;
    case 4:
        diff.snapB.gidB = gidV[0];
        diff.snapB.gidE = gidV[1];
        diff.snapE.gidB = gidV[2];
        diff.snapE.gidE = gidV[3];
        break;
    default:
        throw cybozu::Exception("parseDiffFileName:parse failure3") << name;
    }
    diff.check();
    return diff;
}

/**
 * Create a diff file name.
 */
static inline std::string createDiffFileName(const MetaDiff &diff)
{
    diff.check();
    std::vector<uint64_t> v;
    if (diff.isDirty()) {
        v.push_back(diff.snapB.gidB);
        v.push_back(diff.snapB.gidE);
        v.push_back(diff.snapE.gidB);
        v.push_back(diff.snapE.gidE);
    } else {
        v.push_back(diff.snapB.gidB);
        v.push_back(diff.snapE.gidB);
    }
    std::string s;
    s += cybozu::unixTimeToStr(diff.timestamp);
    s += '-';
    s += diff.canMerge ? '1' : '0';
    for (uint64_t gid : v) {
        s += '-';
        s += cybozu::util::intToHexStr(gid);
    }
    s += ".wdiff";
    return s;
}

/**
 * Check whether a list of meta diffs can be consolidated.
 */
static inline bool canConsolidate(const std::vector<MetaDiff> &diffV)
{
    if (diffV.empty()) return false;
    MetaDiff diff = diffV[0];
    for (size_t i = 1; i < diffV.size(); i++) {
        if (!canMerge(diff, diffV[i])) return false;
        diff = merge(diff, diffV[i]);
    }
    return true;
}

/**
 * Consolidate meta diff list.
 */
static inline MetaDiff consolidate(const std::vector<MetaDiff> &diffV)
{
    assert(!diffV.empty());
    MetaDiff diff = diffV[0];
    for (size_t i = 1; i < diffV.size(); i++) {
        assert(canMerge(diff, diffV[i]));
        diff = merge(diff, diffV[i]);
    }
    return diff;
}

} //namespace walb
