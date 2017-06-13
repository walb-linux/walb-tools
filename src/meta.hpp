#pragma once
/**
 * @file
 * @brief Meta snapshot and diff.
 */
#include <cstdio>
#include <cassert>
#include <stdexcept>
#include <iostream>
#include <cinttypes>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <functional>
#include <mutex>
#include "cybozu/serializer.hpp"
#include "util.hpp"
#include "time.hpp"
#include "walb_util.hpp"

/**
 * Backup facility of walb storage will generate a full image depicted as a MetaSnap
 * and contiguous diff images depicted as MetaDiff list.
 *
 * There are four basic struct types to manage archive data.
 *   MetaSnap: snapshot expression.
 *     denoted as snap, s, (gidB, gidE), or |B,E|.
 *     has gidB and gidE as members. gid is generation id. B means begin, E means end.
 *   MetaDiff: diff expression.
 *     denoted as diff, (snapB, snapE), or |B,E|-->|B',E'|.
 *     has snapB and snapE as members.
 *   MetaState: full image state.
 *     denoted as state, st, <|B,E|>, or <|B,E|-->|B',E'|>.
 *     constructed from one or two MetaSnap data.
 *     used by archive daemon.
 *   MetaLsidGid: corresponding lsid and gid.
 *     used by storage daemon.
 *
 * Wdiff file name format:
 *   [timestamp]-[can_merge]-[snapB.gidB]-[snapE.gidB].wdiff (clean diff)
 *   [timestamp]-[can_merge]-[snapB.gidB]-[snapB.gidE]-[snapE.gidB]-[snapE.gidE].wdiff (dirty diff)
 *   timestamp: YYYYMMDDhhmmss format.
 *   can_merge: 0 or 1.
 *   gid expression: non-negative integer (hex string without prefix "0x" here).
 *
 * Constraints:
 *   s.gidB <= s.gidE must be satisfied in a snapshot.
 *   If s.gidB == s.gidE then the snapshot s is clean, else dirty.
 *   A diff (sB, sE) is called clean only if both sB and sE are clean.
 *   sB.gidB < sE.gidB must be satisfied in a diff (progress constraint).
 *   For every snapshot, there are just one applicable diff
 *   if there is not merged diffs (unique diff constraint).
 *
 * Application rule:
 *   See getRelation(), canMerge(), applying(), and apply() for detail.
 *
 * Merging rule:
 *   See canMerge() and merge() for detail.
 *
 * MetaDiffManager:
 *   manager of multiple diffs.
 *
 * Utility functions:
 *   isAlreadyApplied()
 *   contains()
 *   getMaxProgressDiff()
 *     to choose the most preferable diff from applicable/mergeable candidates.
 *   createDiffFileName()/parseDiffFileName()
 *     to convert from/to a MetaDiff to/from diff filename.
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
    uint64_t gidB, gidE;

    MetaSnap()
        : gidB(-1), gidE(-1) {}
    explicit MetaSnap(uint64_t gid)
        : MetaSnap(gid, gid) {}
    explicit MetaSnap(std::initializer_list<uint64_t> l)
        : MetaSnap() {
        if (l.size() == 1) {
            gidB = gidE = *l.begin();
        } else if (l.size() == 2) {
            auto it = l.begin();
            gidB = *it; ++it;
            gidE = *it;
        } else {
            throw cybozu::Exception("MetaSnap:must have 1 or 2 arguments");
        }
        verify();
    }
    MetaSnap(uint64_t gidB, uint64_t gidE)
        : gidB(gidB), gidE(gidE) {
        verify();
    }
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
    void set(uint64_t gid) {
        gidB = gid;
        gidE = gid;
        verify();
    }
    void set(uint64_t gidB, uint64_t gidE) {
        this->gidB = gidB;
        this->gidE = gidE;
        verify();
    }
    void verify() const {
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
        uint16_t preamble;
        cybozu::load(preamble, is);
        if (preamble != META_SNAP_PREAMBLE) {
            throw cybozu::Exception("MetaSnap::check:wrong preamble") << preamble;
        }
        cybozu::load(gidB, is);
        cybozu::load(gidE, is);
        verify();
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, META_SNAP_PREAMBLE);
        cybozu::save(os, gidB);
        cybozu::save(os, gidE);
    }
};

/**
 * Diff record.
 */
struct MetaDiff
{
    bool isMergeable;
    bool isCompDiff; /* compared diff can not merge with any other diffs. */

    /* This is timestamp of snapshot after applying the diff. */
    uint64_t timestamp;

    MetaSnap snapB, snapE;

    uint64_t dataSize; /* Optional. */

    MetaDiff()
        : isMergeable(false), isCompDiff(false), timestamp(0), snapB(), snapE(), dataSize(0) {}
    MetaDiff(uint64_t snapBgid, uint64_t snapEgid, bool isMergeable = false, uint64_t ts = 0)
        : MetaDiff(MetaSnap(snapBgid), MetaSnap(snapEgid), isMergeable, ts) {}
    MetaDiff(std::initializer_list<uint64_t> b, std::initializer_list<uint64_t> e, bool isMergeable = false, uint64_t ts = 0)
        : MetaDiff(MetaSnap(b), MetaSnap(e), isMergeable, ts) {
        verify();
    }
    MetaDiff(const MetaSnap &snapB, const MetaSnap &snapE, bool isMergeable = false, uint64_t ts = 0)
        : isMergeable(isMergeable), isCompDiff(false), timestamp(ts)
        , snapB(snapB), snapE(snapE), dataSize(0) {
        verify();
    }
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
    void verify() const {
        if (snapB.gidB >= snapE.gidB) {
            throw cybozu::Exception("MetaDiff::broken progress constraint for B")
                << snapB.str() << snapE.str();
        }
        if (snapB.gidE > snapE.gidE) {
            throw cybozu::Exception("MetaDiff::broken progress constraint for E")
                << snapB.str() << snapE.str();
        }
    }
    std::string str(bool verbose = false) const {
        std::string b = snapB.str();
        std::string e = snapE.str();
        auto s = b + "-->" + e;
        if (verbose) {
            s += cybozu::util::formatString(
                " (%c%c %s %" PRIu64 ")"
                , isMergeable ? 'M' : '-'
                , isCompDiff ? 'C' : '-'
                , cybozu::unixTimeToPrettyStr(timestamp).c_str()
                , dataSize);
        }
        return s;
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
        uint16_t preamble;
        cybozu::load(preamble, is);
        if (preamble != META_DIFF_PREAMBLE) {
            throw cybozu::Exception("MetaDiff::check:wrong preamble") << preamble;
        }
        cybozu::load(isMergeable, is);
        cybozu::load(isCompDiff, is);
        cybozu::load(timestamp, is);
        cybozu::load(snapB, is);
        cybozu::load(snapE, is);
        cybozu::load(dataSize, is);
        verify();
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, META_DIFF_PREAMBLE);
        cybozu::save(os, isMergeable);
        cybozu::save(os, isCompDiff);
        cybozu::save(os, timestamp);
        cybozu::save(os, snapB);
        cybozu::save(os, snapE);
        cybozu::save(os, dataSize);
    }
    void merge(const MetaDiff &rhs);
};

using MetaDiffVec = std::vector<MetaDiff>;

inline std::ostream &operator<<(std::ostream &os, const MetaDiffVec &diffV)
{
    MetaDiffVec::const_iterator i = diffV.begin();
    if (i == diffV.end()) return os;
    os << i->str();
    ++i;
    while (i != diffV.end()) {
        os << ", " << i->str();
        ++i;
    }
    return os;
}

/**
 * Base lv state record.
 */
struct MetaState
{
    bool isApplying;
    uint64_t timestamp;
    MetaSnap snapB, snapE; /* snapE is meaningful when isApplying is true */

    bool isExplicit; // This is optional.

    MetaState()
        : isApplying(false), timestamp(0)
        , snapB(), snapE(), isExplicit(false) {}
    explicit MetaState(const MetaSnap &snap, uint64_t ts)
        : isApplying(false), timestamp(ts)
        , snapB(snap), snapE(snap), isExplicit(false) {
        verify();
    }
    MetaState(const MetaSnap &snapB, const MetaSnap &snapE, uint64_t ts)
        : isApplying(true), timestamp(ts)
        , snapB(snapB), snapE(snapE), isExplicit(false) {
        verify();
    }
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
    void verify() const {
        if (isApplying) {
            if (snapB.gidB >= snapE.gidB) {
                throw cybozu::Exception("MetaState::broken progress constraint")
                    << snapB.str() << snapE.str();
            }
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
    std::string strTs() const {
        return str() + "-" + cybozu::unixTimeToStr(timestamp);
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
        uint16_t preamble;
        cybozu::load(preamble, is);
        if (preamble != META_STATE_PREAMBLE) {
            throw cybozu::Exception("MetaState::check:wrong preamble") << preamble;
        }
        cybozu::load(isApplying, is);
        cybozu::load(timestamp, is);
        cybozu::load(snapB, is);
        cybozu::load(snapE, is);
        verify();
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, META_STATE_PREAMBLE);
        cybozu::save(os, isApplying);
        cybozu::save(os, timestamp);
        cybozu::save(os, snapB);
        cybozu::save(os, snapE);
    }
};

/**
 * LsidGid record.
 */
struct MetaLsidGid
{
    bool isMergeable;
    uint64_t timestamp; /* unix time */
    uint64_t lsid; /* log sequence id. */
    uint64_t gid; /* generation id. */

    MetaLsidGid(uint64_t lsid, uint64_t gid, bool isMergeable, uint64_t timestamp)
        : isMergeable(isMergeable), timestamp(timestamp), lsid(lsid), gid(gid) {
    }
    MetaLsidGid() : MetaLsidGid(-1, -1, false, 0) {
    }
    std::string str() const {
        std::string ts = util::timeToPrintable(timestamp);
        return cybozu::util::formatString(
            "LsidGid timestamp %s isMergeable %d lsid %" PRIu64 " gid %" PRIu64 ""
            , ts.c_str(), isMergeable, lsid, gid);
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
        uint16_t preamble;
        cybozu::load(preamble, is);
        if (preamble != META_LSIDGID_PREAMBLE) {
            throw cybozu::Exception("MetaLsidGid::check:wrong preamble") << preamble;
        }
        cybozu::load(isMergeable, is);
        cybozu::load(timestamp, is);
        cybozu::load(lsid, is);
        cybozu::load(gid, is);
    }
    /**
     * For cybozu serializer.
     */
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, META_LSIDGID_PREAMBLE);
        cybozu::save(os, isMergeable);
        cybozu::save(os, timestamp);
        cybozu::save(os, lsid);
        cybozu::save(os, gid);
    }
};

enum class Relation
{
    TOO_OLD_DIFF, TOO_NEW_DIFF, APPLICABLE_DIFF, NOT_APPLICABLE_DIFF,
};

inline Relation getRelation(const MetaSnap &s, const MetaDiff &d)
{
    if (d.snapB.gidB > s.gidB) {
        return Relation::TOO_NEW_DIFF;
    }
    if (d.snapE.gidB <= s.gidB) {
        return Relation::TOO_OLD_DIFF;
    }
    return Relation::APPLICABLE_DIFF;
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
    if (!canApply(snap, diff)) {
        throw cybozu::Exception("apply:can not apply") << snap << diff;
    }
    MetaSnap s;
    if (diff.isClean()) {
        s.gidB = diff.snapE.gidB;
        s.gidE = std::max(diff.snapE.gidB, snap.gidE);
    } else {
        s = diff.snapE;
    }
    s.verify();
    return s;
}

inline bool canApply(const MetaSnap &snap, const MetaDiffVec &v)
{
    MetaSnap s = snap;
    for (const MetaDiff &d : v) {
        if (!canApply(s, d)) return false;
        s = apply(s, d);
    }
    return true;
}

inline MetaSnap apply(const MetaSnap &snap, const MetaDiffVec &v)
{
    MetaSnap s = snap;
    for (const MetaDiff &d : v) {
        if (!canApply(s, d)) {
            throw cybozu::Exception("apply:can not apply") << s << d;
        }
        s = apply(s, d);
    }
    return s;
}

/**
 * d0 ++? d1 := d0.B.B <= d1.B.B <= d0.E.B < d1.E.B
 *              and not d0.isCompDiff
 *              and not d1.isCompDiff
 */
inline bool canMerge(const MetaDiff &d0, const MetaDiff &d1)
{
    if (!d1.isMergeable) return false;
    if (d0.isCompDiff || d1.isCompDiff) return false;
    return d0.snapB.gidB <= d1.snapB.gidB
        && d1.snapB.gidB <= d0.snapE.gidB
        && d0.snapE.gidB < d1.snapE.gidB;
}

/**
 * d0 +++ d1 := d0.B-->|d1.E.B,max(d1.E.B,d0.E.E)| if d1.is_clean
 *              d0.B-->d1.E                        otherwise
 */
inline MetaDiff merge(const MetaDiff &d0, const MetaDiff &d1)
{
    assert(canMerge(d0, d1));
    MetaDiff ret;
    ret.snapB = d0.snapB;
    ret.timestamp = d1.timestamp;
    ret.isMergeable = d0.isMergeable;
    if (d1.isClean()) {
        ret.snapE.gidB = d1.snapE.gidB;
        ret.snapE.gidE = std::max(d1.snapE.gidB, d0.snapE.gidE);
    } else {
        ret.snapE = d1.snapE;
    }
    return ret;
}

inline void MetaDiff::merge(const MetaDiff& rhs)
{
    *this = walb::merge(*this, rhs);
}

inline bool canMerge(const MetaDiffVec &v)
{
    if (v.empty()) return false;
    MetaDiff mdiff = v[0];
    for (size_t i = 1; i < v.size(); i++) {
        if (!canMerge(mdiff, v[i])) return false;
        mdiff = merge(mdiff, v[i]);
    }
    return true;
}

inline MetaDiff merge(const MetaDiffVec &v)
{
    if (v.empty()) {
        throw cybozu::Exception("merge:empty vector.");
    }
    MetaDiff mdiff = v[0];
    for (size_t i = 1; i < v.size(); i++) {
        if (!canMerge(mdiff, v[i])) {
            throw cybozu::Exception("merge:can not merge") << mdiff << v[i];
        }
        mdiff = merge(mdiff, v[i]);
    }
    return mdiff;
}

inline bool canApply(const MetaState &st, const MetaDiff &diff)
{
    const bool b = canApply(st.snapB, diff);
    if (st.isApplying) {
        return b && st.snapE.gidB <= diff.snapE.gidB;
    }
    return b;
}

inline MetaState beginApplying(const MetaState &st, const MetaDiff &diff)
{
    assert(canApply(st, diff));
    return MetaState(st.snapB, apply(st.snapB, diff), diff.timestamp);
}

inline MetaState endApplying(const MetaState &st, const MetaDiff &diff)
{
    assert(canApply(st, diff));
    return MetaState(apply(st.snapB, diff), diff.timestamp);
}

inline MetaState apply(const MetaState &st, const MetaDiff &diff)
{
    /*
     * Both will produce the same result.
     */
#if 0
    MetaState tmp = beginApplying(st, diff);
    return endApplying(tmp, diff);
#else
    return endApplying(st, diff);
#endif
}

inline bool canApply(const MetaState &st, const MetaDiffVec &v)
{
    if (!canApply(st.snapB, v)) return false;
    const MetaSnap s = apply(st.snapB, v);
    if (st.isApplying && st.snapE.gidB > s.gidB) {
        // progress constraint broken.
        return false;
    }
    return true;
}

inline MetaState beginApplying(const MetaState &st, const MetaDiffVec &v)
{
    assert(canApply(st, v));
    if (v.empty()) return st;
    return MetaState(st.snapB, apply(st.snapB, v), v.back().timestamp);
}

inline MetaState endApplying(const MetaState &st, const MetaDiffVec &v)
{
    assert(canApply(st, v));
    if (v.empty()) return st;
    return MetaState(apply(st.snapB, v), v.back().timestamp);
}

inline MetaState apply(const MetaState &st, const MetaDiffVec &v)
{
    /*
     * Both will produce the same result.
     */
#if 0
    MetaState tmp = beginApplying(st, v);
    return endApplying(tmp, v);
#else
    return endApplying(st, v);
#endif
}

/**
 * Check whether diffC contains diff or not.
 * If true, the diff can be deleted safely.
 *
 * For dirty diffs, it works also due to progress constraint.
 */
inline bool contains(const MetaDiff &diffC, const MetaDiff &diff)
{
    return diffC.snapB.gidB <= diff.snapB.gidB && diff.snapE.gidB <= diffC.snapE.gidB;
}


/**
 * @name input file name.
 * RETURN:
 *   parsed diff.
 */
MetaDiff parseDiffFileName(const std::string &name);


/**
 * Create a diff file name.
 */
std::string createDiffFileName(const MetaDiff &diff);


/**
 * Choose one diff from candidates with the maximum snapE.gidB.
 */
MetaDiff getMaxProgressDiff(const MetaDiffVec &v);


/**
 * MetaDiff management structure.
 */
using MetaDiffMmap = std::multimap<uint64_t, MetaDiff>; // key is (MetaDiff obj).snapB.gidB.

/**
 * Internal structure to search diffs.
 */
struct GidRange
{
    uint64_t gidB;
    uint64_t gidE;
    std::vector<MetaDiffMmap::iterator> its;

    /**
     * The object will be moved after calling this method.
     */
    std::list<GidRange> split(uint64_t gid) {
        assert(gidB < gid && gid < gidE);
        std::list<GidRange> li;
        li.push_back({gidB, gid, its}); // its will be copied.
        gidB = gid;
        li.push_back(std::move(*this));
        return li;
    }

    /**
     * for debug.
     */
    std::string str() const {
        std::stringstream ss;
        ss << cybozu::util::formatString("GidRange (%" PRIu64 ", %" PRIu64 ") %zu\n"
                                         , gidB, gidE, its.size());
        for (auto it : its) ss << "  " << it->second << "\n";
        return ss.str();
    }
    friend inline std::ostream &operator<<(std::ostream &os, const GidRange &obj) {
        os << obj.str();
        return os;
    }
};

/**
 * A structure to search ranges that includes a specified gid.
 *
 * range0    |------->|
 * range1        |---------->|
 * internal  |---|----|------|
 *            (1) (2)  (3)
 * (1) has a pointer to range0.
 * (2) has pointers to range0 and range1.
 * (3) has a pointer to range1.
 */
class GidRangeManager
{
private:
    using Map = std::map<uint64_t, GidRange>; // key: (GidRange obj).end.
    Map map_;
    using Mmap = MetaDiffMmap;
public:
    void add(Mmap::iterator it);
    void remove(Mmap::iterator it);
    std::vector<Mmap::iterator> search(uint64_t gid) const;
    void clear() { map_.clear(); }
    void print() const; // for debug.
    void validateExistence(Mmap::const_iterator it, int line) const; // for test.
private:
    std::list<GidRange> getRange(uint64_t gidB, uint64_t gidE);
    void putRange(std::list<GidRange>& rngL);
    void merge(std::list<GidRange>& rngL, GidRange&& rng);
};


/**
 * Multiple diffs manager.
 * This is thread-safe.
 */
class MetaDiffManager
{
private:
    using Mmap = MetaDiffMmap;
    Mmap mmap_;
    GidRangeManager rangeMgr_;

    mutable std::recursive_mutex mu_;
    using AutoLock = std::lock_guard<std::recursive_mutex>;

public:
    MetaDiffManager() = default;
    explicit MetaDiffManager(const std::string &)
        : MetaDiffManager() {}
    void add(const MetaDiff &diff) {
        AutoLock lk(mu_);
        addNolock(diff);
    }
    void erase(const MetaDiff &diff, bool doesThrowError = false) {
        AutoLock lk(mu_);
        eraseNolock(diff, doesThrowError);
    }
    void erase(const MetaDiffVec &diffV, bool doesThrowError = false) {
        AutoLock lk(mu_);
        for (const MetaDiff &diff : diffV) {
            eraseNolock(diff, doesThrowError);
        }
    }
    /**
     * diffV: an empty diff vector.
     *   This will be filled by only updated diffs corresponding to the enabled/disabled snapshots.
     *
     * RETURN:
     *   True when the snapshot for gid is found.
     */
    bool changeSnapshot(uint64_t gid, bool enable, MetaDiffVec &diffV);
    /**
     * Garbage collect.
     *
     * RETURN:
     *     removed diffs.
     */
    MetaDiffVec gc(const MetaSnap &snap);
    /**
     * Garbage collect in a gid range.
     * The diff just having the gid range will not be removed.
     * Use this after diff-repl-server execution.
     * This is lower cost than gc().
     */
    MetaDiffVec gcRange(uint64_t gidB, uint64_t gidE);
    /**
     * Clear all diffs.
     */
    void clear() {
        AutoLock lk(mu_);
        rangeMgr_.clear();
        mmap_.clear();
    }
    /**
     * Clear and add diffs.
     */
    void reset(const MetaDiffVec &v) {
        AutoLock lk(mu_);
        rangeMgr_.clear();
        mmap_.clear();
        for (const MetaDiff &d : v) {
            addNolock(d);
        }
    }
    /**
     * Erase all diffs whose snapE.gidB is not greater than a specified gid.
     */
    MetaDiffVec eraseBeforeGid(uint64_t gid);
    /**
     * Erase too old diffs compared with a state.
     */
    MetaDiffVec eraseBefore(const MetaState &st) {
        return eraseBeforeGid(st.snapB.gidB);
    }
    /**
     * Get mergeable diff list started by lower-bound search with a specified gid,
     * where all the diffs satisfy a specified predicate.
     */
    MetaDiffVec getMergeableDiffList(uint64_t gid, const std::function<bool(const MetaDiff &)> &pred) const;
    MetaDiffVec getMergeableDiffList(uint64_t gid) const {
        auto pred = [](const MetaDiff &) { return true; };
        return getMergeableDiffList(gid, pred);
    }
    bool getApplicableDiff(const MetaSnap &snap, MetaDiff& diff) const {
        AutoLock lk(mu_);
        return getApplicableDiffNolock(snap, diff);
    }
    /**
     * Get applicable diff list to a specified snapshot
     * where all the diffs and applied snapshot satisfy a specified predicate.
     */
    MetaDiffVec getApplicableDiffList(
        const MetaSnap &snap,
        const std::function<bool(const MetaDiff &, const MetaSnap &)> &pred) const;
    MetaDiffVec getApplicableDiffList(const MetaSnap &snap) const {
        auto pred = [](const MetaDiff &, const MetaSnap &) { return true; };
        return getApplicableDiffList(snap, pred);
    }
    MetaDiffVec getApplicableDiffListByGid(const MetaSnap &snap, uint64_t maxGid) const {
        auto pred = [&](const MetaDiff &, const MetaSnap &snap) {
            return snap.gidB <= maxGid;
        };
        return getApplicableDiffList(snap, pred);
    }
    MetaDiffVec getApplicableDiffListByTime(const MetaSnap &snap, uint64_t maxTimestamp) const {
        auto pred = [&](const MetaDiff &diff, const MetaSnap &) {
            return diff.timestamp <= maxTimestamp;
        };
        return getApplicableDiffList(snap, pred);
    }
    MetaDiffVec getApplicableAndMergeableDiffList(const MetaSnap &snap) const;
    /**
     * Minimum number of diffs that are applicable.
     * This is useful for applying state.
     */
    MetaDiffVec getMinimumApplicableDiffList(const MetaState &st) const {
        if (!st.isApplying) return {};
        return getApplicableDiffList(st.snapB, [&](const MetaDiff &, const MetaSnap &snap) {
                return snap.gidB <= st.snapE.gidB;
            });
    }
    /**
     * Get the latest snapshot.
     * Returned snapshot will be clean or dirty.
     * @st base state.
     */
    MetaSnap getLatestSnapshot(const MetaState &st) const {
        return apply(st.snapB, getApplicableDiffList(st.snapB));
    }
    /**
     * Get the latest meta state.
     * Returned meta state will be clean or dirty.
     * @st base state.
     */
    MetaState getLatestState(const MetaState &st) const {
        return apply(st, getApplicableDiffList(st.snapB));
    }
    /**
     * Get the oldest clean snapshot
     * @st base state.
     */
    uint64_t getOldestCleanSnapshot(const MetaState &st0) const {
        MetaState st = getOldestCleanState(st0);
        assert(!st.isApplying);
        assert(st.snapB.isClean());
        return st.snapB.gidB;
    }
    /**
     * Get the oldest clean meta state.
     */
    MetaState getOldestCleanState(const MetaState &st0) const;
    /**
     * Get clean snapshot list sorted by gid.
     */
    std::vector<uint64_t> getCleanSnapshotList(const MetaState &st) const;
    /**
     * @st base meta state.
     * @isAll if true, implicit clean snapshots will be added also.
     *
     * RETURN:
     *   MetaState list sorted by state.snapB.gidB.
     *   where all states satisfy state.isApplying is false, state.snapB.isClean() is true.
     */
    std::vector<MetaState> getRestorableList(const MetaState &st, bool isAll = false) const;
    void getTargetDiffLists(MetaDiffVec& applicableV, MetaDiffVec& minV, const MetaState &st, uint64_t gid) const;
    void getTargetDiffLists(MetaDiffVec& applicableV, MetaDiffVec& minV, const MetaState &st) const;
    /**
     * Get diff list to restore a clean snapshot specified by a gid.
     * RETURN:
     *   Empty vector means the clean snapshot can not be restored.
     */
    MetaDiffVec getDiffListToRestore(const MetaState& st, uint64_t gid) const {
        return getDiffListToSync(st, MetaSnap(gid));
    }
    /**
     * Get diff list to apply all diffs before a specified gid.
     * RETURN:
     *   Empty vector means there is no diff to apply.
     */
    MetaDiffVec getDiffListToApply(const MetaState &st, uint64_t gid) const {
        MetaDiffVec applicableV, minV;
        getTargetDiffLists(applicableV, minV, st, gid);
        if (minV.size() > applicableV.size()) return minV;
        return applicableV;
    }
    /**
     * Get diff list to reproduce a snapshot.
     * RETURN:
     *   Empty vector means the snapshot can not be reprodusable.
     */
    MetaDiffVec getDiffListToSync(const MetaState &st, const MetaSnap &snap) const;
    /**
     * Get all diffs between gid0 and gid1.
     */
    MetaDiffVec getAll(uint64_t gid0 = 0, uint64_t gid1 = -1) const;
    /**
     * Check existance of a diff.
     * Equality check uses MetaDiff::operator==().
     */
    bool exists(const MetaDiff& diff) const;
    bool empty() const {
        AutoLock lk(mu_);
        return mmap_.empty();
    }
    size_t size() const {
        AutoLock lk(mu_);
        return mmap_.size();
    }
    std::pair<uint64_t, uint64_t> getMinMaxGid() const;
    /**
     * For proxy.
     * If diff.snapB.gidB is max, it has the max diff.snapE.gidB.
     */
    bool getDiffWithMaxGid(MetaDiff& diff) const {
        AutoLock lk(mu_);
        if (mmap_.empty()) return false;
        diff = mmap_.crbegin()->second;
        return true;
    }
    /* for debug */
    void debug() {
        AutoLock lk(mu_);
        for (auto pair : mmap_) {
            ::printf("%s\n", pair.second.str().c_str());
        }
        rangeMgr_.print();
    }
    /**
     * Validate consistency of mmap_ and rangeMgr_.
     */
    void validateForTest(int line) const {
        AutoLock lk(mu_);
        auto it = mmap_.begin();
        while (it != mmap_.end()) {
            rangeMgr_.validateExistence(it, line);
            ++it;
        }
    }
private:
    void addNolock(const MetaDiff &diff);
    void eraseNolock(const MetaDiff &diff, bool doesThrowError = false);
    Mmap::iterator searchNolock(const MetaDiff &diff);
    /**
     * Get first diffs;
     * @gid start position to search.
     *
     * RETURN:
     *   Diffs that has smallest diff.snapB.gidB but not less than a specified gid
     *   and they have the same snapB.
     */
    MetaDiffVec getFirstDiffsNolock(uint64_t gid = 0) const;
    MetaDiffVec getMergeableCandidatesNolock(const MetaDiff &diff) const;
    MetaDiffVec getApplicableCandidatesNolock(const MetaSnap &snap) const;

    bool getApplicableDiffNolock(const MetaSnap &snap, MetaDiff& diff) const {
        MetaDiffVec u = getApplicableCandidatesNolock(snap);
        if (u.empty()) return false;
        diff = getMaxProgressDiff(u);
        return true;
    }

    template <typename Pred>
    bool fastSearchNolock(uint64_t gid0, uint64_t gid1, MetaDiffVec &v, Pred &&pred) const {
        assert(gid0 < gid1);
        size_t nr = 0;
        Mmap::const_iterator it, end;
        it = mmap_.lower_bound(gid0);
        end = mmap_.lower_bound(gid1);
        while (it != end) {
            const MetaDiff &d = it->second;
            if (pred(d)) {
                nr++;
                v.push_back(d);
            }
            ++it;
        }
        return nr > 0;
    }
};

namespace meta_local {

size_t findNonInt(const std::string &s, size_t i);
size_t parseMetaSnap(const std::string &s, size_t i, MetaSnap &snap);

} // namespace meta_local


MetaSnap strToMetaSnap(const std::string &s);
MetaState strToMetaState(const std::string &s);

} //namespace walb
