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
#include <map>
#include <set>
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
    bool operator<(const MetaSnap &rhs) const {
        return std::make_pair(gidB, gidE) < std::make_pair(rhs.gidB, rhs.gidE);
    }
    bool operator<=(const MetaSnap &rhs) const {
        return std::make_pair(gidB, gidE) <= std::make_pair(rhs.gidB, rhs.gidE);
    }
    bool operator>(const MetaSnap &rhs) const {
        return std::make_pair(gidB, gidE) > std::make_pair(rhs.gidB, rhs.gidE);
    }
    bool operator>=(const MetaSnap &rhs) const {
        return std::make_pair(gidB, gidE) >= std::make_pair(rhs.gidB, rhs.gidE);
    }
    void set(uint64_t gid) {
        gidB = gid;
        gidE = gid;
    }
    void set(uint64_t gidB, uint64_t gidE) {
        this->gidB = gidB;
        this->gidE = gidE;
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

    /* This is timestamp of snapshot after applying the diff. */
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
    bool operator<(const MetaDiff &rhs) const {
        return std::make_pair(snapB, snapE) < std::make_pair(rhs.snapB, rhs.snapE);
    }
    bool operator<=(const MetaDiff &rhs) const {
        return std::make_pair(snapB, snapE) <= std::make_pair(rhs.snapB, rhs.snapE);
    }
    bool operator>(const MetaDiff &rhs) const {
        return std::make_pair(snapB, snapE) > std::make_pair(rhs.snapB, rhs.snapE);
    }
    bool operator>=(const MetaDiff &rhs) const {
        return std::make_pair(snapB, snapE) >= std::make_pair(rhs.snapB, rhs.snapE);
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
    std::string str(bool verbose = false) const {
        std::string b = snapB.str();
        std::string e = snapE.str();
        auto s = b + "-->" + e;
        if (verbose) {
            s += cybozu::util::formatString(
                " (%d %s)", canMerge ? 1 : 0, cybozu::unixTimeToStr(timestamp).c_str());
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
    uint64_t timestamp;
    MetaSnap snapB, snapE; /* snapE is meaningful when isApplying is true */

    MetaState()
        : preamble(META_STATE_PREAMBLE)
        , isApplying(false), timestamp(0)
        , snapB(), snapE() {}
    explicit MetaState(const MetaSnap &snap)
        : preamble(META_STATE_PREAMBLE)
        , isApplying(false), timestamp(0)
        , snapB(snap), snapE(snap) {}
    MetaState(const MetaSnap &snapB, const MetaSnap &snapE)
        : preamble(META_STATE_PREAMBLE)
        , isApplying(true), timestamp(0)
        , snapB(snapB), snapE(snapE) {}
    void set(const MetaSnap &snapB) {
        isApplying = false;
        this->snapB = snapB;
    }
    void set(const MetaSnap &snapB, const MetaSnap &snapE) {
        isApplying = true;
        this->snapB = snapB;
        this->snapE = snapE;
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
    return s;
}

inline bool canApply(const MetaSnap &snap, const std::vector<MetaDiff> &v)
{
    MetaSnap s = snap;
    for (const MetaDiff &d : v) {
        if (!canApply(s, d)) return false;
        s = apply(s, d);
    }
    return true;
}

inline MetaSnap apply(const MetaSnap &snap, const std::vector<MetaDiff> &v)
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

inline bool canMerge(const MetaDiff &diff0, const MetaDiff &diff1)
{
    return diff1.canMerge && canApply(diff0.snapE, diff1);
}

inline MetaDiff merge(const MetaDiff &diff0, const MetaDiff &diff1)
{
    MetaDiff ret;
    if (!canMerge(diff0, diff1)) {
        throw cybozu::Exception("merge:can not merge") << diff0 << diff1;
    }
    ret.snapB = diff0.snapB;
    ret.snapE = apply(diff0.snapE, diff1);
    ret.canMerge = diff0.canMerge;
    ret.timestamp = diff1.timestamp;
    return ret;
}

inline bool canMerge(const std::vector<MetaDiff> &v)
{
    if (v.empty()) return false;
    MetaDiff mdiff = v[0];
    for (size_t i = 1; i < v.size(); i++) {
        if (!canMerge(mdiff, v[i])) return false;
        mdiff = merge(mdiff, v[i]);
    }
    return true;
}

inline MetaDiff merge(const std::vector<MetaDiff> &v)
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

inline Relation getRelation(const MetaState &st, const MetaDiff &diff)
{
    Relation rel = getRelation(st.snapB, diff);
    if (!st.isApplying || rel != Relation::APPLICABLE_DIFF) return rel;
    // progress constraint check.
    if (st.snapE.gidB <= diff.snapE.gidB) {
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
    if (!canApply(st, diff)) {
        throw cybozu::Exception("applying:can not apply") << st << diff;
    }
    return MetaState(st.snapB, apply(st.snapB, diff));
}

inline MetaState apply(const MetaState &st, const MetaDiff &diff)
{
    if (!canApply(st, diff)) {
        throw cybozu::Exception("applying:can not apply") << st << diff;
    }
    return MetaState(apply(st.snapB, diff));
}

inline bool canApply(const MetaState &st, const std::vector<MetaDiff> &v)
{
    if (!canApply(st.snapB, v)) return false;
    const MetaSnap s = apply(st.snapB, v);
    if (st.isApplying && st.snapE.gidB > s.gidB) {
        // progress constraint broken.
        return false;
    }
    return true;
}

inline MetaState applying(const MetaState &st, const std::vector<MetaDiff> &v)
{
    if (!canApply(st, v)) {
        throw cybozu::Exception("applying:can not apply") << st << v.size();
    }
    return MetaState(st.snapB, apply(st.snapB, v));
}

inline MetaState apply(const MetaState &st, const std::vector<MetaDiff> &v)
{
    if (!canApply(st, v)) {
        throw cybozu::Exception("apply:can not apply") << st << v.size();
    }
    return MetaState(apply(st.snapB, v));
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
inline std::string createDiffFileName(const MetaDiff &diff)
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
 * Choose one diff from candidates with the maximum snapE.gidB.
 */
inline MetaDiff getMaxProgressDiff(const std::vector<MetaDiff> &v) {
    if (v.empty()) throw cybozu::Exception("getMaxProgressDiff:empty");
    MetaDiff diff = v[0];
    for (size_t i = 1; i < v.size(); i++) {
        if (diff.snapE.gidB < v[i].snapE.gidB) {
            diff = v[i];
        }
    }
    return diff;
}

/**
 * Multiple diffs manager.
 */
class MetaDiffManager
{
private:
    using Key = std::pair<uint64_t, uint64_t>; // diff.snapB
    using Mmap = std::multimap<Key, MetaDiff>;
    Mmap mmap_;

public:
    void add(const MetaDiff &diff) {
        uint64_t b = diff.snapB.gidB;
        uint64_t e = diff.snapB.gidE;
        if (search(diff) != mmap_.end()) {
            throw cybozu::Exception("MetaDiffManager::add:already exists") << diff;
        }
        mmap_.emplace(std::make_pair(b, e), diff);
    }
    void erase(const MetaDiff &diff, bool doesThrowError = false) {
        auto it = search(diff);
        if (it == mmap_.end()) {
            if (doesThrowError) {
                throw cybozu::Exception("MetaDiffManager::erase:not found") << diff;
            }
            return;
        }
        mmap_.erase(it);
    }

    /**
     * Garbage collect.
     *
     * Currently we avoid to erase dirty diffs,
     * because it is not confirmed that contains() works well with dirty diffs.
     *
     * RETURN:
     *   Removed diffs.
     */
    std::vector<MetaDiff> gc() {
        std::vector<MetaDiff> v;
        // Get clean diffs.
        for (const auto &p : mmap_) {
            const MetaDiff &d = p.second;
            if (d.isClean()) {
                v.push_back(d);
            }
        }
        std::set<MetaDiff> s;
        // This is simple but takes O(N^2) calculation cost.
        for (const MetaDiff &d0 : v) {
            for (const MetaDiff &d1 : v) {
                if (d0 != d1 && contains(d0, d1)) {
                    auto it = s.find(d1);
                    if (it != s.end()) continue;
                    erase(d1);
                    s.insert(d1);
                }
            }
        }
        return std::vector<MetaDiff>(s.begin(), s.end());
    }
    /**
     * Clear all diffs.
     */
    void clear() {
        mmap_.clear();
    }
    /**
     * Erase all diffs whose snapE.gidE is not greater than a specified gid.
     */
    std::vector<MetaDiff> eraseBeforeGid(uint64_t gid) {
        std::vector<MetaDiff> v;
        auto it = mmap_.begin();
        while (it != mmap_.end()) {
            const MetaDiff &d = it->second;
            if (gid <= d.snapB.gidB) {
                // There are no matching diffs after this.
                break;
            }
            if (d.snapE.gidE <= gid) {
                v.push_back(d);
                it = mmap_.erase(it);
            } else {
                ++it;
            }
        }
        return v;
    }
    /**
     * Erase too old diffs compared with a state.
     */
    std::vector<MetaDiff> eraseBefore(const MetaState &st) {
        return eraseBeforeGid(st.snapB.gidB);
    }
    /**
     * This is used for merge.
     */
    std::vector<MetaDiff> getMergeableDiffList(uint64_t gid, uint32_t limit = 0) const {
        std::vector<MetaDiff> v = getFirstDiffs(gid);
        if (v.empty()) return {};
        MetaDiff diff = getMaxProgressDiff(v);
        v = {diff};
        MetaDiff mdiff = diff;
        while (limit == 0 || v.size() < limit) {
            std::vector<MetaDiff> u = getMergeableCandidates(mdiff);
            if (u.empty()) break;
            diff = getMaxProgressDiff(u);
            v.push_back(diff);
            mdiff = merge(mdiff, diff);
        }
        return v;
    }
    /**
     * This is used for apply or virtual full scan.
     *
     * RETURN:
     *   applicable diff list.
     *   CAUSION: these may not be mergeable.
     */
    std::vector<MetaDiff> getApplicableDiffList(const MetaSnap &snap, uint32_t limit = 0) const {
        MetaSnap s = snap;
        std::vector<MetaDiff> v;
        while (limit == 0 || v.size() < limit) {
            std::vector<MetaDiff> u = getApplicableCandidates(s);
            if (u.empty()) break;
            MetaDiff d = getMaxProgressDiff(u);
            v.push_back(d);
            s = apply(s, d);
        }
        return v;
    }
    /**
     * Applicable and mergeable diff list.
     */
    std::vector<MetaDiff> getApplicableAndMergeableDiffList(const MetaSnap &snap, uint32_t limit = 0) const {
        std::vector<MetaDiff> v = getApplicableDiffList(snap, limit);
        if (v.empty()) return v;
        std::vector<MetaDiff> u;
        MetaDiff mdiff = v[0];
        for (size_t i = 1; i < v.size(); i++) {
            if (!canMerge(mdiff, v[i])) break;
            u.push_back(v[i]);
            mdiff = merge(mdiff, v[i]);
        }
        return u;
    }
    /**
     * Minimum number of diffs that are applicable.
     * This is useful for applying state.
     */
    std::vector<MetaDiff> getMinimumApplicableDiffList(const MetaState &st, uint32_t limit = 0) const {
        std::vector<MetaDiff> v = getApplicableDiffList(st.snapB, limit);
        if (v.empty()) return v;
        if (!st.isApplying) return {v[0]};

        MetaSnap snap = st.snapB;
        for (size_t i = 0; i < v.size(); i++) {
            snap = apply(snap, v[i]);
            if (st.snapE.gidB <= snap.gidB) {
                v.resize(i + 1);
                return v;
            }
        }
        throw cybozu::Exception("MetaDiffManager::getMinimumApplyingCandidates:not applicable");
    }
    /**
     * Get the latest snapshot.
     * Returned snapshot will be clean or dirty.
     * @st base state.
     */
    MetaSnap getLatestSnapshot(const MetaState &st) const {
        auto v0 = getMinimumApplicableDiffList(st);
        auto v1 = getApplicableDiffList(st.snapB);
        if (v1.size() < v0.size()) {
            throw cybozu::Exception("MetaDiffManager::getLatestSnapshot:size bug")
                << v0.size() << v1.size();
        }
        return apply(st.snapB, v1);
    }
    /**
     * Get the oldest clean snapshot
     * @st base state.
     */
    MetaSnap getOldestCleanSnapshot(const MetaState &st) const {
        auto v = getCleanSnapshotList(st);
        if (v.empty()) {
            throw cybozu::Exception("MetaDiffManager::getOldestCleanSnapshot:there is no clean snapshot");
        }
        return v[0];
    }
    /**
     * Get clean snapshot list sorted by gid.
     */
    std::vector<MetaSnap> getCleanSnapshotList(const MetaState &st) const {
        std::vector<MetaSnap> ret;
        if (!st.isApplying && st.snapB.isClean()) {
            ret.push_back(st.snapB);
        }
        std::vector<MetaDiff> v0, v1;
        v0 = getMinimumApplicableDiffList(st);
        v1 = getApplicableDiffList(st.snapB);
        if (v1.size() < v0.size()) {
            throw cybozu::Exception("MetaDiffManager::getCleanSnapshotList:size bug")
                << v0.size() << v1.size();
        }
        MetaSnap snap = st.snapB;
        size_t i = 0;
        while (i < v0.size()) {
            assert(v0[i] == v1[i]);
            snap = apply(snap, v0[i]);
            ++i;
        }
        if (snap.isClean()) ret.push_back(snap);
        while (i < v1.size()) {
            snap = apply(snap, v1[i]);
            if (snap.isClean()) ret.push_back(snap);
            ++i;
        }
        return ret;
    }
    /**
     * Get all diffs between gid0 and gid1.
     */
    std::vector<MetaDiff> getAll(uint64_t gid0 = 0, uint16_t gid1 = -1) const {
        if (gid0 >= gid1) {
            throw cybozu::Exception("MetaDiffManager::getAll:gid0 >= gid1")
                << gid0 << gid1;
        }
        std::vector<MetaDiff> v;
        for (const auto &p : mmap_) {
            const MetaDiff &d = p.second;
            if (gid1 < d.snapB.gidB) {
                // There is no matching diff after here.
                break;
            }
            if (gid0 <= d.snapB.gidB && d.snapE.gidE <= gid1) {
                v.push_back(d);
            }
        }
        return v;
    }
private:
    Key getKey(const MetaDiff &diff) const {
        return std::make_pair(diff.snapB.gidB, diff.snapB.gidE);
    }
    Mmap::iterator search(const MetaDiff &diff) {
        Key key = getKey(diff);
        Mmap::iterator it, end;
        std::tie(it, end) = mmap_.equal_range(key);
        if (it != end) {
            const MetaDiff &d = it->second;
            if (diff == d) return it;
            ++it;
        }
        return mmap_.end();
    }
    /**
     * Get first diffs;
     * @gid start position to search.
     *
     * RETURN:
     *   Diffs that has smallest diff.snapB.gidB but not less than a specified gid
     *   and they have the same snapB.
     */
    std::vector<MetaDiff> getFirstDiffs(uint64_t gid = 0) const {
        Key key0 = {gid, gid};
        auto it0 = mmap_.lower_bound(key0);
        if (it0 == mmap_.cend()) return {};
        const MetaDiff &d = it0->second;
        Key key1 = getKey(d);

        std::vector<MetaDiff> v;
        decltype(it0) it, it1;
        std::tie(it, it1) = mmap_.equal_range(key1);
        while (it != it1) {
            v.push_back(it->second);
            ++it;
        }
        return v;
    }
    std::vector<MetaDiff> getMergeableCandidates(const MetaDiff &diff) const {
        std::vector<MetaDiff> v;
        for (const auto &p : mmap_) {
            const MetaDiff &d = p.second;
            if (diff.snapE.gidE < d.snapB.gidB) {
                // There is no candidates after this.
                break;
            }
            if (diff != d && canMerge(diff, d)) {
                v.push_back(d);
            }
        }
        return v;
    }
    std::vector<MetaDiff> getApplicableCandidates(const MetaSnap &snap) const {
        std::vector<MetaDiff> v;
        for (const auto &p : mmap_) {
            const MetaDiff &d = p.second;
            if (snap.gidE < d.snapB.gidB) {
                // There is no candidates after this.
                break;
            }
            if (canApply(snap, d)) {
                v.push_back(d);
            }
        }
        return v;
    }
};

} //namespace walb
