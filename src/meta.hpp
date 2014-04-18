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
#include <functional>
#include <mutex>
#include "cybozu/serializer.hpp"
#include "util.hpp"
#include "time.hpp"

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

    /* This is timestamp of snapshot after applying the diff. */
    uint64_t timestamp;

    MetaSnap snapB, snapE;

    MetaDiff()
        : isMergeable(false), timestamp(0), snapB(), snapE() {}
    MetaDiff(uint64_t snapBgid, uint64_t snapEgid, bool isMergeable = false, uint64_t ts = 0)
        : MetaDiff(MetaSnap(snapBgid), MetaSnap(snapEgid), isMergeable, ts) {}
    MetaDiff(std::initializer_list<uint64_t> b, std::initializer_list<uint64_t> e, bool isMergeable = false, uint64_t ts = 0)
        : MetaDiff(MetaSnap(b), MetaSnap(e), isMergeable, ts) {}
    MetaDiff(const MetaSnap &snapB, const MetaSnap &snapE, bool isMergeable = false, uint64_t ts = 0)
        : isMergeable(isMergeable), timestamp(ts)
        , snapB(snapB), snapE(snapE) {
        verify();
    }
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
    void verify() const {
        snapB.verify();
        snapE.verify();
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
                " (%d %s)", isMergeable ? 1 : 0, cybozu::unixTimeToStr(timestamp).c_str());
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
        cybozu::save(os, META_DIFF_PREAMBLE);
        cybozu::save(os, isMergeable);
        cybozu::save(os, timestamp);
        cybozu::save(os, snapB);
        cybozu::save(os, snapE);
    }
    void merge(const MetaDiff &rhs);
};

/**
 * Base lv state record.
 */
struct MetaState
{
    bool isApplying;
    uint64_t timestamp;
    MetaSnap snapB, snapE; /* snapE is meaningful when isApplying is true */

    MetaState()
        : isApplying(false), timestamp(0)
        , snapB(), snapE() {}
    explicit MetaState(const MetaSnap &snap, uint64_t ts)
        : isApplying(false), timestamp(ts)
        , snapB(snap), snapE(snap) {
        verify();
    }
    MetaState(const MetaSnap &snapB, const MetaSnap &snapE, uint64_t ts)
        : isApplying(true), timestamp(ts)
        , snapB(snapB), snapE(snapE) {
        verify();
    }
    void set(const MetaSnap &snapB) {
        isApplying = false;
        this->snapB = snapB;
        verify();
    }
    void set(const MetaSnap &snapB, const MetaSnap &snapE) {
        isApplying = true;
        this->snapB = snapB;
        this->snapE = snapE;
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
        snapB.verify();
        if (isApplying) {
            snapE.verify();
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
        std::string ts = cybozu::unixTimeToStr(timestamp);
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
const char *getRelationStr(Relation rel)
{
    switch (rel) {
    case Relation::TOO_OLD_DIFF: return "too-old-diff";
    case Relation::TOO_NEW_DIFF: return "too-new-diff";
    case Relation::APPLICABLE_DIFF: return "applicable-diff";
    case Relation::NOT_APPLICABLE_DIFF: return "not-applicable-diff";
    default:
        throw cybozu::Exception("getRelationStr:bad relation") << (int)rel;
    }
}

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
    s.verify();
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
    return diff1.isMergeable && canApply(diff0.snapE, diff1);
}

inline void MetaDiff::merge(const MetaDiff& rhs)
{
    if (!walb::canMerge(*this, rhs)) {
        throw cybozu::Exception("merge:can not merge") << *this << rhs;
    }
    snapE = apply(snapE, rhs);
    timestamp = rhs.timestamp;
    verify();
}

inline MetaDiff merge(const MetaDiff &diff0, const MetaDiff &diff1)
{
    MetaDiff ret = diff0;
    ret.merge(diff1);
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
    return MetaState(st.snapB, apply(st.snapB, diff), diff.timestamp);
}

inline MetaState apply(const MetaState &st, const MetaDiff &diff)
{
    if (!canApply(st, diff)) {
        throw cybozu::Exception("applying:can not apply") << st << diff;
    }
    return MetaState(apply(st.snapB, diff), diff.timestamp);
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
    return MetaState(st.snapB, apply(st.snapB, v), v.back().timestamp);
}

inline MetaState apply(const MetaState &st, const std::vector<MetaDiff> &v)
{
    if (!canApply(st, v)) {
        throw cybozu::Exception("apply:can not apply") << st << v.size();
    }
    return MetaState(apply(st.snapB, v), v.back().timestamp);
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
    /* isMergeable */
    diff.isMergeable = s[15] != '0';
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
    diff.verify();
    return diff;
}

/**
 * Create a diff file name.
 */
inline std::string createDiffFileName(const MetaDiff &diff)
{
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
    s += diff.isMergeable ? '1' : '0';
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
 * This is thread-safe.
 */
class MetaDiffManager
{
private:
    using Key = std::pair<uint64_t, uint64_t>; // diff.snapB
    using Mmap = std::multimap<Key, MetaDiff>;
    Mmap mmap_;

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
    void erase(const std::vector<MetaDiff> &diffV, bool doesThrowError = false) {
        AutoLock lk(mu_);
        for (const MetaDiff &diff : diffV) {
            eraseNolock(diff, doesThrowError);
        }
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
        AutoLock lk(mu_);
        std::vector<MetaDiff> v;
        // Get clean diffs.
        for (const auto &p : mmap_) {
            const MetaDiff &d = p.second;
            if (d.isClean()) {
                v.push_back(d);
            }
        }

        // This is O(NlogN) algorithm if O(d.snapB.gidE - d.snapB.gidB) is constant.
        std::multimap<uint64_t, MetaDiff> m;
        for (const MetaDiff &d : v) {
            m.emplace(d.snapB.gidB, d);
        }
        std::set<MetaDiff> s;
        for (const MetaDiff &d0 : v) {
            assert(d0.isClean());
            // All candidates exist in this range.
            auto itr = m.lower_bound(d0.snapB.gidB);
            auto end = m.upper_bound(d0.snapE.gidB);
            while (itr != end) {
                const MetaDiff &d1 = itr->second;
                assert(d1.isClean());
                if (d0 != d1 && contains(d0, d1)) {
                    erase(d1);
                    assert(s.find(d1) == s.end());
                    s.insert(d1);
                    itr = m.erase(itr);
                } else {
                    ++itr;
                }
            }
            assert(s.size() + m.size() == v.size());
        }
        return std::vector<MetaDiff>(s.begin(), s.end());
    }
    /**
     * Clear all diffs.
     */
    void clear() {
        AutoLock lk(mu_);
        mmap_.clear();
    }
    /**
     * Clear and add diffs.
     */
    void reset(const std::vector<MetaDiff> &v) {
        AutoLock lk(mu_);
        mmap_.clear();
        for (const MetaDiff &d : v) {
            addNolock(d);
        }
    }
    /**
     * Erase all diffs whose snapE.gidE is not greater than a specified gid.
     */
    std::vector<MetaDiff> eraseBeforeGid(uint64_t gid) {
        AutoLock lk(mu_);
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
     * Get mergeable diff list started by lower-bound search with a specified gid,
     * where all the diffs satisfy a specified predicate.
     */
    std::vector<MetaDiff> getMergeableDiffList(uint64_t gid, const std::function<bool(const MetaDiff &)> &pred) const {
        AutoLock lk(mu_);
        std::vector<MetaDiff> v = getFirstDiffs(gid);
        if (v.empty()) return {};
        MetaDiff diff = getMaxProgressDiff(v);
        v = {diff};
        MetaDiff mdiff = diff;
        for (;;) {
            std::vector<MetaDiff> u = getMergeableCandidates(mdiff);
            if (u.empty()) break;
            diff = getMaxProgressDiff(u);
            if (!pred(diff)) break;
            mdiff = merge(mdiff, diff);
            v.push_back(diff);
        }
        return v;
    }
    std::vector<MetaDiff> getMergeableDiffList(uint64_t gid) const {
        auto pred = [](const MetaDiff &) { return true; };
        return getMergeableDiffList(gid, pred);
    }
    /**
     * Get applicable diff list to a specified snapshot
     * where all the diffs and applied snapshot satisfy a specified predicate.
     */
    std::vector<MetaDiff> getApplicableDiffList(const MetaSnap &snap, const std::function<bool(const MetaDiff &, const MetaSnap &)> &pred) const {
        AutoLock lk(mu_);
        MetaSnap s = snap;
        std::vector<MetaDiff> v;
        for (;;) {
            std::vector<MetaDiff> u = getApplicableCandidates(s);
            if (u.empty()) break;
            MetaDiff d = getMaxProgressDiff(u);
            s = apply(s, d);
            if (!pred(d, s)) break;
            v.push_back(d);
        }
        return v;
    }
    std::vector<MetaDiff> getApplicableDiffList(const MetaSnap &snap) const {
        auto pred = [](const MetaDiff &, const MetaSnap &) { return true; };
        return getApplicableDiffList(snap, pred);
    }
    std::vector<MetaDiff> getApplicableDiffListByGid(const MetaSnap &snap, uint64_t maxGid) const {
        auto pred = [&](const MetaDiff &, const MetaSnap &snap) {
            return snap.gidB <= maxGid;
        };
        return getApplicableDiffList(snap, pred);
    }
    std::vector<MetaDiff> getApplicableDiffListByTime(const MetaSnap &snap, uint64_t maxTimestamp) const {
        auto pred = [&](const MetaDiff &diff, const MetaSnap &) {
            return diff.timestamp <= maxTimestamp;
        };
        return getApplicableDiffList(snap, pred);
    }
    /**
     * Minimum number of diffs that are applicable.
     * This is useful for applying state.
     */
    std::vector<MetaDiff> getMinimumApplicableDiffList(const MetaState &st) const {
        if (!st.isApplying) {
            size_t c = 0;
            return getApplicableDiffList(st.snapB, [&](const MetaDiff &, const MetaSnap &) {
                return (c++) == 0;
            });
        }
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
        std::vector<MetaDiff> v0, v1;
        {
            AutoLock lk(mu_);
            v0 = getMinimumApplicableDiffList(st);
            v1 = getApplicableDiffList(st.snapB);
        }
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
    uint64_t getOldestCleanSnapshot(const MetaState &st) const {
        const std::vector<uint64_t> v = getCleanSnapshotList(st);
        if (v.empty()) {
            throw cybozu::Exception("MetaDiffManager::getOldestCleanSnapshot:there is no clean snapshot");
        }
        return v[0];
    }
    /**
     * Get clean snapshot list sorted by gid.
     */
    std::vector<uint64_t> getCleanSnapshotList(const MetaState &st) const {
        std::vector<uint64_t> ret;
        if (!st.isApplying && st.snapB.isClean()) {
            ret.push_back(st.snapB.gidB);
        }
        std::vector<MetaDiff> v0, v1;
        {
            AutoLock lk(mu_);
            v0 = getMinimumApplicableDiffList(st);
            v1 = getApplicableDiffList(st.snapB);
        }
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
        if (snap.isClean()) ret.push_back(snap.gidB);
        while (i < v1.size()) {
            snap = apply(snap, v1[i]);
            if (snap.isClean()) ret.push_back(snap.gidB);
            ++i;
        }
        return ret;
    }
    void getTargetDiffLists(std::vector<MetaDiff>& applicableV, std::vector<MetaDiff>& minV, const MetaState &st, uint64_t gid) const {
        AutoLock lk(mu_);
        applicableV = getApplicableDiffListByGid(st.snapB, gid);
        // use this if timestamp
        // ret = getApplicableDiffListByTime(st.snapB, timestamp);
        if (applicableV.empty()) return;

        minV = getMinimumApplicableDiffList(st);
    }
    /**
     * Get diff list to restore a clean snapshot specified by a gid.
     * RETURN:
     *   Empty vector means the clean snapshot can not be restored.
     */
    std::vector<MetaDiff> getDiffListToRestore(const MetaState& st, uint64_t gid) const {
        std::vector<MetaDiff> applicableV, minV;
        getTargetDiffLists(applicableV, minV, st, gid);
        if (minV.size() > applicableV.size()) return {};

        const MetaState appliedSt = apply(st, applicableV);
        if (appliedSt.snapB.isClean() && appliedSt.snapB.gidB == gid) {
            return applicableV;
        } else {
            return {};
        }
    }
    /**
     * Get diff list to apply all diffs before a specified gid.
     * RETURN:
     *   Empty vector means there is no diff to apply.
     */
    std::vector<MetaDiff> getDiffListToApply(const MetaState &st, uint64_t gid) const {
        std::vector<MetaDiff> applicableV, minV;
        getTargetDiffLists(applicableV, minV, st, gid);
        if (minV.size() > applicableV.size()) return minV;
        return applicableV;
    }
    /**
     * Get all diffs between gid0 and gid1.
     */
    std::vector<MetaDiff> getAll(uint64_t gid0 = 0, uint64_t gid1 = -1) const {
        if (gid0 >= gid1) {
            throw cybozu::Exception("MetaDiffManager::getAll:gid0 >= gid1")
                << gid0 << gid1;
        }
        AutoLock lk(mu_);
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
    bool empty() const {
        AutoLock lk(mu_);
        return mmap_.empty();
    }
    size_t size() const {
        AutoLock lk(mu_);
        return mmap_.size();
    }
    std::pair<uint64_t, uint64_t> getMinMaxGid() const {
        uint64_t min = 0, max = 0;
        AutoLock lk(mu_);
        for (const auto &p : mmap_) {
            const MetaDiff &d = p.second;
            if (min < d.snapB.gidB) min = d.snapB.gidB;
            if (max < d.snapE.gidB) max = d.snapE.gidB;
        }
        return {min, max};
    }
private:
    void addNolock(const MetaDiff &diff) {
        uint64_t b = diff.snapB.gidB;
        uint64_t e = diff.snapB.gidE;

        if (search(diff) != mmap_.end()) {
            throw cybozu::Exception("MetaDiffManager::add:already exists") << diff;
        }
        mmap_.emplace(std::make_pair(b, e), diff);
    }
    void eraseNolock(const MetaDiff &diff, bool doesThrowError = false) {
        auto it = search(diff);
        if (it == mmap_.end()) {
            if (doesThrowError) {
                throw cybozu::Exception("MetaDiffManager::erase:not found") << diff;
            }
            return;
        }
        mmap_.erase(it);
    }
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
