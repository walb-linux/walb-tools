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
 * Use createDiffFileName() and parseDiffFileName()
 * to convert from/to a MetaDiff to/from its filename.
 *
 * Use canConsolidate() and consolidate() to consolidate diff list.
 */
namespace walb {

const uint16_t META_RECORD_PREAMBLE = 0x0311;

/**
 * Metadata record.
 */
struct meta_record
{
    uint16_t preamble;
    uint8_t is_snapshot; /* snapshot or diff. */
    uint8_t can_merge; /* no meaning for snapshot. */
    uint32_t reserved0;
    uint64_t gid0;
    uint64_t gid1;
    uint64_t gid2; /* no meaning for snapshot. */
    uint64_t lsid;
    uint64_t timestamp; /* unix time. */
} __attribute__((packed));

/**
 * Wrapper of struct meta_record.
 */
class MetaRecord
{
public:
    struct meta_record raw;
    MetaRecord() {
        init();
    }
    MetaRecord(const MetaRecord &rhs) : raw(rhs.raw) {}
    MetaRecord &operator=(const MetaRecord &rhs) {
        raw = rhs.raw;
        return *this;
    }
    virtual void init() {
        ::memset(&raw, 0, sizeof(raw));
        raw.preamble = META_RECORD_PREAMBLE;
    }
    void *rawData() { return &raw; }
    const void *rawData() const { return &raw; }
    size_t rawSize() const { return sizeof(raw); }
    void load(const void *data, size_t size) {
        if (size != sizeof(raw)) {
            throw std::runtime_error("bad size.");
        }
        ::memcpy(&raw, data, sizeof(raw));
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::loadPod(raw, is);
    }
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
class MetaDiff : public MetaRecord
{
public:
    MetaDiff() : MetaRecord() {
        raw.is_snapshot = false;
    }
    /**
     * Dirty diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1, uint64_t gid2)
        : MetaRecord() {
        raw.is_snapshot = false;
        raw.gid0 = gid0;
        raw.gid1 = gid1;
        raw.gid2 = gid2;
        check();
    }
    /**
     * Clean diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1)
        : MetaDiff(gid0, gid1, gid1) {
    }
    MetaDiff(const MetaDiff &rhs) : MetaRecord(rhs) {
    }
    MetaDiff &operator=(const MetaDiff &rhs) {
        MetaRecord::operator=(rhs);
        return *this;
    }
    void init() override {
        MetaRecord::init();
        raw.is_snapshot = false;
    }
    bool operator==(const MetaDiff &rhs) const {
        return gid0() == rhs.gid0()
            && gid1() == rhs.gid1()
            && gid2() == rhs.gid2();
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
    bool isClean() const { return gid1() == gid2(); }
    bool isDirty() const { return gid1() != gid2(); }
    uint64_t gid0() const { return raw.gid0; }
    uint64_t gid1() const { return raw.gid1; }
    uint64_t gid2() const { return raw.gid2; }
    bool canMerge(const MetaDiff &diff, bool ignoreCanMergeFlag = false) const {
        if (!ignoreCanMergeFlag && !diff.raw.can_merge) return false;
#if 1
        /* This is a bit more strict check. */
        return gid1() == diff.gid0() && gid1() < diff.gid1();
#else
        return gid1() < diff.gid1();
#endif
    }
    MetaDiff merge(const MetaDiff &diff, UNUSED bool ignoreCanMergeFlag = false) const {
        assert(canMerge(diff, ignoreCanMergeFlag));
#if 1
        MetaDiff ret(gid0(), diff.gid1(), std::max(gid2(), diff.gid2()));
#else
        MetaDiff ret(std::min(gid0(), diff.gid0()),
                     diff.gid1(), std::max(gid2(), diff.gid2()));
#endif
        ret.raw.can_merge = raw.can_merge;
        ret.raw.timestamp = diff.raw.timestamp;
        return ret;
    }
    friend inline std::ostream &operator<<(std::ostream& os, const MetaDiff &d0) {
        os << d0.gid0() << ", " << d0.gid1() << ", " << d0.gid2() << std::endl;
        return os;
    }
    void print(FILE *fp) const {
        ::fprintf(
            fp,
            "MetaDiff ts %" PRIu64 " gid %" PRIu64 " %" PRIu64 " %" PRIu64 " "
            "can_merge %d lsid %" PRIu64 "\n"
            , raw.timestamp, gid0(), gid1(), gid2()
            , raw.can_merge, raw.lsid);
    }
    void print() const {
        print(::stdout);
    }
private:
    void check() const {
        if (raw.is_snapshot) {
            throw std::runtime_error("is_snapshot must be false.");
        }
        if (!(gid0() < gid1() && gid1() <= gid2())) {
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
class MetaSnap : public MetaRecord
{
public:
    MetaSnap() : MetaRecord() {
        raw.is_snapshot = true;
    }
    /**
     * Dirty snapshot.
     */
    MetaSnap(uint64_t gid0, uint64_t gid1)
        : MetaRecord() {
        raw.is_snapshot = true;
        raw.gid0 = gid0;
        raw.gid1 = gid1;
        check();
    }
    /**
     * Clean snapshot.
     */
    explicit MetaSnap(uint64_t gid)
        : MetaSnap(gid, gid) {
    }
    MetaSnap(const MetaSnap &rhs) : MetaRecord(rhs) {
    }
    MetaSnap &operator=(const MetaSnap &rhs) {
        MetaRecord::operator=(rhs);
        return *this;
    }
    void init() override {
        MetaRecord::init();
        raw.is_snapshot = true;
    }
    uint64_t gid0() const { return raw.gid0; }
    uint64_t gid1() const { return raw.gid1; }
    bool operator==(const MetaSnap &rhs) const {
        return gid0() == rhs.gid0()
            && gid1() == rhs.gid1();
    }
    bool operator!=(const MetaSnap &rhs) const {
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
    bool isClean() const { return gid0() == gid1(); }
    bool isDirty() const { return gid0() != gid1(); }
    /**
     * Check a diff can be applied to the snapshot.
     */
    bool canApply(const MetaDiff &diff) const {
        return diff.gid0() <= gid0() && gid0() < diff.gid1();
    }
    /**
     * startToApply() and finishToApply() will do the same thing
     * as apply(). This is for consistency.
     */
    MetaSnap apply(const MetaDiff &diff) const {
        assert(canApply(diff));
        MetaSnap ret(diff.gid1(), std::max(gid1(), diff.gid2()));
        ret.raw.timestamp = diff.raw.timestamp;
        return ret;
    }
    MetaSnap startToApply(const MetaDiff &diff) const {
        assert(canApply(diff));
        return MetaSnap(gid0(), std::max(gid1(), diff.gid2()));
    }
    MetaSnap finishToApply(const MetaDiff &diff) const {
        assert(canApply(diff));
        assert(gid1() == std::max(gid1(), diff.gid2()));
        assert(diff.gid1() <= gid1());
        MetaSnap ret(diff.gid1(), gid1());
        ret.raw.timestamp = diff.raw.timestamp;
        return ret;
    }
    bool isTooNew(const MetaDiff &diff) const {
        return gid0() < diff.gid0();
    }
    bool isTooOld(const MetaDiff &diff) const {
        return diff.gid1() <= gid0();
    }
    friend inline std::ostream &operator<<(std::ostream& os, const MetaSnap &s0) {
        os << s0.gid0() << ", " << s0.gid1() << std::endl;
        return os;
    }
    void print(FILE *fp) const {
        ::fprintf(
            fp,
            "MetaSnap ts %" PRIu64 " gid %" PRIu64 " %" PRIu64 " "
            "can_merge %d lsid %" PRIu64 "\n"
            , raw.timestamp, gid0(), gid1()
            , raw.can_merge, raw.lsid);
    }
    void print() const {
        print(::stdout);
    }
private:
    void check() const {
        if (!raw.is_snapshot) {
            throw std::runtime_error("is_snapshot must be true.");
        }
        if (!(gid0() <= gid1())) {
            throw std::runtime_error("invalid MetaSnap.");
        }
    }
};

/**
 * RETURN:
 *   MetaSnap as a result of application of the given MetaDiff.
 */
static inline MetaSnap getSnapFromDiff(const MetaDiff &diff)
{
    MetaSnap snap;
    snap.init();
    snap.raw.gid0 = diff.gid1();
    snap.raw.gid1 = diff.gid2();
    snap.raw.timestamp = diff.raw.timestamp;
    snap.raw.lsid = diff.raw.lsid;
    return snap;
}

/**
 * @name input file name.
 * @diff will be set.
 * RETURN:
 *   false if parsing failed. diff may be updated partially.
 */
static inline bool parseDiffFileName(const std::string &name, MetaDiff &diff) {
    diff.init();
    const std::string minName("YYYYMMDDhhmmss-0-0-1.wdiff");
    std::string s = name;
    if (s.size() < minName.size()) {
        return false;
    }
    /* timestamp */
    std::string ts = s.substr(0, 14);
    diff.raw.timestamp = cybozu::strToUnixTime(ts);
    if (s[14] != '-') return false;
    /* can_merge */
    if (s[15] == '0') {
        diff.raw.can_merge = 0;
    } else if (s[15] == '1') {
        diff.raw.can_merge = 1;
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
        diff.raw.gid0 = gidV[0];
        diff.raw.gid1 = gidV[1];
        diff.raw.gid2 = gidV[1];
        break;
    case 3:
        diff.raw.gid0 = gidV[0];
        diff.raw.gid1 = gidV[1];
        diff.raw.gid2 = gidV[2];
        break;
    default:
        return false;
    }
    return diff.isValid();
}

/**
 * Create a diff file name.
 */
static inline std::string createDiffFileName(const MetaDiff &diff) {
    assert(diff.isValid());
    std::string s;
    s += cybozu::unixTimeToStr(diff.raw.timestamp);
    s.push_back('-');
    s.push_back(diff.raw.can_merge ? '1' : '0');
    s.push_back('-');
    s += cybozu::util::intToHexStr(diff.gid0());
    s.push_back('-');
    s += cybozu::util::intToHexStr(diff.gid1());
    if (diff.isDirty()) {
        s.push_back('-');
        s += cybozu::util::intToHexStr(diff.gid2());
    }
    s += ".wdiff";
    return s;
}

/**
 * Check whether a list of meta diffs can be consolidated.
 */
static inline bool canConsolidate(const std::vector<MetaDiff> &diffV) {
    if (diffV.empty()) return false;
    MetaDiff diff = diffV[0];
    for (size_t i = 1; i < diffV.size(); i++) {
        if (!diff.canMerge(diffV[i])) return false;
        diff = diff.merge(diffV[i]);
    }
    return true;
}

/**
 * Consolidate meta diff list.
 */
static inline MetaDiff consolidate(const std::vector<MetaDiff> &diffV) {
    assert(!diffV.empty());
    MetaDiff diff = diffV[0];
    for (size_t i = 1; i < diffV.size(); i++) {
        assert(diff.canMerge(diffV[i]));
        diff = diff.merge(diffV[i]);
    }
    return diff;
}

} //namespace walb
