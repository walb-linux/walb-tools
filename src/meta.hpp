/**
 * @file
 * @brief Meta snapshot and diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <stdexcept>
#include <iostream>
#include "cybozu/serializer.hpp"

#ifndef WALB_TOOLS_META_HPP
#define WALB_TOOLS_META_HPP

namespace walb {

const uint16_t META_RECORD_PREAMBLE = 0x0311;

/**
 * Metadata record.
 */
struct meta_record
{
    uint16_t preamble;
    uint8_t is_snapshot; /* snapshot or diff. */
    uint8_t can_merge;
    uint32_t reserved0;
    uint64_t gid0;
    uint64_t gid1;
    uint64_t gid2;
    uint64_t lsid;
    uint64_t timestamp; /* unix time. */
} __attribute__((packed));

/**
 * Wrapper of struct meta_record.
 */
class MetaRecord
{
private:
    struct meta_record rec_;
public:
    MetaRecord() {
        init();
    }
    void init() {
        ::memset(&rec_, 0, sizeof(rec_));
        raw().preamble = META_RECORD_PREAMBLE;
    }
    struct meta_record &raw() { return rec_; }
    const struct meta_record &raw() const { return rec_; }
    void *rawData() { return &rec_; }
    const void *rawData() const { return &rec_; }
    size_t rawSize() const { return sizeof(rec_); }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::loadPod(rec_, is);
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::savePod(os, rec_);
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
    MetaDiff() : MetaRecord() {}
    /**
     * Dirty diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1, uint64_t gid2)
        : MetaRecord() {
        raw().is_snapshot = false;
        raw().gid0 = gid0;
        raw().gid1 = gid1;
        raw().gid2 = gid2;
        check();
    }
    /**
     * Clean diff.
     */
    MetaDiff(uint64_t gid0, uint64_t gid1)
        : MetaDiff(gid0, gid1, gid1) {
    }
    bool operator==(const MetaDiff &rhs) const {
        return gid0() == rhs.gid0()
            && gid1() == rhs.gid1()
            && gid2() == rhs.gid2();
    }
    bool operator!=(const MetaDiff &rhs) const {
        return !(*this == rhs);
    }
    bool isDirty() const { return gid1() != gid2(); }
    uint64_t gid0() const { return raw().gid0; }
    uint64_t gid1() const { return raw().gid1; }
    uint64_t gid2() const { return raw().gid2; }
    bool canMerge(const MetaDiff &diff) const {
#if 1
        /* This is a bit more strict check. */
        return gid1() == diff.gid0() && gid1() < diff.gid1();
#else
        return gid1() < diff.gid1();
#endif
    }
    MetaDiff merge(const MetaDiff &diff) const {
        assert(canMerge(diff));
#if 1
        return MetaDiff(gid0(), diff.gid1(), std::max(gid2(), diff.gid2()));
#else
        return MetaDiff(std::min(gid0(), diff.gid0()),
                        diff.gid1(), std::max(gid2(), diff.gid2()));
#endif
    }
    friend inline std::ostream &operator<<(std::ostream& os, const MetaDiff &d0) {
        os << d0.gid0() << ", " << d0.gid1() << ", " << d0.gid2() << std::endl;
        return os;
    }
private:
    void check() const {
        if (raw().is_snapshot) {
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
    MetaSnap() : MetaRecord() {}
    /**
     * Dirty snapshot.
     */
    MetaSnap(uint64_t gid0, uint64_t gid1)
        : MetaRecord() {
        raw().is_snapshot = true;
        raw().gid0 = gid0;
        raw().gid1 = gid1;
        check();
    }
    /**
     * Clean snapshot.
     */
    explicit MetaSnap(uint64_t gid)
        : MetaSnap(gid, gid) {
    }
    uint64_t gid0() const { return raw().gid0; }
    uint64_t gid1() const { return raw().gid1; }
    bool operator==(const MetaSnap &rhs) const {
        return gid0() == rhs.gid0()
            && gid1() == rhs.gid1();
    }
    bool operator!=(const MetaSnap &rhs) const {
        return !(*this == rhs);
    }
    bool isDirty() const { return gid0() != gid1(); }
    /**
     * Check a diff can be applied to the snapshot.
     */
    bool canApply(const MetaDiff &diff) const {
        return diff.gid0() <= gid0() && gid0() < diff.gid1();
    }
    MetaSnap apply(const MetaDiff &diff) const {
        assert(canApply(diff));
        return MetaSnap(diff.gid1(), std::max(gid1(), diff.gid2()));
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
private:
    void check() const {
        if (!raw().is_snapshot) {
            throw std::runtime_error("is_snapshot must be true.");
        }
        if (!(gid0() <= gid1())) {
            throw std::runtime_error("invalid MetaSnap.");
        }
    }
};

} //namespace walb

#endif /* WALB_TOOLS_META_HPP */
