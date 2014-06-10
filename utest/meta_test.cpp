#include "cybozu/test.hpp"
#include "cybozu/file.hpp"
#include "meta.hpp"
#include "random.hpp"

using namespace walb;

CYBOZU_TEST_AUTO(diff)
{
    MetaDiff d0({0, 0}, {1, 2}), d1(1, 2);
    d1.isMergeable = true;
    CYBOZU_TEST_ASSERT(canMerge(d0, d1));
    MetaDiff d2 = merge(d0, d1);
    CYBOZU_TEST_ASSERT(d2 == MetaDiff(0, 2));
}

CYBOZU_TEST_AUTO(snap)
{
    {
        MetaSnap s0(0, 7);
        MetaDiff d1({0, 0}, {10, 10});
        CYBOZU_TEST_ASSERT(apply(s0, d1) == MetaSnap(10, 10));
    }
    {
        MetaSnap s0(0, 10);
        MetaDiff d1({0, 0}, {10, 10});
        CYBOZU_TEST_ASSERT(apply(s0, d1) == MetaSnap(10, 10));
    }
    {
        MetaSnap s0(0, 13);
        MetaDiff d1({0, 13}, {10, 12});
        CYBOZU_TEST_ASSERT(apply(s0, d1) == MetaSnap(10, 12));
    }
}

CYBOZU_TEST_AUTO(serialize)
{
    MetaSnap s0(0), s1;
    MetaDiff d0(0, 1), d1;

    cybozu::File f0;
    f0.openW("test0.bin");
    cybozu::save(f0, s0);
    cybozu::save(f0, d0);
    f0.close();

    cybozu::File f1;
    f1.openR("test0.bin");
    cybozu::load(s1, f1);
    cybozu::load(d1, f1);
    CYBOZU_TEST_EQUAL(s0, s1);
    CYBOZU_TEST_EQUAL(d0, d1);

    ::unlink("test0.bin");
}

void testApply(const MetaSnap &s0, const MetaDiff &d1, const MetaSnap &s1)
{
    std::cout << "testApply start:" << s0 << " `apply` " << d1 << " == " << s1 << std::endl;
    CYBOZU_TEST_ASSERT(canApply(s0, d1));

    MetaState st(s0, 0);
    MetaState sa, sb, sc;
    CYBOZU_TEST_ASSERT(canApply(st, d1));
    sa = applying(st, d1);
    sb = apply(st, d1);
    CYBOZU_TEST_ASSERT(canApply(sa, d1));
    sc = apply(sa, d1);
    CYBOZU_TEST_ASSERT(sa.isApplying);
    CYBOZU_TEST_ASSERT(!sb.isApplying);
    CYBOZU_TEST_ASSERT(!sc.isApplying);
    CYBOZU_TEST_EQUAL(sa.timestamp, d1.timestamp);
    CYBOZU_TEST_EQUAL(sb.timestamp, d1.timestamp);
    CYBOZU_TEST_EQUAL(sc.timestamp, d1.timestamp);
    CYBOZU_TEST_EQUAL(sb, sc);

    CYBOZU_TEST_EQUAL(MetaState(s1, 0), sb);
    std::cout << "testApply end:  " << s0 << " `apply` " << d1 << " == " << s1 << std::endl;
}

CYBOZU_TEST_AUTO(apply)
{
    {
        MetaSnap s0(0, 0);
        MetaDiff d1({0, 0}, {10, 10});
        MetaSnap s1(10, 10);
        testApply(s0, d1, s1);
    }
    {
        MetaSnap s0(0, 0);
        MetaDiff d1({0, 1}, {2, 3});
        CYBOZU_TEST_ASSERT(!canApply(s0, d1));
    }
    {
        MetaSnap s0(0, 10);
        MetaDiff d1({0, 0}, {12, 12});
        MetaSnap s1(12, 12);
        testApply(s0, d1, s1);
    }
    {
        MetaSnap s0(5, 10);
        MetaDiff d1({5, 5}, {9, 9});
        MetaSnap s1(9, 10);
        testApply(s0, d1, s1);
    }
    {
        MetaSnap s0(0, 5);
        MetaDiff d1({0, 5}, {15, 16});
        MetaSnap s1(15, 16);
        testApply(s0, d1, s1);
    }
    {
        MetaSnap s0(0, 5);
        MetaDiff d1({0, 5}, {3, 6});
        MetaSnap s1(3, 6);
        testApply(s0, d1, s1);
    }
    {
        MetaSnap s0(0, 10);
        MetaDiff d1({0, 5}, {12, 15});
        CYBOZU_TEST_ASSERT(!canApply(s0, d1));
    }
}

CYBOZU_TEST_AUTO(oldOrNew)
{
    {
        MetaSnap s0(5);
        MetaDiff d1(4, 5);
        CYBOZU_TEST_ASSERT(!canApply(s0, d1));
        CYBOZU_TEST_ASSERT(isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(!isTooNew(s0, d1));
    }
    {
        MetaSnap s0(5);
        MetaDiff d1(6, 7);
        CYBOZU_TEST_ASSERT(!canApply(s0, d1));
        CYBOZU_TEST_ASSERT(!isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(isTooNew(s0, d1));
    }
    {
        MetaSnap s0(5);
        MetaDiff d1(3, 7);
        CYBOZU_TEST_ASSERT(canApply(s0, d1));
        CYBOZU_TEST_ASSERT(!isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(!isTooNew(s0, d1));
    }
}

/**
 * Generate a diff randomly that can be applied to a specified snap.
 */
MetaDiff randDiff(const MetaSnap &snap, bool allMergeable = false)
{
    snap.verify();
    cybozu::util::Random<uint32_t> rand;
    uint64_t b0, e0, b1, e1;
    bool isDirty = rand() % 3 == 0;
    if (isDirty) {
        /* dirty diff */
        b0 = snap.gidB;
        e0 = snap.gidE;
    } else {
        /* clean diff */
#if 0
        b0 = std::min(snap.gidB - (rand() % 10), snap.gidB);
#else
        b0 = snap.gidB;
#endif
        e0 = b0;
    }
    b1 = snap.gidB + 1 + rand() % 10; // progress constraint.
    if (isDirty) {
        if (rand() % 3 == 0) {
            e1 = b1;
        } else {
            e1 = b1 + rand() % 20;
        }
    } else {
        e1 = b1;
    }
    MetaDiff ret;
    ret.isMergeable = true;
    ret.snapB.set(b0, e0);
    ret.snapE.set(b1, e1);
    ret.verify();
    assert(canApply(snap, ret));
    if (!allMergeable) ret.isMergeable = (rand() % 2 == 0);
    return ret;
}

std::vector<MetaDiff> randDiffList(const MetaSnap &snap, size_t n, bool allMergeable = false)
{
    std::vector<MetaDiff> v;
    MetaSnap s = snap;
    for (size_t i = 0; i < n; i++) {
        MetaDiff d = randDiff(s, allMergeable);
        v.push_back(d);
        s = apply(s, d);
    }
    return v;
}

void testContains()
{
    std::vector<MetaDiff> v;
    MetaSnap snap(0);
    for (size_t i = 0; i < 10; i++) {
        MetaDiff d = randDiff(snap);
        snap = apply(snap, d);
        d.isMergeable = true;
        v.push_back(d);
    }
#if 0
    for (MetaDiff &d : v) {
        std::cout << d << std::endl;
    }
#endif
    MetaDiff mdiff = merge(v);
    for (MetaDiff &d : v) {
        CYBOZU_TEST_ASSERT(contains(mdiff, d));
    }
}

CYBOZU_TEST_AUTO(contains)
{
    for (size_t i = 0; i < 10; i++) {
        testContains();
    }
}

void testApplyMerged(const MetaSnap &snap, const MetaDiffVec &diffV)
{
#if 0
    std::cout << diffV.size() << " " << diffV << std::endl;
#endif
    const MetaSnap snapE = apply(snap, diffV);
    for (size_t s0 = 0; s0 < diffV.size(); s0++) {
        MetaDiffVec v0 = diffV;
        v0.resize(s0 + 1);
        if (!canMerge(v0)) break;
        const MetaDiff mDiff = merge(v0);
        CYBOZU_TEST_ASSERT(canApply(snap, mDiff));
        const MetaSnap snap1a = apply(snap, mDiff);
        const MetaSnap snap1b = apply(snap, v0);
        CYBOZU_TEST_EQUAL(snap1a, snap1b);

        const MetaDiffVec v1(diffV.begin() + s0 + 1, diffV.end());
        // v0 + v1 = diffV

        CYBOZU_TEST_ASSERT(canApply(snap1a, v1));
        CYBOZU_TEST_EQUAL(apply(snap1a, v1), snapE);
    }
}

CYBOZU_TEST_AUTO(applyMerged)
{
    {
        const MetaSnap snap(0, 1);
        const MetaDiffVec diffV = {
            MetaDiff({0}, {1}, true, 0),
            MetaDiff({1, 1}, {2, 3}, true, 0),
            MetaDiff({2}, {3}, true, 0),
            MetaDiff({3}, {4}, true, 0),
        };
        testApplyMerged(snap, diffV);
    }
    const size_t n = 100;
    for (size_t i = 0; i < n; i++) {
        const MetaSnap snap(0);
        testApplyMerged(snap, randDiffList(snap, 10, true));
    }
    for (size_t i = 0; i < n; i++) {
        const MetaSnap snap(0, 5);
        testApplyMerged(snap, randDiffList(snap, 10, true));
    }
    for (size_t i = 0; i < n; i++) {
        const MetaSnap snap(0, 100);
        testApplyMerged(snap, randDiffList(snap, 10, true));
    }
}

/**
 *     |0|
 * (1) |0|-->|1|
 * (2)       |1|-->|2|
 * (3)             |2|-->|3|
 * (4)                   |3|-->|4|
 * (5)                         |4|-->|5|
 */
CYBOZU_TEST_AUTO(metaDiffManager1)
{
    MetaSnap snap(0);
    MetaState st(snap, 0);
    std::vector<MetaDiff> v;

    v.emplace_back(0, 1, true, 1000);
    v.emplace_back(1, 2, true, 1001);
    v.emplace_back(2, 3, true, 1002);
    v.emplace_back(3, 4, false, 1003);
    v.emplace_back(4, 5, true, 1004);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), MetaSnap(5));
    CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), 0);
    CYBOZU_TEST_EQUAL(mgr.getApplicableDiffListByGid(snap, 4).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getApplicableDiffListByTime(snap, 1003).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 2).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 4).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 2).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 4).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(2)).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(4)).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, true).size(), 6);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, false).size(), 3);

    auto v1 = mgr.getMergeableDiffList(0);
    CYBOZU_TEST_EQUAL(v1.size(), 3);
    auto v2 = mgr.getMergeableDiffList(3);
    CYBOZU_TEST_EQUAL(v2.size(), 2);
    auto mdiff1 = merge(v1);
    auto mdiff2 = merge(v2);
    mgr.add(mdiff1);
    mgr.add(mdiff2);
    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), MetaSnap(5));
    mgr.gc();
    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), MetaSnap(5));
    auto v3 = mgr.getAll();
    CYBOZU_TEST_EQUAL(v3.size(), 2);
    CYBOZU_TEST_EQUAL(v3[0], mdiff1);
    CYBOZU_TEST_EQUAL(v3[1], mdiff2);

}

/**
 *     |0,       10|
 * (1) |0|-->|5|
 * (2)       |5, 10|
 *           -->|10|
 * (3)          |10|-->|15|
 */
CYBOZU_TEST_AUTO(metaDiffManager2)
{
    MetaSnap snap(0, 10);
    MetaState st(snap, 0);
    std::vector<MetaDiff> v;
    v.emplace_back(0, 5, false, 1000);
    v.push_back(MetaDiff({5, 10}, {10}, false, 1001));
    v.emplace_back(10, 15, false, 1002);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), 10);
    auto v0 = mgr.getApplicableDiffList(st.snapB);
    CYBOZU_TEST_EQUAL(v0.size(), 3);
    auto v1 = mgr.getMinimumApplicableDiffList(st);
    CYBOZU_TEST_EQUAL(v1.size(), 0);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 5).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 10).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 5).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 10).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(5)).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(5, 10)).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(10)).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, true).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, false).size(), 2);

    auto st1a = applying(st, v);
    auto st1b = applying(st1a, v);
    MetaState st1c(MetaSnap(0, 10), MetaSnap(15), 0);
    auto st2a = apply(st, v);
    auto st2b = apply(st1a, v);
    MetaState st2c(MetaSnap(15), 0);
    CYBOZU_TEST_EQUAL(st1a, st1b);
    CYBOZU_TEST_EQUAL(st1a, st1c);
    CYBOZU_TEST_EQUAL(st2a, st2b);
    CYBOZU_TEST_EQUAL(st2a, st2c);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 5).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 10).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 15).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 5).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 10).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 15).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st1c, MetaSnap(5)).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st1c, MetaSnap(10)).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st1c, MetaSnap(15)).size(), 3);
}

/**
 *     |0,       10|
 * (1) |0|-->|5|
 * (2)       |5, 10|-->|15,    20|
 * (3)                 |15|-->|20|
 */
CYBOZU_TEST_AUTO(metaDiffManager3)
{
    MetaSnap snap(0, 10);
    MetaState st(snap, 0);
    std::vector<MetaDiff> v;
    v.emplace_back(0, 5, false, 1000);
    v.push_back(MetaDiff({5, 10}, {15, 20}, false, 1001));
    v.emplace_back(15, 20, false, 1002);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), 20);
    auto v0 = mgr.getApplicableDiffList(st.snapB);
    CYBOZU_TEST_EQUAL(v0.size(), 3);
    auto v1 = mgr.getMinimumApplicableDiffList(st);
    CYBOZU_TEST_EQUAL(v1.size(), 0);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 5).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 10).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 15).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 20).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 5).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 10).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 15).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 20).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(5, 10)).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(15, 20)).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(20)).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, true).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, false).size(), 1);
}

/**
 * Use randomly generated diff list.
 */
CYBOZU_TEST_AUTO(metaDiffManager4)
{
    MetaSnap snap(0), s0(snap), s1(snap);
    MetaState st(snap, 0);
    auto v = randDiffList(snap, 20);
#if 0
    for (MetaDiff &d : v) {
        std::cout << d << std::endl;
    }
#endif

    MetaDiffManager mgr;
    for (MetaDiff &d : v) {
        s1 = mgr.getLatestSnapshot(st);
        CYBOZU_TEST_EQUAL(s0, s1);
        mgr.add(d);
        s0 = apply(s0, d);
    }

    auto v1 = mgr.getApplicableDiffList(snap);
    CYBOZU_TEST_ASSERT(v == v1);

    cybozu::util::Random<uint32_t> rand;
#if 0
    for (const MetaDiff &d : mgr.getAll()) {
        std::cout << "diff " << d << std::endl;
    }
#endif
    s0 = mgr.getLatestSnapshot(st);
    uint64_t maxGid = s0.gidE;

    const size_t restorable0 = mgr.getRestorableList(st, false).size();
    const size_t restorable0a = mgr.getRestorableList(st, true).size();

    for (size_t i = 0; i < 10; i++) {
        auto v2 = mgr.getMergeableDiffList(rand() % maxGid);
        if (v2.size() > 1) {
            mgr.add(merge(v2));
            mgr.gc();
        }
        s1 = mgr.getLatestSnapshot(st);
        CYBOZU_TEST_EQUAL(s0, s1);
    }

    const size_t restorable1 = mgr.getRestorableList(st, false).size();
    const size_t restorable1a = mgr.getRestorableList(st, true).size();
    CYBOZU_TEST_EQUAL(restorable0, restorable1);
    CYBOZU_TEST_ASSERT(restorable0a >= restorable1a);

    std::vector<uint64_t> gidV = mgr.getCleanSnapshotList(st);
    if (!gidV.empty()) {
        CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), gidV[0]);
    }
}
