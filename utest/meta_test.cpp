#include "cybozu/test.hpp"
#include "cybozu/file.hpp"
#include "meta.hpp"
#include "random.hpp"

CYBOZU_TEST_AUTO(diff)
{
    walb::MetaDiff d0({0, 0}, {1, 2}), d1(1, 2);
    d1.canMerge = true;
    CYBOZU_TEST_ASSERT(walb::canMerge(d0, d1));
    walb::MetaDiff d2 = walb::merge(d0, d1);
    CYBOZU_TEST_ASSERT(d2 == walb::MetaDiff(0, 2));
}

CYBOZU_TEST_AUTO(snap)
{
    {
        walb::MetaSnap s0(0, 7);
        walb::MetaDiff d1({0, 0}, {10, 10});
        CYBOZU_TEST_ASSERT(walb::apply(s0, d1) == walb::MetaSnap(10, 10));
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1({0, 0}, {10, 10});
        CYBOZU_TEST_ASSERT(walb::apply(s0, d1) == walb::MetaSnap(10, 10));
    }
    {
        walb::MetaSnap s0(0, 13);
        walb::MetaDiff d1({0, 13}, {10, 12});
        CYBOZU_TEST_ASSERT(walb::apply(s0, d1) == walb::MetaSnap(10, 12));
    }
}

CYBOZU_TEST_AUTO(serialize)
{
    walb::MetaSnap s0(0), s1;
    walb::MetaDiff d0(0, 1), d1;

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

void testApply(const walb::MetaSnap &s0, const walb::MetaDiff &d1, const walb::MetaSnap &s1)
{
    std::cout << "testApply start:" << s0 << " `apply` " << d1 << " == " << s1 << std::endl;
    CYBOZU_TEST_ASSERT(walb::canApply(s0, d1));

    walb::MetaState st(s0);
    walb::MetaState sa, sb, sc;
    CYBOZU_TEST_ASSERT(walb::canApply(st, d1));
    sa = walb::applying(st, d1);
    sb = walb::apply(st, d1);
    CYBOZU_TEST_ASSERT(walb::canApply(sa, d1));
    sc = walb::apply(sa, d1);
    CYBOZU_TEST_ASSERT(sa.isApplying);
    CYBOZU_TEST_ASSERT(!sb.isApplying);
    CYBOZU_TEST_ASSERT(!sc.isApplying);
    CYBOZU_TEST_EQUAL(sb, sc);

    CYBOZU_TEST_EQUAL(walb::MetaState(s1), sb);
    std::cout << "testApply end:  " << s0 << " `apply` " << d1 << " == " << s1 << std::endl;
}

CYBOZU_TEST_AUTO(apply)
{
    {
        walb::MetaSnap s0(0, 0);
        walb::MetaDiff d1({0, 0}, {10, 10});
        walb::MetaSnap s1(10, 10);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 0);
        walb::MetaDiff d1({0, 1}, {2, 3});
        CYBOZU_TEST_ASSERT(!walb::canApply(s0, d1));
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1({0, 0}, {12, 12});
        walb::MetaSnap s1(12, 12);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(5, 10);
        walb::MetaDiff d1({5, 5}, {9, 9});
        walb::MetaSnap s1(9, 10);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 5);
        walb::MetaDiff d1({0, 5}, {15, 16});
        walb::MetaSnap s1(15, 16);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 5);
        walb::MetaDiff d1({0, 5}, {3, 6});
        walb::MetaSnap s1(3, 6);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1({0, 5}, {12, 15});
        CYBOZU_TEST_ASSERT(!walb::canApply(s0, d1));
    }
}

CYBOZU_TEST_AUTO(oldOrNew)
{
    {
        walb::MetaSnap s0(5);
        walb::MetaDiff d1(4, 5);
        CYBOZU_TEST_ASSERT(!walb::canApply(s0, d1));
        CYBOZU_TEST_ASSERT(walb::isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(!walb::isTooNew(s0, d1));
    }
    {
        walb::MetaSnap s0(5);
        walb::MetaDiff d1(6, 7);
        CYBOZU_TEST_ASSERT(!walb::canApply(s0, d1));
        CYBOZU_TEST_ASSERT(!walb::isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(walb::isTooNew(s0, d1));
    }
    {
        walb::MetaSnap s0(5);
        walb::MetaDiff d1(3, 7);
        CYBOZU_TEST_ASSERT(walb::canApply(s0, d1));
        CYBOZU_TEST_ASSERT(!walb::isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(!walb::isTooNew(s0, d1));
    }
}

/**
 * Generate a diff randomly that can be applied to a specified snap.
 */
walb::MetaDiff randDiff(const walb::MetaSnap &snap)
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
    walb::MetaDiff ret;
    ret.canMerge = true;
    ret.snapB.set(b0, e0);
    ret.snapE.set(b1, e1);
    ret.verify();
    assert(canApply(snap, ret));
    ret.canMerge = (rand() % 2 == 0);
    return ret;
}

std::vector<walb::MetaDiff> randDiffList(const walb::MetaSnap &snap, size_t n)
{
    std::vector<walb::MetaDiff> v;
    walb::MetaSnap s = snap;
    for (size_t i = 0; i < n; i++) {
        walb::MetaDiff d = randDiff(s);
        v.push_back(d);
        s = walb::apply(s, d);
    }
    return v;
}

void testContains()
{
    std::vector<walb::MetaDiff> v;
    walb::MetaSnap snap(0);
    for (size_t i = 0; i < 10; i++) {
        walb::MetaDiff d = randDiff(snap);
        snap = apply(snap, d);
        d.canMerge = true;
        v.push_back(d);
    }
#if 0
    for (walb::MetaDiff &d : v) {
        std::cout << d << std::endl;
    }
#endif
    walb::MetaDiff mdiff = walb::merge(v);
    for (walb::MetaDiff &d : v) {
        CYBOZU_TEST_ASSERT(walb::contains(mdiff, d));
    }
}

CYBOZU_TEST_AUTO(contains)
{
    for (size_t i = 0; i < 10; i++) {
        testContains();
    }
}

CYBOZU_TEST_AUTO(metaDiffManager1)
{
    walb::MetaSnap snap(0);
    walb::MetaState st(snap);
    std::vector<walb::MetaDiff> v;

    v.emplace_back(0, 1, true, 1000);
    v.emplace_back(1, 2, true, 1001);
    v.emplace_back(2, 3, true, 1002);
    v.emplace_back(3, 4, false, 1003);
    v.emplace_back(4, 5, true, 1004);

    walb::MetaDiffManager mgr;
    for (walb::MetaDiff &d : v) mgr.add(d);
    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), walb::MetaSnap(5));
    CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), 0);
    CYBOZU_TEST_EQUAL(mgr.getApplicableDiffListByGid(snap, 4).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getApplicableDiffListByTime(snap, 1003).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 1003).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 4).size(), 4);

    auto v1 = mgr.getMergeableDiffList(0);
    CYBOZU_TEST_EQUAL(v1.size(), 3);
    auto v2 = mgr.getMergeableDiffList(3);
    CYBOZU_TEST_EQUAL(v2.size(), 2);
    auto mdiff1 = walb::merge(v1);
    auto mdiff2 = walb::merge(v2);
    mgr.add(mdiff1);
    mgr.add(mdiff2);
    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), walb::MetaSnap(5));
    mgr.gc();
    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), walb::MetaSnap(5));
    auto v3 = mgr.getAll();
    CYBOZU_TEST_EQUAL(v3.size(), 2);
    CYBOZU_TEST_EQUAL(v3[0], mdiff1);
    CYBOZU_TEST_EQUAL(v3[1], mdiff2);

}

CYBOZU_TEST_AUTO(metaDiffManager2)
{
    walb::MetaSnap snap(0, 10);
    walb::MetaState st(snap);
    std::vector<walb::MetaDiff> v;
    v.emplace_back(0, 5, false, 1000);
    v.push_back(walb::MetaDiff({5, 10}, {10}, false, 1001));
    v.emplace_back(10, 15, false, 1002);

    walb::MetaDiffManager mgr;
    for (walb::MetaDiff &d : v) mgr.add(d);
    CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), 10);
    auto v0 = mgr.getApplicableDiffList(st.snapB);
    CYBOZU_TEST_EQUAL(v0.size(), 3);
    auto v1 = mgr.getMinimumApplicableDiffList(st);
    CYBOZU_TEST_EQUAL(v1.size(), 1);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 1001).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 10).size(), 2);

    auto st1a = walb::applying(st, v);
    auto st1b = walb::applying(st1a, v);
    walb::MetaState st1c(walb::MetaSnap(0, 10), walb::MetaSnap(15));
    auto st2a = walb::apply(st, v);
    auto st2b = walb::apply(st1a, v);
    walb::MetaState st2c(walb::MetaSnap(15));
    CYBOZU_TEST_EQUAL(st1a, st1b);
    CYBOZU_TEST_EQUAL(st1a, st1c);
    CYBOZU_TEST_EQUAL(st2a, st2b);
    CYBOZU_TEST_EQUAL(st2a, st2c);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 1001).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 1001).size(), 0);
}

CYBOZU_TEST_AUTO(metaDiffManager3)
{
    walb::MetaSnap snap(0), s0(snap), s1(snap);
    walb::MetaState st(snap);
    auto v = randDiffList(snap, 20);
#if 0
    for (walb::MetaDiff &d : v) {
        std::cout << d << std::endl;
    }
#endif

    walb::MetaDiffManager mgr;
    for (walb::MetaDiff &d : v) {
        s1 = mgr.getLatestSnapshot(st);
        CYBOZU_TEST_EQUAL(s0, s1);
        mgr.add(d);
        s0 = walb::apply(s0, d);
    }

    auto v1 = mgr.getApplicableDiffList(snap);
    CYBOZU_TEST_ASSERT(v == v1);

    cybozu::util::Random<uint32_t> rand;
#if 0
    for (const walb::MetaDiff &d : mgr.getAll()) {
        std::cout << "diff " << d << std::endl;
    }
#endif
    s0 = mgr.getLatestSnapshot(st);
    uint64_t maxGid = s0.gidE;

    for (size_t i = 0; i < 10; i++) {
        auto v2 = mgr.getMergeableDiffList(rand() % maxGid);
        if (v2.size() > 1) {
            mgr.add(walb::merge(v2));
            mgr.gc();
        }
        s1 = mgr.getLatestSnapshot(st);
        CYBOZU_TEST_EQUAL(s0, s1);
    }

    std::vector<uint64_t> gidV = mgr.getCleanSnapshotList(st);
    if (!gidV.empty()) {
        CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), gidV[0]);
    }
}
