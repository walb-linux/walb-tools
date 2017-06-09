#include "cybozu/test.hpp"
#include "cybozu/file.hpp"
#include "meta.hpp"
#include "random.hpp"

using namespace walb;

cybozu::util::Random<size_t> randx;

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
        MetaSnap s0(0, 10);
        MetaDiff d1({0, 10}, {11, 12});
        CYBOZU_TEST_ASSERT(apply(s0, d1) == MetaSnap(11, 12));
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

void testMerge(const MetaDiff &d0, const MetaDiff &d1, const MetaDiff &md)
{
    std::cout << "testMerge start:" << d0 << " `merge` " << d1 << " == " << md << std::endl;
    CYBOZU_TEST_ASSERT(canMerge(d0, d1));

    MetaDiff mdx = merge(d0, d1);
    CYBOZU_TEST_EQUAL(mdx, md);
}

CYBOZU_TEST_AUTO(merge)
{
    // Clean diff +++ clean diff
    {
        /*
         * |--->|
         *      |--->|
         */
        MetaDiff d0({0}, {1}, true);
        MetaDiff d1({1}, {2}, true);
        MetaDiff md({0}, {2}, true);
        testMerge(d0, d1, md);
    }
    {
        /*
         * |--->|
         *    |--->|
         */
        MetaDiff d0({0}, {2}, true);
        MetaDiff d1({1}, {3}, true);
        MetaDiff md({0}, {3}, true);
        testMerge(d0, d1, md);
    }
    {
        /*
         * |--->|
         *         |--->|
         */
        MetaDiff d0({0}, {2}, true);
        MetaDiff d1({3}, {5}, true);
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
    }
    {
        /*
         * |--------->|
         *    |--->|
         */
        MetaDiff d0({0}, {5}, true);
        MetaDiff d1({1}, {3}, true);
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
    }
    {
        /*
         *    |--->|
         * |--------->|
         */
        MetaDiff d0({1}, {3}, true);
        MetaDiff d1({0}, {5}, true);
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
    }


    // clean diff +++ dirty diff
    {
        /*
         * |---->|
         *       |--->|  |
         */
        MetaDiff d0({0}, {1}, true);
        MetaDiff d1({1}, {2, 3}, true);
        MetaDiff md({0}, {2, 3}, true);
        testMerge(d0, d1, md);
    }
    {
        /*
         * |------->|
         *     |  |--->|  |
         */
        MetaDiff d0({0}, {5}, true);
        MetaDiff d1({1, 2}, {7, 8}, true);
        MetaDiff md({0}, {7, 8}, true);
        testMerge(d0, d1, md);
    }
    {
        /*
         * |  |---->|     |
         *       |---->|
         */
        MetaDiff d0({0, 1}, {5, 10}, true);
        MetaDiff d1({2}, {7}, true);
        MetaDiff md({0, 1}, {7, 10}, true);
        testMerge(d0, d1, md);
    }
    {
        /*
         * |  |---->|     |
         *       |---     ->|
         */
        MetaDiff d0({0, 1}, {5, 10}, true);
        MetaDiff d1({2}, {12}, true);
        MetaDiff md({0, 1}, {12}, true);
        testMerge(d0, d1, md);
    }

    // isMergeable
    {
        MetaDiff d0({0}, {1}, true);
        MetaDiff d1({1}, {2}, false);
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
        d1.isMergeable = false;
        d1.isMergeable = true;
        CYBOZU_TEST_ASSERT(canMerge(d0, d1));
    }

    // isCompDiff
    {
        MetaDiff d0({0}, {1}, true);
        MetaDiff d1({1}, {2}, true);
        d0.isCompDiff = true;
        d1.isCompDiff = false;
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
        d0.isCompDiff = false;
        d1.isCompDiff = true;
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
        d0.isCompDiff = true;
        d1.isCompDiff = true;
        CYBOZU_TEST_ASSERT(!canMerge(d0, d1));
    }
}

void testApply(const MetaSnap &s0, const MetaDiff &d0, const MetaSnap &s1)
{
    std::cout << "testApply start:" << s0 << " `apply` " << d0 << " == " << s1 << std::endl;
    CYBOZU_TEST_ASSERT(canApply(s0, d0));

    MetaState st(s0, 0);
    MetaState sa, sb, sc;
    CYBOZU_TEST_ASSERT(canApply(st, d0));
    sa = beginApplying(st, d0);
    sb = apply(st, d0);
    CYBOZU_TEST_ASSERT(canApply(sa, d0));
    sc = endApplying(sa, d0);
    CYBOZU_TEST_ASSERT(sa.isApplying);
    CYBOZU_TEST_ASSERT(!sb.isApplying);
    CYBOZU_TEST_ASSERT(!sc.isApplying);
    CYBOZU_TEST_EQUAL(sa.timestamp, d0.timestamp);
    CYBOZU_TEST_EQUAL(sb.timestamp, d0.timestamp);
    CYBOZU_TEST_EQUAL(sc.timestamp, d0.timestamp);
    CYBOZU_TEST_EQUAL(sb, sc);

    CYBOZU_TEST_EQUAL(MetaState(s1, 0), sb);
    std::cout << "testApply end:  " << s0 << " `apply` " << d0 << " == " << s1 << std::endl;
}

CYBOZU_TEST_AUTO(apply)
{
    // Clean snapshot and diffs.
    {
        MetaSnap s0(0);
        MetaDiff d0({0}, {1});
        MetaSnap s1(1);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(1);
        MetaDiff d0({0}, {2});
        MetaSnap s1(2);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(0);
        MetaDiff d0({1}, {2});
        CYBOZU_TEST_ASSERT(getRelation(s0, d0) == Relation::TOO_NEW_DIFF);
        CYBOZU_TEST_ASSERT(!canApply(s0, d0));
    }
    {
        MetaSnap s0(2);
        MetaDiff d0({0}, {1});
        CYBOZU_TEST_ASSERT(getRelation(s0, d0) == Relation::TOO_OLD_DIFF);
        CYBOZU_TEST_ASSERT(!canApply(s0, d0));
    }
    // Dirty snaphsots and clean diffs.
    {
        MetaSnap s0(0, 2);
        MetaDiff d0({0}, {2});
        MetaSnap s1(2);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(0, 2);
        MetaDiff d0({0}, {4});
        MetaSnap s1(4);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(0, 2);
        MetaDiff d0({0}, {1});
        MetaSnap s1(1, 2);
        testApply(s0, d0, s1);
    }
    // Dirty snapshots and dirty diffs.
    {
        MetaSnap s0(0, 5);
        MetaDiff d0({0, 5}, {15, 16});
        MetaSnap s1(15, 16);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(0, 5);
        MetaDiff d0({0, 5}, {10});
        MetaSnap s1(10);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(5, 10);
        MetaDiff d0({0, 5}, {15});
        MetaSnap s1(15);
        testApply(s0, d0, s1);
    }
    {
        MetaSnap s0(5, 10);
        MetaDiff d0({0, 5}, {7, 10});
        MetaSnap s1(7, 10);
        testApply(s0, d0, s1);
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
    uint64_t b0, e0, b1, e1;
    bool isDirty = randx() % 3 == 0;
    if (isDirty) {
        /* dirty diff */
        b0 = snap.gidB;
        e0 = snap.gidE;
    } else {
        /* clean diff */
        b0 = snap.gidB;
        e0 = b0;
    }
    b1 = snap.gidE + 1 + randx() % 10; // progress constraint.
    if (isDirty) {
        e1 = b1 + 1 + randx() % 20;
    } else {
        e1 = b1;
    }
    MetaDiff ret;
    ret.isMergeable = true;
    ret.snapB.set(b0, e0);
    ret.snapE.set(b1, e1);
    ret.verify();
    assert(canApply(snap, ret));
    if (!allMergeable) ret.isMergeable = (randx() % 2 == 0);
    return ret;
}

MetaDiffVec randDiffList(const MetaSnap &snap, size_t n, bool allMergeable = false)
{
    MetaDiffVec v;
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
    MetaDiffVec v;
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

bool mergeAndReplace(MetaDiffVec& diffV)
{
    if (diffV.size() < 2) return false;

    const size_t size = diffV.size();
    const size_t bgn = size == 2 ? 0 : (randx() % (size - 2));
    const size_t end = size == 2 ? 2 : (bgn + 2 + randx() % (size - bgn - 2));

    auto ibgn = diffV.begin() + bgn;
    auto iend = diffV.begin() + end;
    MetaDiffVec mergeV(ibgn, iend);
    CYBOZU_TEST_ASSERT(mergeV.size() >= 2);
    CYBOZU_TEST_ASSERT(canMerge(mergeV));
    MetaDiff md = merge(mergeV);

    iend = diffV.erase(ibgn, iend - 1);
    *iend = md;
    return true;
}

bool existsApplicableDiffs(const MetaState& st, const MetaDiffVec& diffV)
{
    const size_t nr = diffV.size();

    for (size_t i = 0; i < nr; i++) {
        if (canApply(st.snapB, diffV[i])) {
            if (!st.isApplying) return true;
            for (size_t j = i + 1; j <= nr; j++) {
                MetaDiffVec v(diffV.begin() + i, diffV.begin() + j);
                if (canApply(st, v)) return true;
            }
            return false;
        }
    }
    return false;
}

void printDiffV(const MetaDiffVec& v)
{
    for (const MetaDiff& d : v) {
        std::cout << d << " ";
    }
    std::cout << std::endl;
}

void testMergedApply(const MetaSnap& s0, const MetaDiffVec& dV)
{
    CYBOZU_TEST_ASSERT(canMerge(dV));
    const MetaDiff md = merge(dV);
    CYBOZU_TEST_ASSERT(canApply(s0, md));
    const MetaSnap sx0 = apply(s0, md);

    MetaSnap sx1 = s0;
    for (const MetaDiff& d : dV) {
        CYBOZU_TEST_ASSERT(canApply(sx1, d));
        sx1 = apply(sx1, d);
    }
    CYBOZU_TEST_EQUAL(sx0, sx1);
}

CYBOZU_TEST_AUTO(diffList)
{
    const size_t nr = 10;
    const size_t loop = 1000;

    MetaSnap s0(0, 5);
    const MetaDiffVec diffV = randDiffList(s0, nr, true);

    /*
     * d_i ++? d_{j} = j == i + 1
     */
    for (size_t i = 0; i < nr - 1; i++) {
        for (size_t j = 0; j < nr; j++) {
            if (j == i + 1) {
                CYBOZU_TEST_ASSERT(canMerge(diffV[i], diffV[j]));
            } else {
                CYBOZU_TEST_ASSERT(!canMerge(diffV[i], diffV[j]));
            }
        }
    }
    /*
     * s0 <<< d0 <<< d1 <<< ... = s0 <<< d0 +++ d1 +++ ...
     */
    for (size_t i = 0; i < nr; i++) {
        MetaDiffVec v0(diffV.begin(), diffV.begin() + i);
        CYBOZU_TEST_ASSERT(canApply(s0, v0));
        const MetaSnap si = apply(s0, v0);
        for (size_t j = i; j < nr; j++) {
            MetaDiffVec v1(diffV.begin() + i, diffV.begin() + j + 1);
            testMergedApply(si, v1);
        }
    }

    /*
     * For all metastate st, there exists diffs such that canApply(st, diffs) is true.
     */
    for (size_t i = 0; i < loop; i++) {
        const size_t x1 = randx() % (nr - 2);
        const size_t x2 = x1 + 1 + randx() % (nr - x1 - 1);
        MetaDiffVec diffV1(diffV.begin(), diffV.begin() + x1);
        MetaDiffVec diffV2(diffV.begin() + x1, diffV.begin() + x2);

        MetaSnap sx = apply(s0, diffV1);
        MetaState st = beginApplying(MetaState(sx, 0), diffV2);
        //std::cout << st << std::endl;

        MetaDiffVec diffV3 = diffV; // copy
        //printDiffV(diffV);
        while (mergeAndReplace(diffV3)) {
            //printDiffV(diffV);
            CYBOZU_TEST_ASSERT(existsApplicableDiffs(st, diffV3));
        }
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
    MetaDiffVec v;

    v.emplace_back(0, 1, true, 1000);
    v.emplace_back(1, 2, true, 1001);
    v.emplace_back(2, 3, true, 1002);
    v.emplace_back(3, 4, false, 1003);
    v.emplace_back(4, 5, true, 1004);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    mgr.validateForTest(__LINE__);
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
    mgr.validateForTest(__LINE__);

    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), MetaSnap(5));
    mgr.gc(MetaSnap(0));
    mgr.validateForTest(__LINE__);

    CYBOZU_TEST_EQUAL(mgr.getLatestSnapshot(st), MetaSnap(5));
    auto v3 = mgr.getAll();
    CYBOZU_TEST_EQUAL(v3.size(), 2);
    CYBOZU_TEST_EQUAL(v3[0], mdiff1);
    CYBOZU_TEST_EQUAL(v3[1], mdiff2);
}

/**
 * s0  |0,       10|
 * d0  |0|-->|5|
 * d1        |5, 10|->|15,    20|
 * d2                 |15|-->|20|
 * d3                        |20|-->|25|
 *
 * s1        |5, 10|
 * s2                 |15,    20|
 * s3                        |20|
 * s4                               |25|
 */
CYBOZU_TEST_AUTO(metaDiffManager2)
{
    MetaSnap snap(0, 10);
    MetaState st(snap, 0);
    MetaDiffVec v;
    v.emplace_back(0, 5, false, 1000);
    v.push_back(MetaDiff({5, 10}, {15, 20}, false, 1001));
    v.emplace_back(15, 20, false, 1002);
    v.emplace_back(20, 25, false, 1003);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    CYBOZU_TEST_EQUAL(mgr.getOldestCleanSnapshot(st), 20);
    auto v0 = mgr.getApplicableDiffList(st.snapB);
    CYBOZU_TEST_EQUAL(v0.size(), 4);
    auto v1 = mgr.getMinimumApplicableDiffList(st);
    CYBOZU_TEST_EQUAL(v1.size(), 0);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 5).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 15).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 20).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st, 25).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 5).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 15).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 20).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st, 25).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(5)).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(5, 10)).size(), 1);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(15, 20)).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(20)).size(), 3);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st, MetaSnap(25)).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, true).size(), 2);
    CYBOZU_TEST_EQUAL(mgr.getRestorableList(st, false).size(), 2);

    auto st1a = beginApplying(st, v);
    auto st1b = beginApplying(st1a, v);
    MetaState st1c(MetaSnap(0, 10), MetaSnap(25), 0);
    auto st2a = endApplying(st, v);
    auto st2b = endApplying(st1a, v);
    MetaState st2c(MetaSnap(25), 0);
    CYBOZU_TEST_EQUAL(st1a, st1b);
    CYBOZU_TEST_EQUAL(st1a, st1c);
    CYBOZU_TEST_EQUAL(st2a, st2b);
    CYBOZU_TEST_EQUAL(st2a, st2c);

    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 5).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 20).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToApply(st1c, 25).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 5).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 20).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToRestore(st1c, 25).size(), 4);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st1c, MetaSnap(5)).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st1c, MetaSnap(20)).size(), 0);
    CYBOZU_TEST_EQUAL(mgr.getDiffListToSync(st1c, MetaSnap(25)).size(), 4);
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
    MetaDiffVec v;
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

    mgr.validateForTest(__LINE__);
    auto v1 = mgr.getApplicableDiffList(snap);
    CYBOZU_TEST_ASSERT(v == v1);

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
        auto v2 = mgr.getMergeableDiffList(randx() % maxGid);
        if (v2.size() > 1) {
            mgr.add(merge(v2));
            mgr.gc(MetaSnap(0));
            mgr.validateForTest(__LINE__);
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

CYBOZU_TEST_AUTO(metaDiffManagerGcRange)
{
    MetaSnap snap(0);
    MetaState st(snap, 0);

    MetaDiffVec v;
    v.emplace_back(0, 1, true, 1000);
    v.emplace_back(0, 2, true, 1000);
    v.emplace_back(1, 2, true, 1000);
    v.emplace_back(1, 3, true, 1000);
    v.emplace_back(2, 3, true, 1002);
    v.emplace_back(2, 4, true, 1003);

    MetaDiffManager mgr;
    for (const MetaDiff& d : v) mgr.add(d);
    mgr.validateForTest(__LINE__);

    MetaDiffVec v2 = mgr.gcRange(1, 3);
    mgr.validateForTest(__LINE__);
    //printDiffV(v2);
    CYBOZU_TEST_EQUAL(v2.size(), 2);
    CYBOZU_TEST_EQUAL(v2[0], v[2]);
    CYBOZU_TEST_EQUAL(v2[1], v[4]);
}

CYBOZU_TEST_AUTO(metaDiffManagerMerge1)
{
    MetaSnap snap(0);
    MetaDiffVec v;

    v.emplace_back(0, 1, true, 1000);
    v.emplace_back(1, 2, true, 1001);
    v.emplace_back(2, 3, true, 1002);
    v.emplace_back(3, 4, true, 1003);
    v.emplace_back(4, 5, true, 1004);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    mgr.validateForTest(__LINE__);

    auto mdiff1 = merge({v[0], v[1], v[2]});
    auto mdiff2 = merge({v[1], v[2], v[3], v[4]});
    mgr.add(mdiff1);
    mgr.add(mdiff2);
    {
        mgr.validateForTest(__LINE__);
        auto v1 = mgr.getApplicableDiffList(snap);
        CYBOZU_TEST_EQUAL(v1.size(), 2);
        CYBOZU_TEST_EQUAL(v1[0], mdiff1);
        CYBOZU_TEST_EQUAL(v1[1], mdiff2);
    }

    mgr.gc(snap);
    {
        mgr.validateForTest(__LINE__);
        auto v1 = mgr.getApplicableDiffList(snap);
        CYBOZU_TEST_EQUAL(v1.size(), 2);
        CYBOZU_TEST_EQUAL(v1[0], mdiff1);
        CYBOZU_TEST_EQUAL(v1[1], mdiff2);
    }

    auto mdiff3 = merge({v[3], v[4]});
    mgr.add(mdiff3);
    mgr.gc(snap);
    {
        mgr.validateForTest(__LINE__);
        auto v1 = mgr.getApplicableDiffList(snap);
        CYBOZU_TEST_EQUAL(v1.size(), 2);
        CYBOZU_TEST_EQUAL(v1[0], mdiff1);
        CYBOZU_TEST_EQUAL(v1[1], mdiff3);
    }
}

CYBOZU_TEST_AUTO(metaDiffManagerMerge2)
{
    MetaSnap snap(0);
    MetaDiffVec v;

    v.emplace_back(0, 1, true, 1000);
    //v.emplace_back(1, 2, true, 1001);
    v.emplace_back(2, 3, true, 1002);
    //v.emplace_back(3, 4, true, 1003);
    v.emplace_back(4, 5, true, 1004);

    v.emplace_back(0, 3, true, 1002);
    v.emplace_back(2, 5, true, 1004);

    MetaDiffManager mgr;
    for (MetaDiff &d : v) mgr.add(d);
    mgr.validateForTest(__LINE__);

    mgr.gc(snap);
    {
        mgr.validateForTest(__LINE__);
        auto v1 = mgr.getApplicableDiffList(snap);
        CYBOZU_TEST_EQUAL(v1.size(), 2);
        CYBOZU_TEST_EQUAL(v1[0], v[3]);
        CYBOZU_TEST_EQUAL(v1[1], v[4]);
    }
}

CYBOZU_TEST_AUTO(metaDiffManagerMerge3)
{
    MetaSnap snap0(0);
    uint64_t ts = ::time(0);
    MetaDiffManager mgr;
    for (uint64_t gid = 0; gid < 1000; gid++) {
        MetaDiff d(gid, gid + 1, true, ts++);
        d.dataSize = 1 + randx() % 10000;
        mgr.add(d);
        mgr.validateForTest(__LINE__);
        MetaDiffVec v = mgr.getApplicableDiffList(snap0);
        if (mgr.size() - v.size() > 10) {
            mgr.gc(snap0);
            mgr.validateForTest(__LINE__);
        }
        if (v.size() > 20) {
            uint64_t gidE = 2 + randx() % (v.size() - 1);
            uint64_t gidB = randx() % gidE;

            MetaDiffVec v1;
            std::copy(v.begin() + gidB, v.begin() + gidE, std::back_inserter(v1));
            MetaDiff mdiff = merge(v1);
            mgr.add(mdiff);
#if 0
            mgr.erase(v1);
#endif
            mgr.validateForTest(__LINE__);
        }
        v = mgr.getApplicableDiffList(snap0);
        if (v.size() > 10) {
            MetaDiffVec v1 = mgr.eraseBeforeGid(v[randx() % v.size()].snapE.gidB);
            snap0 = apply(snap0, v1);
            mgr.validateForTest(__LINE__);
        }
    };
}

void testDiffFileName(const MetaDiff& d)
{
    const std::string name = createDiffFileName(d);
    const MetaDiff dx = parseDiffFileName(name);
    CYBOZU_TEST_EQUAL(d, dx);
    CYBOZU_TEST_EQUAL(d.isMergeable, dx.isMergeable);
    CYBOZU_TEST_EQUAL(d.isCompDiff, dx.isCompDiff);
    CYBOZU_TEST_EQUAL(d.timestamp, dx.timestamp);
}

CYBOZU_TEST_AUTO(diffFileName)
{
    const size_t loop = 100;

    for (size_t i = 0; i < loop; i++) {
        const size_t b = randx() % 10;
        const size_t e = b + randx() % 10;
        {
            MetaDiff d = randDiff(MetaSnap(b));
            d.isMergeable = true;
            d.isCompDiff = true;
            testDiffFileName(d);
            d.isMergeable = true;
            d.isCompDiff = false;
            testDiffFileName(d);
            d.isMergeable = false;
            d.isCompDiff = true;
            testDiffFileName(d);
            d.isMergeable = false;
            d.isCompDiff = false;
            testDiffFileName(d);
        }
        {
            MetaDiff d = randDiff(MetaSnap(b, e));
            d.isMergeable = true;
            d.isCompDiff = true;
            testDiffFileName(d);
            d.isMergeable = true;
            d.isCompDiff = false;
            testDiffFileName(d);
            d.isMergeable = false;
            d.isCompDiff = true;
            testDiffFileName(d);
            d.isMergeable = false;
            d.isCompDiff = false;
            testDiffFileName(d);
        }
    }
}

CYBOZU_TEST_AUTO(snapStr)
{
    MetaSnap snap;
    {
        snap.set(5, 10);
        MetaSnap snap2 = strToMetaSnap(snap.str());
        CYBOZU_TEST_EQUAL(snap, snap2);
    }
    {
        snap.set(100);
        MetaSnap snap2 = strToMetaSnap(snap.str());
        CYBOZU_TEST_EQUAL(snap, snap2);
    }
}

CYBOZU_TEST_AUTO(metaStataStr)
{
    {
        MetaState metaSt(MetaSnap(100), ::time(0));
        MetaState metaSt2 = strToMetaState(metaSt.str());
        CYBOZU_TEST_EQUAL(metaSt, metaSt2);
    }
    {
        MetaState metaSt(MetaSnap(100, 200), ::time(0));
        MetaState metaSt2 = strToMetaState(metaSt.str());
        CYBOZU_TEST_EQUAL(metaSt, metaSt2);
    }
    {
        MetaState metaSt(MetaSnap(100), MetaSnap(300), ::time(0));
        MetaState metaSt2 = strToMetaState(metaSt.str());
        CYBOZU_TEST_EQUAL(metaSt, metaSt2);
    }
    {
        MetaState metaSt(MetaSnap(100), MetaSnap(300, 400), ::time(0));
        MetaState metaSt2 = strToMetaState(metaSt.str());
        CYBOZU_TEST_EQUAL(metaSt, metaSt2);
    }
    {
        MetaState metaSt(MetaSnap(100, 200), MetaSnap(300), ::time(0));
        MetaState metaSt2 = strToMetaState(metaSt.str());
        CYBOZU_TEST_EQUAL(metaSt, metaSt2);
    }
    {
        MetaState metaSt(MetaSnap(100, 200), MetaSnap(300, 400), ::time(0));
        MetaState metaSt2 = strToMetaState(metaSt.str());
        CYBOZU_TEST_EQUAL(metaSt, metaSt2);
    }
    {
        time_t ts = ::time(0);
        MetaState metaSt0 (MetaSnap(100), ts);
        MetaState metaSt1 = strToMetaState(metaSt0.strTs());
        CYBOZU_TEST_EQUAL(metaSt0, metaSt1);
        CYBOZU_TEST_EQUAL(metaSt0.timestamp, metaSt1.timestamp);
    }
}
