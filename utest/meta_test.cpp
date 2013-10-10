#include "cybozu/test.hpp"
#include "cybozu/file.hpp"
#include "meta.hpp"

CYBOZU_TEST_AUTO(diff)
{
    walb::MetaDiff d0(0, 0, 1, 2), d1(1, 1, 2, 3);
    d1.setCanMerge(true);
    CYBOZU_TEST_ASSERT(walb::canMerge(d0, d1));
    walb::MetaDiff d2 = walb::merge(d0, d1);
    CYBOZU_TEST_ASSERT(d2 == walb::MetaDiff(0, 0, 2, 3));
}

CYBOZU_TEST_AUTO(snap)
{
    {
        walb::MetaSnap s0(0, 7);
        walb::MetaDiff d1(0, 0, 10, 12);
        CYBOZU_TEST_ASSERT(walb::apply(s0, d1) == walb::MetaSnap(10, 12));
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1(0, 0, 10, 12);
        CYBOZU_TEST_ASSERT(walb::apply(s0, d1) == walb::MetaSnap(10, 12));
    }
    {
        walb::MetaSnap s0(0, 13);
        walb::MetaDiff d1(0, 0, 10, 12);
        CYBOZU_TEST_ASSERT(walb::apply(s0, d1) == walb::MetaSnap(10, 13));
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

void testApply(const walb::MetaSnap &s0, const walb::MetaDiff &d1)
{
    walb::MetaSnap s1, s2, sa, sb;
    sa = walb::startToApply(s0, d1);
    sb = walb::startToApply(sa, d1);
    CYBOZU_TEST_EQUAL(sa, sb);

    s1 = walb::finishToApply(sa, d1);
    s2 = walb::apply(s0, d1);
    CYBOZU_TEST_EQUAL(s1, s2);
}

CYBOZU_TEST_AUTO(apply)
{
    {
        walb::MetaSnap s0(0, 0);
        walb::MetaDiff d1(0, 0, 10, 10);
        testApply(s0, d1);
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1(0, 0, 10, 12);
        testApply(s0, d1);
    }
    {
        walb::MetaSnap s0(5, 10);
        walb::MetaDiff d1(5, 5, 15, 16);
        testApply(s0, d1);
    }
    {
        walb::MetaSnap s0(0, 5);
        walb::MetaDiff d1(0, 5, 15, 16);
        testApply(s0, d1);
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
#if 0
    {
        /* If canApply() use more accurate definition,
           This test will pass. */
        walb::MetaSnap s0(5);
        walb::MetaDiff d1(3, 7);
        CYBOZU_TEST_ASSERT(walb::canApply(s0, d1));
        CYBOZU_TEST_ASSERT(!walb::isTooOld(s0, d1));
        CYBOZU_TEST_ASSERT(!walb::isTooNew(s0, d1));
    }
#endif
}
