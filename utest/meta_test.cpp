#include "cybozu/test.hpp"
#include "cybozu/file.hpp"
#include "meta.hpp"

CYBOZU_TEST_AUTO(diff)
{
    walb::MetaDiff d0(0, 0, 1, 2), d1(1, 2);
    d1.canMerge = true;
    CYBOZU_TEST_ASSERT(walb::canMerge(d0, d1));
    walb::MetaDiff d2 = walb::merge(d0, d1);
    CYBOZU_TEST_ASSERT(d2 == walb::MetaDiff(0, 2));
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
        walb::MetaDiff d1(0, 13, 10, 12);
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
        walb::MetaDiff d1(0, 0, 10, 10);
        walb::MetaSnap s1(10, 10);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 0);
        walb::MetaDiff d1(0, 1, 2, 3);
        CYBOZU_TEST_ASSERT(!walb::canApply(s0, d1));
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1(0, 0, 12, 12);
        walb::MetaSnap s1(12, 12);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(5, 10);
        walb::MetaDiff d1(5, 5, 9, 9);
        walb::MetaSnap s1(9, 10);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 5);
        walb::MetaDiff d1(0, 5, 15, 16);
        walb::MetaSnap s1(15, 16);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 5);
        walb::MetaDiff d1(0, 5, 3, 6);
        walb::MetaSnap s1(3, 6);
        testApply(s0, d1, s1);
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1(0, 5, 12, 15);
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
