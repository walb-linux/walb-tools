#include "cybozu/test.hpp"
#include "cybozu/file.hpp"
#include "meta.hpp"

CYBOZU_TEST_AUTO(diff)
{
    walb::MetaDiff d0(0, 1, 2), d1(1, 2, 3);
    d1.raw.can_merge = 1;
    CYBOZU_TEST_ASSERT(d0.canMerge(d1));
    walb::MetaDiff d2 = d0.merge(d1);
    CYBOZU_TEST_ASSERT(d2 == walb::MetaDiff(0, 2, 3));
}

CYBOZU_TEST_AUTO(snap)
{
    walb::MetaSnap s0(0, 10);
    walb::MetaDiff d1(0, 10, 12);
    CYBOZU_TEST_ASSERT(s0.apply(d1) == walb::MetaSnap(10, 12));
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

CYBOZU_TEST_AUTO(apply)
{
    {
        walb::MetaSnap s0(0, 0);
        walb::MetaDiff d1(0, 10, 10);
        CYBOZU_TEST_EQUAL(s0.startToApply(d1).finishToApply(d1), s0.apply(d1));
    }
    {
        walb::MetaSnap s0(0, 10);
        walb::MetaDiff d1(0, 10, 12);
        CYBOZU_TEST_EQUAL(s0.startToApply(d1).finishToApply(d1), s0.apply(d1));
    }
    {
        walb::MetaSnap s0(5, 10);
        walb::MetaDiff d1(0, 15, 16);
        CYBOZU_TEST_EQUAL(s0.startToApply(d1).finishToApply(d1), s0.apply(d1));
    }
}
