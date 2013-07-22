#include "cybozu/test.hpp"
#include "meta.hpp"

CYBOZU_TEST_AUTO(diff)
{
    walb::MetaDiff d0(0, 1, 2), d1(1, 2, 3);
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
