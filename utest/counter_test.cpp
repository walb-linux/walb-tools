#include "cybozu/test.hpp"
#include "counter.hpp"

using namespace counter;

const int type = 0;

CYBOZU_TEST_AUTO(counter)
{
    CYBOZU_TEST_EQUAL(getCounter<type>(), 0);
    {
        CounterTransaction<type> trn;
        CYBOZU_TEST_EQUAL(trn.get(), 1);
    }
    CYBOZU_TEST_EQUAL(getCounter<type>(), 0);
    {
        CounterTransaction<type> trn0;
        CYBOZU_TEST_EQUAL(trn0.get(), 1);
        CounterTransaction<type> trn1;
        CYBOZU_TEST_EQUAL(trn0.get(), 2);
        CYBOZU_TEST_EQUAL(trn1.get(), 2);
    }
    CYBOZU_TEST_EQUAL(getCounter<type>(), 0);
}
