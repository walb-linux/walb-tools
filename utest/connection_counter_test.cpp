#include "cybozu/test.hpp"
#include "connection_counter.hpp"

CYBOZU_TEST_AUTO(connectionCounter)
{
    CYBOZU_TEST_EQUAL(getConnectionCounter(), 0);
    {
        ConnectionCounterTransation trn;
        CYBOZU_TEST_EQUAL(trn.get(), 1);
    }
    CYBOZU_TEST_EQUAL(getConnectionCounter(), 0);
    {
        ConnectionCounterTransation trn0;
        CYBOZU_TEST_EQUAL(trn0.get(), 1);
        ConnectionCounterTransation trn1;
        CYBOZU_TEST_EQUAL(trn0.get(), 2);
        CYBOZU_TEST_EQUAL(trn1.get(), 2);
    }
    CYBOZU_TEST_EQUAL(getConnectionCounter(), 0);
}
