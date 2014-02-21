#include "cybozu/test.hpp"
#include "state_machine.hpp"

CYBOZU_TEST_AUTO(add)
{
    walb::StateMachine sm;
    sm.addEdge("a", "b");
    sm.addEdge("a", "c");
    sm.addEdge("a", "d");
    sm.addEdge("b", "c");
    CYBOZU_TEST_EXCEPTION(sm.get(), cybozu::Exception);
    sm.set("a");
    CYBOZU_TEST_EQUAL(sm.get(), "a");
    CYBOZU_TEST_EXCEPTION(sm.set("none"), cybozu::Exception);
    sm.set("d");
}

CYBOZU_TEST_AUTO(change)
{
    walb::StateMachine sm;
    sm.addEdge("a", "b");
    sm.addEdge("a", "c");
    sm.addEdge("a", "d");
    sm.addEdge("b", "c");
    sm.addEdge("c", "d");
    sm.set("a");
    // change fail
    CYBOZU_TEST_ASSERT(!sm.change("a", "x"));
    CYBOZU_TEST_ASSERT(!sm.change("b", "c"));
    CYBOZU_TEST_EQUAL(sm.get(), "a");
    // change
    CYBOZU_TEST_ASSERT(sm.change("a", "b"));
    CYBOZU_TEST_EQUAL(sm.get(), "b");
    CYBOZU_TEST_ASSERT(sm.change("b", "c"));
    CYBOZU_TEST_EQUAL(sm.get(), "c");
    CYBOZU_TEST_ASSERT(sm.change("c", "d"));
    CYBOZU_TEST_EQUAL(sm.get(), "d");
    // change fail
    CYBOZU_TEST_ASSERT(!sm.change("d", "c"));
    CYBOZU_TEST_EQUAL(sm.get(), "d");
}
