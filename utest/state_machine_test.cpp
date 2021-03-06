#include "cybozu/test.hpp"
#include "state_machine.hpp"

std::recursive_mutex g_m;

CYBOZU_TEST_AUTO(add)
{
    walb::StateMachine sm(g_m);
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
    walb::StateMachine sm(g_m);
    sm.addEdge("a", "b");
    sm.addEdge("a", "c");
    sm.addEdge("a", "d");
    sm.addEdge("b", "c");
    sm.addEdge("c", "d");
    sm.addEdge("c", "e");
    sm.set("a");
    // fail
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
    // fail
    CYBOZU_TEST_ASSERT(!sm.change("d", "c"));
    CYBOZU_TEST_EQUAL(sm.get(), "d");
    sm.set("a");
    CYBOZU_TEST_EQUAL(sm.get(), "a");
}

CYBOZU_TEST_AUTO(trans)
{
    walb::StateMachine sm(g_m);
    sm.addEdge("a", "b");
    sm.addEdge("a", "c");
    sm.addEdge("a", "d");
    sm.addEdge("b", "c");
    sm.addEdge("c", "d");
    sm.set("a");
    {
        walb::StateMachineTransaction ts(sm);
        // fail
        CYBOZU_TEST_ASSERT(!ts.tryChange("a", "e"));
        CYBOZU_TEST_EQUAL(sm.get(), "a");
        // change
        CYBOZU_TEST_ASSERT(ts.tryChange("a", "b"));
        CYBOZU_TEST_EQUAL(sm.get(), "b");
        ts.commit("c");
        CYBOZU_TEST_EQUAL(sm.get(), "c");
    }
    CYBOZU_TEST_EQUAL(sm.get(), "c");
    // rollback
    sm.set("a");
    {
        walb::StateMachineTransaction ts(sm);
        CYBOZU_TEST_ASSERT(ts.tryChange("a", "b"));
        CYBOZU_TEST_EQUAL(sm.get(), "b");
    }
    CYBOZU_TEST_EQUAL(sm.get(), "a");
    // err
    sm.set("a");
    {
        walb::StateMachineTransaction ts(sm);

        CYBOZU_TEST_EXCEPTION(ts.commit("b"), cybozu::Exception);
    }
    {
        walb::StateMachineTransaction ts(sm);
        CYBOZU_TEST_ASSERT(ts.tryChange("a", "b"));
        CYBOZU_TEST_EQUAL(sm.get(), "b");
        CYBOZU_TEST_EXCEPTION(ts.tryChange("b", "c"), cybozu::Exception);
        CYBOZU_TEST_EXCEPTION(sm.change("b", "c"), cybozu::Exception);
        ts.commit("c");
        CYBOZU_TEST_EQUAL(sm.get(), "c");
        CYBOZU_TEST_EXCEPTION(ts.commit("d"), cybozu::Exception);
    }
    CYBOZU_TEST_EQUAL(sm.get(), "c");
}

CYBOZU_TEST_AUTO(double_trans)
{
    walb::StateMachine sm(g_m);
    sm.addEdge("a", "b");
    sm.addEdge("a", "c");
    sm.addEdge("b", "c");
    sm.addEdge("c", "d");
    sm.addEdge("b", "a");
    sm.set("a");
    {
        walb::StateMachineTransaction ts(sm);
        ts.tryChange("a", "b");
        {
            walb::StateMachineTransaction ts2(sm);
        }
        ts.commit("c");
    }
    sm.set("a");
    {
        walb::StateMachineTransaction ts(sm);
        ts.tryChange("a", "b");
        {
            walb::StateMachineTransaction ts2(sm);
            CYBOZU_TEST_EXCEPTION(ts2.tryChange("a", "c"), cybozu::Exception);
        }
        ts.commit("c");
    }

    sm.set("a");
    walb::StateMachineTransaction ts2(sm);
    {
        walb::StateMachineTransaction ts(sm, "a", "b");
        ts.commit("a");
        ts2.tryChange("a", "b");
    }
    ts2.commit("a");
}
