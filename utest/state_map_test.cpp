#include "cybozu/test.hpp"
#include "state_map.hpp"

struct State {
    int a;
    State() : a(5) {}
};

CYBOZU_TEST_AUTO(StateMap)
{
    walb::StateMap<State> m;
    {
        bool maked = false;
        State& s = m.get("abc", &maked);
        CYBOZU_TEST_ASSERT(maked);
        CYBOZU_TEST_EQUAL(s.a, 5);
    }
    {
        bool maked = false;
        State& s = m.get("abc", &maked);
        CYBOZU_TEST_ASSERT(!maked);
        CYBOZU_TEST_EQUAL(s.a, 5);
        s.a = 9;
    }
    {
        bool maked = false;
        State& s = m.get("abc", &maked);
        CYBOZU_TEST_ASSERT(!maked);
        CYBOZU_TEST_EQUAL(s.a, 9);
    }
    m.del("abc");
    {
        bool maked = false;
        State& s = m.get("abc", &maked);
        CYBOZU_TEST_ASSERT(maked);
        CYBOZU_TEST_EQUAL(s.a, 5);
    }
    m.del("abc");
    CYBOZU_TEST_EXCEPTION(m.del("abc"), cybozu::Exception);
}

