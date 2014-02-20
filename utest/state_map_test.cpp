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
        State *p = 0;
        bool maked = false;
        std::tie(p, maked) = m.get("abc");
        CYBOZU_TEST_ASSERT(maked);
        CYBOZU_TEST_ASSERT(p);
        CYBOZU_TEST_EQUAL(p->a, 5);
    }
    {
        State *p = 0;
        bool maked = false;
        std::tie(p, maked) = m.get("abc");
        CYBOZU_TEST_ASSERT(!maked);
        CYBOZU_TEST_ASSERT(p);
        CYBOZU_TEST_EQUAL(p->a, 5);
        p->a = 9;
    }
    {
        State *p = 0;
        bool maked = false;
        std::tie(p, maked) = m.get("abc");
        CYBOZU_TEST_ASSERT(!maked);
        CYBOZU_TEST_ASSERT(p);
        CYBOZU_TEST_EQUAL(p->a, 9);
    }
    m.del("abc");
    {
        State *p = 0;
        bool maked = false;
        std::tie(p, maked) = m.get("abc");
        CYBOZU_TEST_ASSERT(maked);
        CYBOZU_TEST_ASSERT(p);
        CYBOZU_TEST_EQUAL(p->a, 5);
    }
    m.del("abc");
    CYBOZU_TEST_EXCEPTION(m.del("abc"), cybozu::Exception);
}

