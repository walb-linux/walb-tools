#include "cybozu/test.hpp"
#include "state_map.hpp"

struct State {
    std::string id;
    State(const std::string& id) : id(id) {}
};

CYBOZU_TEST_AUTO(StateMap)
{
    walb::StateMap<State> m;
    {
        State& s = m.get("abc");
        CYBOZU_TEST_EQUAL(s.id, "abc");
        s.id = "xyz";
    }
    {
        State& s = m.get("abc");
        CYBOZU_TEST_EQUAL(s.id, "xyz");
    }
    m.del("abc");
    {
        State& s = m.get("abc");
        CYBOZU_TEST_EQUAL(s.id, "abc");
    }
    m.del("abc");
    CYBOZU_TEST_EXCEPTION(m.del("abc"), cybozu::Exception);
}
