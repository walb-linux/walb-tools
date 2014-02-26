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
}

bool g_b;

struct A
{
    std::recursive_mutex mu;
    explicit A(const std::string &) {
        if (g_b) throw std::exception();
    }
};

CYBOZU_TEST_AUTO(StateMapException)
{
    using AutoLock = std::lock_guard<std::recursive_mutex>;
    walb::StateMap<A> stMap;
    g_b = true;
    try {
        A &a = stMap.get("vol0");
        AutoLock lk(a.mu);
    } catch (...) {
    }
    g_b = false;
    {
        A &a = stMap.get("vol0");
        AutoLock lk(a.mu);
    }
}
