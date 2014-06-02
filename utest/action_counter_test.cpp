#include "cybozu/test.hpp"
#include "action_counter.hpp"
#include "walb_types.hpp"
#include <thread>
#include <vector>
#include <mutex>

CYBOZU_TEST_AUTO(actionCounter)
{
    std::recursive_mutex mu;
    walb::ActionCounters ac(mu);
    {
        walb::ActionCounterTransaction tran(ac, "a0");
        CYBOZU_TEST_EQUAL(ac.getValue("a0"), 1);
    }
    CYBOZU_TEST_EQUAL(ac.getValue("a0"), 0);
}

CYBOZU_TEST_AUTO(actionCounters)
{
    walb::StrVec tbl({"a0", "a1", "a2"});
    std::recursive_mutex mu;
    walb::ActionCounters ac(mu);

    auto doWork = [&]() {
        for (size_t i = 0; i < 1000; i++) {
            for (const std::string &s : tbl) {
                walb::ActionCounterTransaction tran(ac, s);
            }
        }
    };

    std::vector<std::thread> v;
    for (size_t i = 0; i < 10; i++) {
        v.emplace_back(doWork);
    }
    for (auto &th : v) {
        th.join();
    }
    std::vector<int> iv = ac.getValues(tbl);
    CYBOZU_TEST_EQUAL(iv.size(), 3);
    for (size_t i = 0; i < 3; i++) {
        CYBOZU_TEST_EQUAL(ac.getValue(tbl[i]), 0);
        CYBOZU_TEST_EQUAL(iv[i], 0);
    }
}
