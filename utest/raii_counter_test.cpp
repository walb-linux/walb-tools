#include "cybozu/test.hpp"
#include "raii_counter.hpp"
#include <thread>
#include <vector>
#include <mutex>

CYBOZU_TEST_AUTO(raiiCounter)
{
    std::mutex mu;
    walb::RaiiCounter c;
    c.setMutex(&mu);

    auto doWork = [&]() {
        for (size_t i = 0; i < 1000; i++) {
            std::lock_guard<walb::RaiiCounter> lk(c);
        }
    };

    std::vector<std::thread> v;
    for (size_t i = 0; i < 10; i++) {
        v.emplace_back(doWork);
    }
    for (auto &th : v) {
        th.join();
    }
    CYBOZU_TEST_EQUAL(c.get(), 0);
}

CYBOZU_TEST_AUTO(multiRaiiCounter)
{
    std::mutex mu;
    walb::MultiRaiiCounter c(mu);

    auto doWork = [&]() {
        for (size_t i = 0; i < 1000; i++) {
            for (const std::string &s : {"a0", "a1", "a2"}) {
                auto lk(c.getLock(s));
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

    std::vector<int> iv = c.getValues({"a0", "a1", "a2"});
    CYBOZU_TEST_EQUAL(iv.size(), 3);
    for (int i : iv) CYBOZU_TEST_EQUAL(i, 0);
}
