#include <memory>
#include <vector>
#include "cybozu/test.hpp"
#include "thread_util.hpp"
#include "random.hpp"

struct Worker : public cybozu::thread::Runnable
{
    int i_;
    size_t sleepMs_;
    Worker(int i, size_t sleepMs) : i_(i), sleepMs_(sleepMs) {}
    void operator()() override {
        try {
            ::printf("start %d\n", i_);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs_));
            ::printf("done %d\n", i_);
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
};

CYBOZU_TEST_AUTO(pool)
{
    cybozu::thread::ThreadRunnerPool pool;
    cybozu::util::Random<uint32_t> rand(100, 300);
    for (int i = 0; i < 10; i++) {
        pool.add(std::make_shared<Worker>(i, rand()));
    }
    pool.join();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}

CYBOZU_TEST_AUTO(fixedPool)
{
    cybozu::thread::ThreadRunnerPool pool(5);
    std::vector<uint32_t> v;
    cybozu::util::Random<uint32_t> rand(100, 300);
    for (int i = 0; i < 10; i++) {
        uint32_t id = pool.add(std::make_shared<Worker>(i, rand()));
        v.push_back(id);
    }
    for (uint32_t id : v) pool.join(id);
    CYBOZU_TEST_ASSERT(pool.size() == 0);
    pool.join();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}

CYBOZU_TEST_AUTO(fixedPoolCancel)
{
    cybozu::thread::ThreadRunnerPool pool(5);
    std::vector<uint32_t> v;
    for (int i = 0; i < 10; i++) {
        uint32_t id = pool.add(std::make_shared<Worker>(i, 200));
        v.push_back(id);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    size_t count = 0;
    for (uint32_t id : v) {
        if (pool.cancel(id)) count++;
    }
    CYBOZU_TEST_EQUAL(count, 5);
    pool.join();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}
