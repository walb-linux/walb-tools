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

CYBOZU_TEST_AUTO(unlimitedPool)
{
    cybozu::thread::ThreadRunnerPool pool;
    cybozu::util::Random<uint32_t> rand(100, 300);
    for (int i = 0; i < 10; i++) {
        pool.add(std::make_shared<Worker>(i, rand()));
    }
    pool.waitForAll();
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
    for (uint32_t id : v) pool.waitFor(id);
    CYBOZU_TEST_ASSERT(pool.size() == 0);
    pool.waitForAll();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}

CYBOZU_TEST_AUTO(fixedPoolCancel)
{
    const int POOL_SIZE = 5;
    const int N_TASKS = 15;
    cybozu::thread::ThreadRunnerPool pool(POOL_SIZE);
    std::vector<uint32_t> v;
    for (int i = 0; i < N_TASKS; i++) {
        uint32_t id = pool.add(std::make_shared<Worker>(i, 200));
        v.push_back(id);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int count = 0;
    for (uint32_t id : v) {
        if (pool.cancel(id)) count++;
    }
    CYBOZU_TEST_EQUAL(count, N_TASKS - POOL_SIZE);
    pool.waitForAll();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}

struct FailWorker : public cybozu::thread::Runnable
{
    int i_;
    size_t sleepMs_;
    FailWorker(int i, size_t sleepMs) : i_(i), sleepMs_(sleepMs) {}
    void operator()() override {
        try {
            ::printf("start %d\n", i_);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs_));
            ::printf("done %d\n", i_);
            throw std::runtime_error("error exception for test.");
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
};

CYBOZU_TEST_AUTO(poolWithFailWorker)
{
    cybozu::thread::ThreadRunnerPool pool;
    std::vector<uint32_t> v;
    cybozu::util::Random<uint32_t> rand(100, 300);
    for (int i = 0; i < 10; i++) {
        uint32_t id = pool.add(std::make_shared<FailWorker>(i, rand()));
        pool.gc();
        v.push_back(id);
    }
    for (uint32_t id : v) {
        std::exception_ptr ep = pool.waitFor(id);
        CYBOZU_TEST_ASSERT(ep);
    }
    CYBOZU_TEST_ASSERT(pool.size() == 0);
    pool.waitForAll();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}
