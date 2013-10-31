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

CYBOZU_TEST_AUTO(BoundedQueue)
{
    cybozu::thread::BoundedQueue<int> q(10);
    std::exception_ptr ep0, ep1;
    int total = 0;

    std::thread th0([&q, &ep0]() {
            try {
                for (size_t i = 0; i < 100; i++) {
                    q.push(1);
                }
                q.sync();
            } catch (...) {
                ep0 = std::current_exception();
            }
        });
    std::thread th1([&q, &ep1, &total]() {
            try {
                for (size_t i = 0; i < 100; i++) {
                    total += q.pop();
                }
            } catch (...) {
                ep1 = std::current_exception();
            }
        });

    th0.join();
    th1.join();

    CYBOZU_TEST_ASSERT(!ep0);
    CYBOZU_TEST_ASSERT(!ep1);
    CYBOZU_TEST_EQUAL(total, 100);
}

CYBOZU_TEST_AUTO(BoundedQueueMove)
{
    cybozu::thread::BoundedQueue<std::unique_ptr<int>, true> q(10);
    std::exception_ptr ep0, ep1;
    int total = 0;

    std::thread th0([&q, &ep0]() {
            try {
                for (size_t i = 0; i < 100; i++) {
                    q.push(std::unique_ptr<int>(new int(1)));
                }
                q.sync();
            } catch (...) {
                ep0 = std::current_exception();
            }
        });
    std::thread th1([&q, &ep1, &total]() {
            try {
                for (size_t i = 0; i < 100; i++) {
                    std::unique_ptr<int> ip = q.pop();
                    total += *ip;
                }
            } catch (...) {
                ep1 = std::current_exception();
            }
        });

    th0.join();
    th1.join();

    CYBOZU_TEST_ASSERT(!ep0);
    CYBOZU_TEST_ASSERT(!ep1);
    CYBOZU_TEST_EQUAL(total, 100);
}

CYBOZU_TEST_AUTO(BoundedQueueResize)
{
    cybozu::thread::BoundedQueue<size_t> q;
    size_t n = 100000;
    size_t total = 0;

    std::thread th0([&q, &total] {
            try {
                size_t c;
                while (q.pop(c)) {
                    total += c;
                }
            } catch (...) {
                q.fail();
            }
        });

    for (size_t i = 0; i < n; i++) {
        if (i % 100 == 0) q.resize(i % 10 + 10);
        q.push(1);
    }
    q.sync();
    th0.join();

    CYBOZU_TEST_EQUAL(total, n);
}
