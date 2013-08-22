#include <memory>
#include "cybozu/test.hpp"
#include "thread_util.hpp"
#include "random.hpp"

class Worker : public cybozu::thread::Runnable
{
    void operator()() override {
        try {
            cybozu::util::Random<uint32_t> rand(100, 300);
            std::this_thread::sleep_for(std::chrono::milliseconds(rand()));
            ::printf("done\n");
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
};

CYBOZU_TEST_AUTO(test)
{
    cybozu::thread::ThreadRunnerPool pool;
    for (int i = 0; i < 10; i++) {
        pool.add(std::make_shared<Worker>()).start();
    }
    pool.join();
    CYBOZU_TEST_ASSERT(pool.size() == 0);
}
