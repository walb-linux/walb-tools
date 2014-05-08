#include "cybozu/test.hpp"
#include "aio_util.hpp"
#include "random.hpp"

using namespace cybozu::aio;

void testAioDataAllocator(size_t nTrials)
{
    AioDataAllocator allocator;
    std::queue<AioDataPtr> queue;

    while (queue.size() < 64) {
        AioDataPtr p = allocator.alloc();
        queue.push(p);
        //::printf("add %u\n", p->key);
    }

    cybozu::util::Random<size_t> rand;

    const double bTime = cybozu::util::getTime();
    for (size_t i = 0; i < nTrials; i++) {
        int nr = rand() % 10;
        for (int j = 0; j < nr; j++) {
            AioDataPtr p = queue.front();
            queue.pop();
            queue.push(p);
        }
        AioDataPtr p = queue.front();
        queue.pop();
        //::printf("del %u\n", p->key);

        p = allocator.alloc();
        queue.push(p);
        //::printf("add %u\n", p->key);
    }
    const double eTime = cybozu::util::getTime();

    while (!queue.empty()) {
        AioDataPtr p = queue.front();
        queue.pop();
        //::printf("del %u\n", p->key);
    }
    ::printf("%.06f sec. %.f /sec.\n",
             (eTime - bTime),
             (double)nTrials / (eTime - bTime));
}

CYBOZU_TEST_AUTO(testAioDataAllocator)
{
    const size_t nTrials = 10000;
    testAioDataAllocator(nTrials);
}
