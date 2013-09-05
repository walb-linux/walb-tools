#include "aio_util.hpp"
#include "random.hpp"

namespace cybozu {
namespace aio {

static void testAioDataAllocator()
{
    AioDataAllocator allocator;
    std::queue<AioDataPtr> queue;
    const size_t nTrials = 1000000;

    while (queue.size() < 64) {
        AioDataPtr p = allocator.alloc();
        queue.push(p);
        //::printf("add %u\n", p->key);
    }

    util::Random<size_t> rand;

    double bTime = util::getTime();
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
    double eTime = util::getTime();

    while (!queue.empty()) {
        AioDataPtr p = queue.front();
        queue.pop();
        //::printf("del %u\n", p->key);
    }
    ::printf("%.06f sec. %.f /sec.\n",
             (eTime - bTime),
             (double)nTrials / (eTime - bTime));
}

}} //namespace cybozu::aio

int main()
{
    cybozu::aio::testAioDataAllocator();
}

/* end of file. */
