#include <cybozu/test.hpp>
#include <cstdio>
#include <cstring>
#include "random.hpp"
#include "compressed_data.hpp"
#include "thread_util.hpp"
#include "checksum.hpp"

using namespace walb;

void testCompressedData(std::vector<char> &&v)
{
    CompressedData cd0, cd1, cd2;
    cd0.setUncompressed(std::move(v));
    cd1 = cd0;
    cd1.compress();
    if (cd1.isCompressed()) {
        cd2 = cd1;
        cd2.uncompress();
    } else {
        cd2 = cd1;
    }
    CYBOZU_TEST_EQUAL(cd0.rawSize(), cd2.rawSize());
    CYBOZU_TEST_ASSERT(::memcmp(cd0.rawData(), cd2.rawData(), cd0.rawSize()) == 0);
#if 0
    ::printf("orig size %zu compressed size %zu\n", cd0.rawSize(), cd1.rawSize());
#endif
}

CYBOZU_TEST_AUTO(compressedData)
{
    cybozu::util::Random<uint32_t> rand;
    for (size_t i = 0; i < 100; i++) {
        size_t s = rand.get16() + 32;
        std::vector<char> v(s);
        rand.fill(&v[0], 32);
        testCompressedData(std::move(v));
    }
}

void throwErrorIf(std::vector<std::exception_ptr> &&ev)
{
    bool isError = false;
    if (!ev.empty()) {
        isError = true;
        ::fprintf(::stderr, "Number of error: %zu\n", ev.size());
    }
    for (std::exception_ptr &ep : ev) {
        try {
            std::rethrow_exception(ep);
        } catch (std::exception &e) {
            ::fprintf(::stderr, "caught error: %s.\n", e.what());
        } catch (...) {
            ::fprintf(::stderr, "caught unknown error.\n");
        }
    }
    ev.clear();
    if (isError) throw std::runtime_error("Error ocurred.");
}

uint32_t calcCsum(const CompressedData &data)
{
    return cybozu::util::calcChecksum(data.rawData(), data.rawSize(), 0);
}

CYBOZU_TEST_AUTO(compressor)
{
    using BoundedQ = cybozu::thread::BoundedQueue<CompressedData>;
    size_t qs = 10;
    BoundedQ q0(qs), q1(qs), q2(qs);

    class Producer
    {
    private:
        BoundedQ &outQ_;
        size_t n_;
        std::vector<uint32_t> csumV_;
    public:
        Producer(BoundedQ &outQ, size_t n, std::vector<uint32_t> &csumV)
            : outQ_(outQ), n_(n), csumV_(csumV) {}
        void operator()() try {
            cybozu::util::Random<uint32_t> rand;
            for (size_t i = 0; i < n_; i++) {
                size_t s = rand.get16() + 32;
                std::vector<char> v(s);
                rand.fill(&v[0], 32);
                CompressedData cd;
                cd.setUncompressed(std::move(v));
                csumV_.push_back(calcCsum(cd));
                outQ_.push(std::move(cd));
            }
            outQ_.sync();
        } catch (...) {
            outQ_.fail();
            throw;
        }
    };
    class Consumer
    {
    private:
        BoundedQ &inQ_;
        std::vector<uint32_t> csumV_;
    public:
        Consumer(BoundedQ &inQ, std::vector<uint32_t> &csumV)
            : inQ_(inQ), csumV_(csumV) {}
        void operator()() try {
            CompressedData cd;
            while (inQ_.pop(cd)) {
                uint32_t csum = calcCsum(cd);
                csumV_.push_back(csum);
            }
        } catch (...) {
            inQ_.fail();
            throw;
        }
    };

    std::vector<uint32_t> csumV0, csumV1;
    auto producer = std::make_shared<Producer>(q0, 100, csumV0);
    auto compressor = [&]() {
        try {
            CompressedData d;
            while (q0.pop(d)) {
                if (!d.isCompressed()) d.compress();
                q1.push(std::move(d));
            }
            q1.sync();
        } catch (...) {
            q0.fail();
            q1.fail();
            throw;
        }
    };
    auto uncompressor = [&]() {
        try {
            CompressedData d;
            while (q1.pop(d)) {
                if (d.isCompressed()) d.uncompress();
                q2.push(std::move(d));
            }
            q2.sync();
        } catch (...) {
            q1.fail();
            q2.fail();
            throw;
        }
    };
    auto consumer = std::make_shared<Consumer>(q2, csumV1);
    cybozu::thread::ThreadRunnerSet thSet;
    thSet.add(producer);
    thSet.add(compressor);
    thSet.add(uncompressor);
    thSet.add(consumer);
    thSet.start();
    throwErrorIf(thSet.join());

    CYBOZU_TEST_ASSERT(csumV0 == csumV1);
}
