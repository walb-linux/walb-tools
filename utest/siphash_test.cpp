#include "cybozu/test.hpp"
#include "siphash.hpp"
#include "random.hpp"

cybozu::util::Random<size_t> rand_;

CYBOZU_TEST_AUTO(simpleSiphash)
{
    std::vector<char> buf;
    for (size_t i = 0; i < 64; i++) {
        buf.resize(i);
        rand_.fill(buf.data(), buf.size());
        cybozu::sipHash24_64(buf.data(), buf.size());
    }
}

CYBOZU_TEST_AUTO(siphashFromStream)
{
    std::vector<char> buf;
    buf.resize(64 * 1024 + rand_() % 1024);
    rand_.fill(buf.data(), buf.size());

    const size_t N = 100;
    std::vector<uint64_t> hashV(N);
    for (size_t i = 0; i < N; i++) {
        size_t off = 0;
        cybozu::SipHash24 hasher;
        while (off < buf.size()) {
            const size_t s = std::min(buf.size() - off, rand_() % 1024);
            hasher.compress(&buf[off], s);
            off += s;
        }
        hashV[i] = hasher.finalize();
    }
    for (size_t i = 1; i < N; i++) {
        CYBOZU_TEST_EQUAL(hashV[0], hashV[i]);
    }
}
