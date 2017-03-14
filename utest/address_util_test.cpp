#include "cybozu/test.hpp"
#include "random.hpp"
#include "address_util.hpp"
#include <unistd.h>

/**
 * for CYBOZU_TEST_EQUAL to treat std::vector<uint32_t> values.
 */
std::ostream& operator<<(std::ostream& os, const std::vector<uint32_t>& v)
{
    if (v.empty()) {
        os << "[]";
        return os;
    }
    os << "[" << v[0];
    for (size_t i = 1; i < v.size(); i++) {
        os << ", " << v[i];
    }
    os << "]";
    return os;
}

void doTest(uint64_t addr, uint32_t blks, const std::vector<uint32_t>& answer)
{
    const std::vector<uint32_t> v = splitIoToAligned(addr, blks);
    CYBOZU_TEST_EQUAL(v, answer);
    uint64_t a = addr;
    uint32_t b = 0;
    for (uint32_t s : v) {
        CYBOZU_TEST_ASSERT(isAlignedIo(a, s));
        a += s;
        b += s;
    }
    CYBOZU_TEST_EQUAL(b, blks);
}

CYBOZU_TEST_AUTO(splitIoToAligned)
{
    doTest(0, 1, {1});
    doTest(0, 2, {2});
    doTest(0, 4, {4});
    doTest(0, 8, {8});
    doTest(0, 16, {16});
    doTest(0, 32, {32});
    doTest(1, 31, {1, 2, 4, 8, 16});
    doTest(0, 31, {16, 8, 4, 2, 1});
}

void doTestLoop(uint64_t maxAddr, uint32_t maxBlks)
{
    cybozu::util::Xoroshiro128Plus rand(::time(0) + ::getpid());
    for (size_t i = 0; i < 10000; i++) {
        uint64_t addr = rand() % maxAddr;
        uint32_t blks = std::min(rand(), UINT64_MAX - addr) % maxBlks;
        if (blks == 0) continue;

#if 0
        ::printf("addr %" PRIx64 " blks %u\n", addr, blks);
#endif
        std::vector<uint32_t> v = splitIoToAligned(addr, blks);
        uint64_t a = addr;
        uint32_t b = 0;
        for (uint32_t s : v) {
#if 0
            ::printf("%" PRIx64 "\t%u\t%d\n", a, s, isAlignedIo(a, s));
#endif
            CYBOZU_TEST_ASSERT(isAlignedIo(a, s));
            a += s;
            b += s;
        }
#if 0
        ::printf("blks %u b %u\n", blks, b);
#endif
    }
}

CYBOZU_TEST_AUTO(splitIoToAlignedLoop)
{
    doTestLoop(64, 64);
    doTestLoop(1024, 1024);
    doTestLoop(UINT64_MAX, UINT32_MAX);
}
