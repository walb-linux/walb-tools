#include "cybozu/test.hpp"
#include "random.hpp"
#include "address_util.hpp"
#include <unistd.h>


CYBOZU_TEST_AUTO(MaxAlignedSize)
{
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(0U), 0U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(1U), 1U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(2U), 2U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(3U), 2U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(4U), 4U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(5U), 4U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(6U), 4U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(7U), 4U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(8U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(9U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(10U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(11U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(12U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(13U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(14U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(15U), 8U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(16U), 16U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(17U), 16U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(4095U), 2048U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(4096U), 4096U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(4097U), 4096U);
    CYBOZU_TEST_EQUAL(getMaxAlignedSize(0xffff), 0x8000U);
}


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

void doTest(uint64_t addr, uint32_t blks, uint32_t maxIoBlocks, const std::vector<uint32_t>& answer)
{
    const std::vector<uint32_t> v = splitIoToAligned(addr, blks, maxIoBlocks);
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
    doTest(0, 1, 0, {1});
    doTest(0, 2, 0, {2});
    doTest(0, 4, 0, {4});
    doTest(0, 8, 0, {8});
    doTest(0, 16, 0, {16});
    doTest(0, 32, 0, {32});
    doTest(1, 31, 0, {1, 2, 4, 8, 16});
    doTest(0, 31, 0, {16, 8, 4, 2, 1});

    doTest(0, 31, 8, {8, 8, 8, 4, 2, 1});
    doTest(0, 31, 4, {4, 4, 4, 4, 4, 4, 4, 2, 1});
    doTest(0, 16, 4, {4, 4, 4, 4});
    doTest(4, 32, 8, {4, 8, 8, 8, 4});
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
