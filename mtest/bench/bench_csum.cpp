#include "checksum.hpp"
#include "random.hpp"
#include "constant.hpp"
#include "walb_types.hpp"
#include <cstdio>
#include <cinttypes>

using namespace walb;

uint64_t rdtscp()
{
    uint32_t a, d;
    __asm__ volatile ("rdtscp" : "=a" (a), "=d" (d) :: "ecx");
    return uint64_t(a) | (uint64_t(d) << 32);
}

int main()
{
    cybozu::util::Xoroshiro128Plus rand(::time(0));
    uint64_t t0, t1;
    AlignedArray buf;
    buf.resize(32 * KIBI);
    rand.fill(buf.data(), buf.size());

    for (size_t i = 0; i < 10; i++) {
        t0 = rdtscp();
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
        t1 = rdtscp();
        ::printf("%" PRIu64 " %08x\n", t1 - t0, csum);
    }
}
