#include "cybozu/test.hpp"
#include "util.hpp"

template <typename IntType>
void checkHex(IntType x0)
{
    IntType x1;
    std::string s = cybozu::util::intToHexStr(x0);
    cybozu::util::hexStrToInt(s, x1);
    CYBOZU_TEST_EQUAL(x0, x1);
}

CYBOZU_TEST_AUTO(hex)
{
    for (int i = 0; i < 512; i++) {
        checkHex(i);
    }
    uint32_t x32 = 0;
    checkHex(x32);
    checkHex(~x32);
    uint64_t x64 = 0;
    checkHex(x64);
    checkHex(~x64);
    cybozu::util::Rand<uint64_t> rand;
    for (int i = 0; i < 100; i++) {
        checkHex(rand.get());
    }
}
