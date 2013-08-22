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

char randomChar(cybozu::util::Rand<uint32_t> &rand)
{
    while (true) {
        char c = rand.get() % 128;
        if ('0' <= c && c <= '9') return c;
        if ('a' <= c && c <= 'z') return c;
        if ('A' <= c && c <= 'Z') return c;
        if (c == '-' || c == '_') return c;
    }
}

std::string randomString(cybozu::util::Rand<uint32_t> &rand, size_t min, size_t max)
{
    assert(min <= max);
    size_t plus = 0;
    if (min < max) plus = rand.get() % (max - min);
    std::string s;
    for (size_t i = 0; i < min + plus; i++) {
        s.push_back(randomChar(rand));
    }
    return std::move(s);
}

void randomStringSplitTest(cybozu::util::Rand<uint32_t> &rand)
{
    std::vector<std::string> v0, v1;
    std::string s1 = randomString(rand, 0, 5);
    v1.push_back(s1);
    size_t len = rand.get() % 10;
    for (size_t i = 0; i < len; i++) {
        s1.push_back(',');
        std::string s2 = randomString(rand, 0, 5);
        s1 += s2;
        v1.push_back(s2);
    }
    v0 = cybozu::util::splitString(s1, ",");
    if (v0 != v1) {
        cybozu::util::printList(v0);
        cybozu::util::printList(v1);
    }
    CYBOZU_TEST_ASSERT(v0 == v1);
}

CYBOZU_TEST_AUTO(string)
{
    std::string s0 = "0,1.,3.4,";
    std::vector<std::string> v0, v1;
    v0 = cybozu::util::splitString(s0, ",.");
    CYBOZU_TEST_EQUAL(v0.size(), 6);
    CYBOZU_TEST_EQUAL(v0[0], "0");
    CYBOZU_TEST_EQUAL(v0[1], "1");
    CYBOZU_TEST_EQUAL(v0[2], "");
    CYBOZU_TEST_EQUAL(v0[3], "3");
    CYBOZU_TEST_EQUAL(v0[4], "4");
    CYBOZU_TEST_EQUAL(v0[5], "");

    v0 = cybozu::util::splitString("", ".");
    v1 = {""};
    CYBOZU_TEST_ASSERT(v0 == v1);

    cybozu::util::Rand<uint32_t> rand;
    for (int i = 0; i < 100; i++) {
        randomStringSplitTest(rand);
    }
}

CYBOZU_TEST_AUTO(trim)
{
    CYBOZU_TEST_EQUAL(cybozu::util::trimSpace(""), "");
    CYBOZU_TEST_EQUAL(cybozu::util::trimSpace("      "), "");
    CYBOZU_TEST_EQUAL(cybozu::util::trimSpace(" abc  "), "abc");
    CYBOZU_TEST_EQUAL(cybozu::util::trimSpace(" abc"), "abc");
    CYBOZU_TEST_EQUAL(cybozu::util::trimSpace("abc   "), "abc");
}
