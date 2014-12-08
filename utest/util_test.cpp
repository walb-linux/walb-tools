#include "cybozu/test.hpp"
#include "random.hpp"
#include "util.hpp"
#include "walb_types.hpp"

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
    cybozu::util::Random<uint64_t> rand;
    for (int i = 0; i < 100; i++) {
        checkHex(rand());
    }
}

char randomChar(cybozu::util::Random<uint32_t> &rand)
{
    while (true) {
        char c = rand() % 128;
        if ('0' <= c && c <= '9') return c;
        if ('a' <= c && c <= 'z') return c;
        if ('A' <= c && c <= 'Z') return c;
        if (c == '-' || c == '_') return c;
    }
}

std::string randomString(cybozu::util::Random<uint32_t> &rand, size_t min, size_t max)
{
    assert(min <= max);
    size_t plus = 0;
    if (min < max) plus = rand() % (max - min);
    std::string s;
    for (size_t i = 0; i < min + plus; i++) {
        s.push_back(randomChar(rand));
    }
    return s;
}

void randomStringSplitTest(cybozu::util::Random<uint32_t> &rand)
{
    walb::StrVec v0, v1;
    std::string s1 = randomString(rand, 0, 5);
    v1.push_back(s1);
    size_t len = rand() % 10;
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
    walb::StrVec v0, v1;
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

    cybozu::util::Random<uint32_t> rand;
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

CYBOZU_TEST_AUTO(removeEmptyItem)
{
    std::string s = "0  1   2 ";
    walb::StrVec v = cybozu::util::splitString(s, " ");
    CYBOZU_TEST_EQUAL(v.size(), 7);
    CYBOZU_TEST_EQUAL(v[0], "0");
    CYBOZU_TEST_EQUAL(v[1], "");
    CYBOZU_TEST_EQUAL(v[2], "1");
    CYBOZU_TEST_EQUAL(v[3], "");
    CYBOZU_TEST_EQUAL(v[4], "");
    CYBOZU_TEST_EQUAL(v[5], "2");
    CYBOZU_TEST_EQUAL(v[6], "");

    cybozu::util::removeEmptyItemFromVec(v);
    CYBOZU_TEST_EQUAL(v.size(), 3);
    CYBOZU_TEST_EQUAL(v[0], "0");
    CYBOZU_TEST_EQUAL(v[1], "1");
    CYBOZU_TEST_EQUAL(v[2], "2");
}

CYBOZU_TEST_AUTO(UnitIntString)
{
    using namespace cybozu::util;
    auto check = [](const std::string &s, uint64_t v) {
        CYBOZU_TEST_EQUAL(fromUnitIntString(s), v);
        CYBOZU_TEST_EQUAL(toUnitIntString(v), s);
    };
    check("12345", 12345);
    check("1k", 1ULL << 10);
    check("2m", 2ULL << 20);
    check("3g", 3ULL << 30);
    check("4t", 4ULL << 40);
    check("5p", 5ULL << 50);
    check("6e", 6ULL << 60);

    /* Overflow check. */
    CYBOZU_TEST_EQUAL(fromUnitIntString("15e"), uint64_t(15) << 60);
    CYBOZU_TEST_EXCEPTION(fromUnitIntString("16e"), std::exception);
    CYBOZU_TEST_EQUAL(fromUnitIntString("16383p"), uint64_t(16383) << 50);
    CYBOZU_TEST_EXCEPTION(fromUnitIntString("16384p"), std::exception);
}

CYBOZU_TEST_AUTO(formatString)
{
    {
        std::string st(cybozu::util::formatString("%s%c%s", "012", (char)0, "345"));
#if 0
        for (size_t i = 0; i < st.size(); i++) {
            printf("%0x ", st[i]);
        }
        ::printf("\n size %zu\n", st.size());
#endif
        CYBOZU_TEST_EQUAL(st.size(), 7);
    }

    {
        std::string st(cybozu::util::formatString(""));
        CYBOZU_TEST_EQUAL(st, "");
    }

    {
        try {
            std::string st(cybozu::util::formatString(nullptr));
            CYBOZU_TEST_ASSERT(false);
        } catch (std::runtime_error& e) {
        }
    }

    {
        std::string st(cybozu::util::formatString("%s%s", "0123456789", "0123456789"));
        CYBOZU_TEST_EQUAL(st.size(), 20);
    }
}

CYBOZU_TEST_AUTO(isAllZero)
{
    const size_t s = 1024;
    std::vector<char> v(s);
    for (size_t i = 0; i < 32; i++) {
        CYBOZU_TEST_ASSERT(cybozu::util::isAllZero(&v[i], s - i));
    }
    v[s - 1] = 1;
    for (size_t i = 0; i < 32; i++) {
        CYBOZU_TEST_ASSERT(!cybozu::util::isAllZero(&v[i], s - i));
    }
}
