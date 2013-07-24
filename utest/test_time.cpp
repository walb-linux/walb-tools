#include <cstdio>
#include <string>
#include <time.h>
#include "cybozu/test.hpp"
#include "time.hpp"

CYBOZU_TEST_AUTO(test)
{
    {
        time_t t0 = ::time(nullptr);
        std::string s0 = cybozu::unixTimeToStr(t0);
        ::printf("%s\n", s0.c_str());
        time_t t1 = cybozu::strToUnixTime(s0);
        CYBOZU_TEST_EQUAL(t0, t1);
    }
    {
        std::string s0("20130724193601");
        time_t t0 = cybozu::strToUnixTime(s0);
        std::string s1 = cybozu::unixTimeToStr(t0);
        CYBOZU_TEST_EQUAL(s0, s1);
    }
}
