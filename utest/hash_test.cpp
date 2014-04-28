#include "cybozu/test.hpp"
#include "murmurhash3.hpp"
#include <vector>
#include <cstdio>
#include <sstream>

CYBOZU_TEST_AUTO(hash)
{
    std::vector<char> v(1024);

    cybozu::murmurhash3::Hasher hasher(0);
    cybozu::murmurhash3::Hash h0 = hasher(&v[0], v.size());
    std::string s0 = h0.str();
    ::printf("%zu [%s]\n", s0.size(), s0.c_str());

    std::stringstream ss0;
    cybozu::save(ss0, h0);
    std::stringstream ss1(ss0.str());
    cybozu::murmurhash3::Hash h1;
    cybozu::load(h1, ss1);
    std::string s1 = h1.str();
    ::printf("%zu [%s]\n", s1.size(), s1.c_str());

    CYBOZU_TEST_EQUAL(h0, h1);
    CYBOZU_TEST_EQUAL(s0, s1);
}
