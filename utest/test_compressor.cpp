#include "compressor.hpp"
#include <cybozu/test.hpp>

void test(walb::Compressor::Mode mode)
{
    const std::string in = "aaaabbbbccccddddeeeeffffgggghhhhiiiijjjjjaaaaaaaaaaaaabbbcccxxxxxxxxxxxxxxxxxsssssssssssssssssssssssssssssssss";
    walb::Compressor c(mode);
    std::string enc;
    enc.resize(in.size());
    size_t encSize = c.run(&enc[0], enc.size(), &in[0], in.size());
    printf("inSize=%d, encSize=%d\n", (int)in.size(), (int)encSize);
    std::string dec;
    walb::Uncompressor d(mode);
    dec.resize(in.size() + 10);
    size_t decSize = d.run(&dec[0], dec.size(), &enc[0], encSize);
    CYBOZU_TEST_EQUAL(decSize, in.size());
    dec.resize(decSize);
    CYBOZU_TEST_EQUAL(dec, in);
}

CYBOZU_TEST_AUTO(test)
{
    test(walb::Compressor::AsIs);
    test(walb::Compressor::Snappy);
    test(walb::Compressor::Zlib);
    test(walb::Compressor::Xz);
}
