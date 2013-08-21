#include "compressor.hpp"
#include <cybozu/test.hpp>

void test(walb::Compressor::Mode mode)
{
    const std::string in = "aaaabbbbccccddddeeeeffffgggghhhhiiiijjjjjaaaaaaaaaaaaabbbcccxxxxxxxxxxxxxxxxxsssssssssssssssssssssssssssssssss";
    const size_t maxInSize = in.size();
    walb::Compressor c(mode, maxInSize);
    std::string enc;
    enc.resize(c.getMaxOutSize());
    size_t encSize = c.run(&enc[0], &in[0], in.size());
    printf("inSize=%d, encSize=%d, maxOutSize=%d\n", (int)in.size(), (int)encSize, (int)c.getMaxOutSize());
    std::string dec;
    walb::Uncompressor d(mode);
    dec.resize(maxInSize);
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
