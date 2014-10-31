#include "cybozu/test.hpp"
#include "bdev_io.hpp"
#include "tmp_file.hpp"
#include "random.hpp"

using namespace walb;

using AArray = cybozu::AlignedArray<char, 512>;
const size_t LBS = 512;


CYBOZU_TEST_AUTO(testAsyncBdevReader)
{
    cybozu::util::Random<size_t> rand;
    const size_t devSize = (8 << 20) + rand() % 32 * LBS; /* 8MiB+ */
    const size_t bufSize = (1 << 20) + rand() % 32 * LBS; /* 1MiB+ */
    const size_t maxIoSize = (64 << 10) + rand() % 32 * LBS; /* 64KiB+ */

    CYBOZU_TEST_ASSERT(devSize % LBS == 0);
    AArray buf0(devSize);
    rand.fill(buf0.data(), buf0.size());

    cybozu::TmpFile tmpFile(".");
    {
        cybozu::util::File f(tmpFile.fd());
        f.write(buf0.data(), buf0.size());
        f.fdatasync();
    }

    AsyncBdevReader reader(tmpFile.path(), bufSize, maxIoSize);
    AArray buf1(devSize);
    size_t off = 0;
    while (off < devSize) {
        const size_t s = std::min(1 + rand() % (maxIoSize * 2), devSize - off);
        reader.read(&buf1[off], s);
        off += s;
        //::printf("read size %zu\n", s);
    }

    CYBOZU_TEST_EQUAL(::memcmp(buf0.data(), buf1.data(), devSize), 0);
}
