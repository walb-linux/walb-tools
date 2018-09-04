#include "cybozu/test.hpp"
#include "bdev_reader.hpp"
#include "tmp_file.hpp"
#include "random.hpp"

using namespace walb;

using AArray = cybozu::AlignedArray<char, 512, false>;
const size_t LBS = 512;

void testUnlimited(const std::string& path,
          uint64_t offLb, size_t bufSize, size_t maxIoSize,
          const char *data, size_t size)
{
    const uint64_t off0 = offLb * LBS;
    assert(off0 < size);
    AsyncBdevReader reader(path, offLb, bufSize, maxIoSize);
    reader.setReadAheadUnlimited();
    AArray buf1(size);
    size_t off = off0;
    while (off < size) {
        const size_t s = std::min(1 + rand() % (maxIoSize * 2), size - off);
        reader.read(&buf1[off], s);
        off += s;
        //::printf("read size %zu\n", s);
    }

    CYBOZU_TEST_EQUAL(::memcmp(&data[off0], &buf1[off0], size - off0), 0);
}


void testLimited(const std::string& path,
                 size_t aheadLb, size_t bufSize, size_t maxIoSizes,
                 const char *data, size_t size)
{
    AsyncBdevReader reader(path, 0, bufSize, maxIoSizes);

    const size_t sizeLb = size / LBS;
    AArray buf1(size);

    for (size_t i = 0; i < 100; i++) {
        uint64_t offLb = rand() % sizeLb;
        size_t off = offLb * LBS;
        size_t readLb = std::min<size_t>(rand() % (aheadLb * 2 + 1) + 1, sizeLb - offLb);
        //::printf("sizesLb %" PRIu64 " offLb %" PRIu64 " readLb %zu\n", sizeLb, offLb, readLb);
        size_t readSize = readLb * LBS;
        reader.seek(offLb);
        reader.setReadAheadLimit(aheadLb * LBS);
        reader.read(&buf1[off], readSize);
        CYBOZU_TEST_EQUAL(::memcmp(&data[off], &buf1[off], readSize), 0);
        ::memset(&buf1[off], 0, readSize);
    }
}


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

    testUnlimited(tmpFile.path(), 0, bufSize, maxIoSize, buf0.data(), devSize);
    testUnlimited(tmpFile.path(), 1, bufSize, maxIoSize, buf0.data(), devSize);
    testUnlimited(tmpFile.path(), (4 << 20) / LBS, bufSize, maxIoSize, buf0.data(), devSize); /* 4MiB */
    for (size_t aheadLb : {0, 1, 2, 8}) {
        testLimited(tmpFile.path(), aheadLb, bufSize, maxIoSize, buf0.data(), devSize);
    }
}
