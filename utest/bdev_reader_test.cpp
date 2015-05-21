#include "cybozu/test.hpp"
#include "bdev_reader.hpp"
#include "tmp_file.hpp"
#include "random.hpp"

using namespace walb;

using AArray = cybozu::AlignedArray<char, 512, false>;
const size_t LBS = 512;

void test(const std::string& path,
          uint64_t offLb, size_t bufSize, size_t maxIoSize,
          const char *data, size_t size)
{
    const uint64_t off0 = offLb * LBS;
    assert(off0 < size);
    AsyncBdevReader reader(path, offLb, bufSize, maxIoSize);
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

    test(tmpFile.path(), 0, bufSize, maxIoSize, buf0.data(), devSize);
    test(tmpFile.path(), 1, bufSize, maxIoSize, buf0.data(), devSize);
    test(tmpFile.path(), (4 << 20) / LBS, bufSize, maxIoSize, buf0.data(), devSize); /* 4MiB */
}
