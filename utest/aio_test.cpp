#include "cybozu/test.hpp"
#include "aio_util.hpp"
#include "random.hpp"
#include "tmp_file.hpp"
#include "fileio.hpp"
#include "cybozu/array.hpp"
#include <thread>
#include <set>

using namespace cybozu::aio;
using AArray = cybozu::AlignedArray<char, 512>;
const size_t LBS = 512;

cybozu::util::Random<size_t> randx;

cybozu::TmpFile prepareTmpFile(size_t blks)
{
    cybozu::TmpFile tmpF(".", O_DIRECT);
    cybozu::util::File f(tmpF.fd());
    AArray v0(LBS * blks);
    f.write(v0.data(), v0.size());
    return tmpF;
}

void fillArray(AArray& v)
{
    const size_t blks = v.size() / LBS;
    for (size_t i = 0; i < blks; i++) {
        ::memcpy(&v[LBS * i], &i, sizeof(i));
    }
}

void writeArray(Aio& aio, const AArray& v)
{
    const size_t blks = v.size() / LBS;
    std::list<uint32_t> keys;
    size_t i = 0;
    while (!aio.isQueueFull()) {
        const off_t oft = LBS * i;
        const uint32_t key = aio.prepareWrite(oft, LBS, &v[oft]);
        CYBOZU_TEST_ASSERT(key != 0);
        aio.submit();
        keys.push_back(key);
        i++;
    }
    while (i < blks) {
        aio.waitFor(keys.front());
        keys.pop_front();
        const off_t oft = LBS * i;
        const uint32_t key = aio.prepareWrite(oft, LBS, &v[oft]);
        CYBOZU_TEST_ASSERT(key != 0);
        aio.submit();
        keys.push_back(key);
        i++;
    }
    while (!keys.empty()) {
        aio.waitFor(keys.front());
        keys.pop_front();
    }
}

void readArray(Aio& aio, AArray& v)
{
    const size_t blks = v.size() / LBS;
    std::list<uint32_t> keys;
    size_t i = 0;
    while (!aio.isQueueFull()) {
        const off_t oft = LBS * i;
        const uint32_t key = aio.prepareRead(oft, LBS, &v[oft]);
        CYBOZU_TEST_ASSERT(key != 0);
        aio.submit();
        keys.push_back(key);
        i++;
    }
    while (i < blks) {
        aio.waitFor(keys.front());
        keys.pop_front();
        const off_t oft = LBS * i;
        const uint32_t key = aio.prepareRead(oft, LBS, &v[oft]);
        aio.submit();
        keys.push_back(key);
        i++;
    }
    while (!keys.empty()) {
        aio.waitFor(keys.front());
        keys.pop_front();
    }
}

CYBOZU_TEST_AUTO(testAioSimple)
{
    cybozu::TmpFile tmpF = prepareTmpFile(128);
    Aio aio(tmpF.fd(), 8);

    /* Write */
    AArray v0(LBS * 128);
    fillArray(v0);
    writeArray(aio, v0);

    /* Read */
    AArray v1(LBS * 128);
    readArray(aio, v1);

    /* Verify */
    CYBOZU_TEST_EQUAL(::memcmp(v0.data(), v1.data(), v0.size()), 0);
}

CYBOZU_TEST_AUTO(testAioIsCompleted)
{
    const size_t QSIZE = 8;
    cybozu::TmpFile tmpF = prepareTmpFile(128);
    AArray v0(LBS * 128);
    AArray v1(LBS * 128);
    fillArray(v0);

    Aio aio(tmpF.fd(), QSIZE);
    writeArray(aio, v0);

    std::list<uint32_t> keys;
    std::set<size_t> blkS;
    for (size_t i = 0; i < QSIZE; i++) {
        const size_t blk = randx() % 128;
        blkS.insert(blk);
        const size_t off = blk * LBS;
        const uint32_t key = aio.prepareRead(off, LBS, &v1[off]);
        keys.push_back(key);
    }
    aio.submit();

    while (!keys.empty()) {
        const uint32_t key = keys.front();
        while (!aio.isCompleted(key)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        aio.waitFor(key);
        keys.pop_front();
    }

    // Verify
    for (size_t blk : blkS) {
        const size_t off = blk * LBS;
        CYBOZU_TEST_EQUAL(::memcmp(&v0[off], &v1[off], LBS), 0);
    }
}

CYBOZU_TEST_AUTO(testAioWait)
{
    const size_t QSIZE = 8;
    cybozu::TmpFile tmpF = prepareTmpFile(128);
    Aio aio(tmpF.fd(), QSIZE);

    AArray v0(LBS * 128);
    fillArray(v0);
    writeArray(aio, v0);

    AArray v1(LBS * 128);
    std::list<uint32_t> keys;
    std::set<size_t> blkS;
    for (size_t i = 0; i < QSIZE; i++) {
        const size_t blk = randx() % 128;
        const size_t off = blk * LBS;
        const uint32_t key = aio.prepareRead(off, LBS, &v1[off]);
        aio.submit();
        keys.push_back(key);
        blkS.insert(blk);
    }

    std::queue<uint32_t> q;
    aio.wait(keys.size(), q);
    CYBOZU_TEST_EQUAL(q.size(), keys.size());

    std::vector<uint32_t> s0, s1;
    while (!q.empty()) {
        s0.push_back(q.front());
        q.pop();
    }
    s1.assign(keys.begin(), keys.end());
    std::sort(s0.begin(), s0.end());
    std::sort(s1.begin(), s1.end());
    CYBOZU_TEST_ASSERT(s0 == s1);
}

CYBOZU_TEST_AUTO(testAioCancel)
{
    const size_t QSIZE = 8;
    cybozu::TmpFile tmpF = prepareTmpFile(128);
    Aio aio(tmpF.fd(), QSIZE);

    AArray v0(LBS * 128);
    fillArray(v0);
    writeArray(aio, v0);

    AArray v1(LBS * 128);
    std::list<uint32_t> keys;
    std::set<size_t> blkS;
    for (size_t i = 0; i < QSIZE; i++) {
        const size_t blk = randx() % 128;
        const size_t off = blk * LBS;
        const uint32_t key = aio.prepareRead(off, LBS, &v1[off]);
        if (randx() % 2 == 0) {
            aio.submit();
        }
        if (!aio.cancel(key)) {
            keys.push_back(key);
            blkS.insert(blk);
        }
    }

    std::queue<uint32_t> q;
    aio.wait(keys.size(), q);
    CYBOZU_TEST_EQUAL(q.size(), keys.size());

    std::vector<uint32_t> s0, s1;
    while (!q.empty()) {
        s0.push_back(q.front());
        q.pop();
    }
    s1.assign(keys.begin(), keys.end());
    std::sort(s0.begin(), s0.end());
    std::sort(s1.begin(), s1.end());
    CYBOZU_TEST_ASSERT(s0 == s1);
}

CYBOZU_TEST_AUTO(testAioWaitOne)
{
    cybozu::TmpFile tmpF = prepareTmpFile(128);
    Aio aio(tmpF.fd(), 8);

    AArray v0(LBS * 128);
    fillArray(v0);
    writeArray(aio, v0);

    AArray v1(LBS * 128);

    std::list<uint32_t> keys0, keys1;
    for (size_t i = 0; i < 1024; i++) {
        while (aio.isQueueFull()) {
            keys1.push_back(aio.waitOne());
        }
        const size_t blk = randx() % 128;
        const size_t off = blk * LBS;
        const uint32_t key = aio.prepareRead(off, LBS, &v1[off]);
        CYBOZU_TEST_ASSERT(key != 0);
        keys0.push_back(key);
        aio.submit();
    }
    while (!aio.empty()) keys1.push_back(aio.waitOne());
    if (keys0.size() != keys1.size()) {
        for (const uint32_t key : keys0) {
            ::printf("%u ", key);
        }
        ::printf("\n");
        for (const uint32_t key : keys1) {
            ::printf("%u ", key);
        }
        ::printf("\n");
    }
    CYBOZU_TEST_EQUAL(keys0.size(), keys1.size());

    std::vector<uint32_t> s0, s1;
    s0.assign(keys0.begin(), keys0.end());
    s1.assign(keys1.begin(), keys1.end());
    std::sort(s0.begin(), s0.end());
    std::sort(s1.begin(), s1.end());
    CYBOZU_TEST_ASSERT(s0 == s1);
}
