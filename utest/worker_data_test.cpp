#include <cstdio>
#include <unistd.h>
#include "cybozu/test.hpp"
#include "worker_data.hpp"
#include "for_test.hpp"
#include "file_path.hpp"

const uint64_t SIZE_PB = (128ULL << 20) >> 9;

CYBOZU_TEST_AUTO(data)
{
    std::string dirName("worker_data0");
    cybozu::FilePath fp = cybozu::FilePath(dirName).toFullPath();
    ::printf("%s\n", fp.cStr());
    TestDirectory testDir(dirName);
    walb::WorkerData wData(fp.str(), "0", SIZE_PB);
    wData.init(100);
    CYBOZU_TEST_ASSERT(wData.empty());
    std::vector<std::pair<uint64_t, uint64_t> > v0;
    v0.push_back(wData.takeSnapshot(200, true));
    v0.push_back(wData.takeSnapshot(300, true));
    v0.push_back(wData.takeSnapshot(300, true));
    v0.push_back(wData.takeSnapshot(400, true));
    v0.push_back(wData.takeSnapshot(400, true));
    v0.push_back(wData.takeSnapshot(500, true));
    v0.push_back(wData.takeSnapshot(500, false));
    v0.push_back(wData.takeSnapshot(600, false));
    v0.push_back(wData.takeSnapshot(700, true));
    v0.push_back(wData.takeSnapshot(800, false));
    v0.push_back(wData.takeSnapshot(900, true));
    v0.push_back(wData.takeSnapshot(SIZE_PB * 5, true));
    CYBOZU_TEST_EQUAL(v0.back().second - v0.back().first, 5);

    std::vector<walb::MetaSnap> v1 = wData.getAllRecords();
    CYBOZU_TEST_EQUAL(v1.size(), 12);
#if 0
    for (walb::MetaSnap &snap : v1) {
        snap.print();
    }
#endif

    walb::MetaSnap rec0 = wData.front();
    CYBOZU_TEST_EQUAL(rec0.gid1() - rec0.gid0(), 1);
    CYBOZU_TEST_EQUAL(rec0.raw().lsid, 200);
    wData.pop();
    CYBOZU_TEST_EQUAL(wData.getAllRecords().size(), 11);

    walb::MetaSnap rec1 = wData.front();
    CYBOZU_TEST_EQUAL(rec1.gid1() - rec1.gid0(), 1);
    CYBOZU_TEST_EQUAL(rec1.raw().lsid, 300);
    wData.pop();
    CYBOZU_TEST_EQUAL(wData.getAllRecords().size(), 10);

    CYBOZU_TEST_ASSERT(!wData.empty());

    //wData.removeBefore();
}
