#include "cybozu/test.hpp"
#include "walb_diff_mem.hpp"
#include "for_walb_diff_test.hpp"

using namespace walb;

cybozu::util::Random<size_t> g_rand;

CYBOZU_TEST_AUTO(Setup)
{
#if 0
    g_rand.setSeed(2742957103);
#endif
    ::printf("random number generator seed: %zu\n", g_rand.getSeed());
    setRandForTest(g_rand);
}

using Recipe = std::vector<std::pair<uint64_t, uint32_t> >;

void testDiffMemory(size_t diskLen, const Recipe& recipe)
{
    DiffMemory diffM;

    std::list<Sio> sioList0;
    for (const std::pair<uint64_t, uint32_t>& pair : recipe) {
        Sio sio;
        sio.setRandomly(pair.first, pair.second, DiffRecType::NORMAL);
        sioList0.push_back(sio);
    }
    TmpDisk disk0(diskLen), disk1(diskLen);
    disk0.apply(sioList0);

    for (const Sio& sio : sioList0) {
        DiffRecord rec;
        DiffIo io;
        AlignedArray data;
        sio.copyTo(rec, data);
        io.set(rec, std::move(data));
        diffM.add(rec, std::move(io));
    }
    diffM.checkNoOverlappedAndSorted();

    std::list<Sio> sioList1;
    {
        for (const DiffMemory::Map::value_type& pair : diffM.getMap()) {
            const DiffRecord &rec = pair.second.record();
            const DiffIo &io = pair.second.io();
            Sio sio;
            sio.copyFrom(rec, io.data);
            mergeOrAddSioList(sioList1, std::move(sio));
        }
    }
    disk1.apply(sioList1);
    disk0.verifyEquals(disk1);
}

CYBOZU_TEST_AUTO(SimpleDiff)
{
    /*
     * in0         11111111
     * in1     22222222
     * in2 33333333
     * out 3333333322221111
     */
    testDiffMemory(16, {{8, 8}, {4, 8}, {0, 8}});

    /*
     * in0 11111111
     * in1     22222222
     * in2         33333333
     * out 1111222233333333
     */
    testDiffMemory(16, {{0, 8}, {4, 8}, {8, 8}});

    /*
     * Random test.
     */
    for (size_t i = 0; i < 16; i++) {
        Recipe recipe;
        size_t diskLen = 128;
        size_t nrIo = 32;
        size_t maxIoLb = 32;
        for (size_t j = 0; j < nrIo; j++) {
            uint64_t addr = g_rand() % diskLen;
            uint32_t blks = g_rand() % std::min(maxIoLb, diskLen - addr) + 1;
            recipe.push_back({addr, blks});
        }
        testDiffMemory(diskLen, recipe);
    }
}
