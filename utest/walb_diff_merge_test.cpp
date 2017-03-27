#include "cybozu/test.hpp"
#include "cybozu/array.hpp"
#include "walb_diff_merge.hpp"
#include "tmp_file.hpp"
#include "random.hpp"
#include "for_walb_diff_test.hpp"
#include <vector>

using namespace walb;
struct TmpDiffFile;
using TmpDiffFileVec = std::vector<TmpDiffFile>;

cybozu::util::Random<size_t> g_rand;

struct TmpDiffFile : cybozu::TmpFile
{
    TmpDiffFile() : TmpFile(".") {}
};

CYBOZU_TEST_AUTO(Setup)
{
#if 0
    g_rand.setSeed(395019343);
#endif
    ::printf("random number generator seed: %zu\n", g_rand.getSeed());
    setRandForTest(g_rand);
}

void verifyMergedDiff(size_t len, TmpDiffFileVec &d)
{
    TmpDisk disk0(len), disk1(len);

    for (size_t i = 0; i < d.size(); i++) {
        disk0.apply(d[i].path());
    }

    TmpDiffFile merged;
    DiffMerger merger(0);
    for (size_t i = 0; i < d.size(); i++) {
        merger.addWdiff(d[i].path());
    }
    merger.mergeToFd(merged.fd());
    disk1.apply(merged.path());

    disk0.verifyEquals(disk1);
}

void verifyDiffEquality(size_t len, TmpDiffFileVec &d0, TmpDiffFileVec &d1)
{
    CYBOZU_TEST_EQUAL(d0.size(), d1.size());
    const size_t nr = std::min(d0.size(), d1.size());
    for (size_t i = 0; i < nr; i++) {
        TmpDisk disk0(len), disk1(len);
        disk0.apply(d0[i].path());
        disk1.apply(d1[i].path());
        disk0.verifyEquals(disk1);
    }
}

struct MetaIo
{
    uint64_t addr;
    size_t len;
};

using Recipe = std::vector<std::vector<MetaIo> >;


using SioListVec = std::vector<SioList>;

SioListVec generateSioListVec(const Recipe &recipe)
{
    SioListVec ret;
    for (size_t i = 0; i < recipe.size(); i++) {
        ret.emplace_back();
        SioList &sioList = ret.back();
        for (const MetaIo &mio : recipe[i]) {
            sioList.emplace_back();
            sioList.back().setRandomly(mio.addr, mio.len);
        }
    }
    return ret;
}

int getCompressionTypeRandomly()
{
    size_t x = g_rand() % 100;
    if (x < 70) {
        return ::WALB_DIFF_CMPR_SNAPPY;
    } else if (x < 20) {
        return ::WALB_DIFF_CMPR_GZIP;
    } else {
        return ::WALB_DIFF_CMPR_LZMA;
    }
}

/**
 * sl must be sorted and overlap areas does not exist.
 */
void makeSortedWdiff1(TmpDiffFile &file, const SioList &sl)
{
    DiffWriter writer(file.fd());
    DiffFileHeader header;
    header.max_io_blocks = 0;
    for (const Sio &sio : sl) {
        header.max_io_blocks = std::max(header.max_io_blocks, sio.ioBlocks);
    }
    writer.writeHeader(header);
    for (const Sio &sio : sl) {
        DiffRecord rec;
        AlignedArray data;
        sio.copyTo(rec, data);

#if 0
        if (rec.isNormal()) rec.checksum = calcDiffIoChecksum(data);
        writer.writeDiff(rec, data.data());
#else
        writer.compressAndWriteDiff(rec, data.data(), getCompressionTypeRandomly());
#endif
    }
    writer.close();
}

void makeSortedWdiffs1(TmpDiffFileVec &tfv, const SioListVec &slv)
{
    CYBOZU_TEST_EQUAL(tfv.size(), slv.size());
    size_t nr = std::min(tfv.size(), slv.size());
    for (size_t i = 0; i < nr; i++) {
        makeSortedWdiff1(tfv[i], slv[i]);
    }
}

/**
 * Any sl is allowed.
 */
void makeSortedWdiff2(TmpDiffFile &file, const SioList &sl)
{
    DiffMemory mem;

    for (const Sio &sio : sl) {
        DiffRecord rec;
        DiffIo io;
        sio.copyTo(rec, io.data);
        io.set(rec);
        mem.add(rec, std::move(io));
    }
    mem.writeTo(file.fd());
}

void makeSortedWdiffs2(TmpDiffFileVec &tfv, const SioListVec &slv)
{
    CYBOZU_TEST_EQUAL(tfv.size(), slv.size());
    size_t nr = std::min(tfv.size(), slv.size());
    for (size_t i = 0; i < nr; i++) {
        makeSortedWdiff2(tfv[i], slv[i]);
    }
}

void makeIndexedWdiff(TmpDiffFile &file, const SioList &sl)
{
    IndexedDiffWriter writer;
    writer.setFd(file.fd());
    DiffFileHeader header;
    writer.writeHeader(header);
    for (const Sio &sio : sl) {
        IndexedDiffRecord rec;
        AlignedArray data;
        sio.copyTo(rec, data);
#if 0
        if (rec.isNormal()) rec.io_checksum = calcDiffIoChecksum(data);
        writer.writeDiff(rec, data.data());
#else
        writer.compressAndWriteDiff(rec, data.data(), getCompressionTypeRandomly());
#endif
    }
    writer.finalize();
}

void makeIndexedWdiffs(TmpDiffFileVec &tfv, const SioListVec &slv)
{
    CYBOZU_TEST_EQUAL(tfv.size(), slv.size());
    size_t nr = std::min(tfv.size(), slv.size());
    for (size_t i = 0; i < nr; i++) {
        makeIndexedWdiff(tfv[i], slv[i]);
    }
}

void testMerge1(size_t len, const Recipe &recipe)
{
    SioListVec slv = generateSioListVec(std::move(recipe));
    size_t nr = recipe.size();
    TmpDiffFileVec d0(nr), d1(nr);
    makeSortedWdiffs1(d0, slv);
    makeIndexedWdiffs(d1, slv);
    verifyMergedDiff(len, d0);
    verifyMergedDiff(len, d1);
    verifyDiffEquality(len, d0, d1);
}

CYBOZU_TEST_AUTO(wdiffMerge)
{
    /*
     * addr   01234567890123456789
     * diff2  XXX XXX XXX
     * diff1  XXX XXX XXX
     * diff0  XXX XXX XXX
     */
    {
        testMerge1(20,  {
                {{0, 3}, {4, 3}, {8, 3}},
                {{0, 3}, {4, 3}, {8, 3}},
                {{0, 3}, {4, 3}, {8, 3}}});
    }

    /*
     * addr   01234567890123456789
     * diff2  ZZZZ
     * diff1    YYYY
     * diff0      XXXX
     */
    {
        testMerge1(10, {{{4, 4}}, {{2, 4}}, {{0, 4}}});
    }

    /*
     * addr   01234567890123456789
     * diff3  DDDD
     * diff2    ZZZZ
     * diff1        BBBB
     * diff0          AAAA
     */
    {
        testMerge1(20, {{{8, 4}}, {{6, 4}}, {{2, 4}}, {{0, 4}}});
    }

    /**
     * addr   01234567890123456789
     * diff2  CCCCCCCCCC
     * diff1    BBBB
     * diff0      AAAA
     */
    {
        testMerge1(20, {{{4, 4}}, {{2, 4}}, {{0, 10}}});
    }

    /**
     * addr   01234567890123456789
     * diff2  FFFFFFFFFFFFFFFF
     * diff1    DDDD EEEE
     * diff0      AAAA BBBB CCCC
     */
    {
        testMerge1(20, {
                {{4, 4}, {9, 4}, {14, 4}},
                {{2, 4}, {7, 4}},
                {{0, 16}}});
    }

    /**
     * addr   012345678901234567890123456789
     * diff5  XXXX
     * diff4    XXXX
     * diff2      XXXXXXXXXXXXXX
     * diff1        XXXX XXXX
     * diff0          XXXX XXXX XXXX
     */
    {
        testMerge1(30, {
                {{8, 4}, {13, 4}, {18, 4}},
                {{6, 4}, {11, 4}},
                {{4, 14}},
                {{2, 4}},
                {{0, 4}}
            });
    }

    /**
     * addr   01234567890123456789
     * diff2      XXX XXX XXX XXX
     * diff1    XXX XXX XXX XXX
     * diff0  XXX XXX XXX XXX
     */
    {
        testMerge1(20, {
                {{0, 3}, {4, 3}, {8, 3}, {12, 3}},
                {{2, 3}, {6, 3}, {10, 3}, {14, 3}},
                {{4, 3}, {8, 3}, {12, 3}, {16, 3}}});
    }

    /**
     * addr   01234567890123456789
     * diff2      XXX XXX XXX XXX
     * diff1    XXX XXX XXX XXX
     * diff0  XXX XXX XXX XXX
     */
    {
        testMerge1(20, {
                {{0, 3}, {4, 3}, {8, 3}, {12, 3}},
                {{2, 3}, {6, 3}, {10, 3}, {14, 3}},
                {{4, 3}, {8, 3}, {12, 3}, {16, 3}}});
    }

    /**
     * addr   01234567890123456789
     * diff5  XXX
     * diff4    XXX
     * diff3              XXX
     * diff2      XXX
     * diff1        XXX
     * diff0              XXX
     */
    {
        testMerge1(20, {
                {{12, 3}},
                {{6, 3}},
                {{4, 3}},
                {{12, 3}},
                {{2, 3}},
                {{0, 3}}
            });
    }

    /**
     * addr   0123456789
     * diff5  XXX
     * diff4    XXX
     * diff3  XXX
     * diff2    XXX
     * diff1  XXX
     * diff0    XXX
     */
    {
        testMerge1(10, {
                {{2, 3}},
                {{0, 3}},
                {{2, 3}},
                {{0, 3}},
                {{2, 3}},
                {{0, 3}}
            });
    }
}

void testMerge2(size_t len, const Recipe &recipe)
{
    SioListVec slv = generateSioListVec(recipe);
    size_t nr = recipe.size();
    TmpDiffFileVec d0(nr), d1(nr);
    makeSortedWdiffs2(d0, slv);
    makeIndexedWdiffs(d1, slv);
    verifyMergedDiff(len, d0);
    verifyMergedDiff(len, d1);
    verifyDiffEquality(len, d0, d1);
}

CYBOZU_TEST_AUTO(wdiffMergeRandom)
{
    const size_t len = 512;
    const size_t ioNr = 32;
    const size_t diffNr = 8;
    for (size_t i = 0; i < 10; i++) {
        Recipe recipe;
        for (size_t j = 0; j < diffNr; j++) {
            recipe.emplace_back();
            for (size_t k = 0; k < ioNr; k++) {
                const uint64_t ioAddr = g_rand() % len;
                const uint32_t ioBlocks = std::min(g_rand() % 16 + 1, len - ioAddr);
                recipe.back().push_back({ioAddr, ioBlocks});
            }
        }
        testMerge2(len, recipe);
    }
}
