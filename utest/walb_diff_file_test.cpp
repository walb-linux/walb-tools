#include "cybozu/test.hpp"
#include "walb_diff_file.hpp"
#include "tmp_file.hpp"
#include "random.hpp"
#include "for_walb_diff_test.hpp"
#include <sstream>

using namespace walb;

cybozu::util::Random<size_t> g_rand;

/**
 * If you want to fix the seed, specify it to g_rand.setSeed().
 */
CYBOZU_TEST_AUTO(Setup)
{
#if 0
    g_rand.setSeed(395019343);
#endif
    ::printf("random number generator seed: %zu\n", g_rand.getSeed());
    setRandForTest(g_rand);
}

CYBOZU_TEST_AUTO(NullSortedDiffFile)
{
    cybozu::TmpFile tmpFile(".");

    SortedDiffWriter writer(tmpFile.fd());
    DiffFileHeader header0, header1;
    writer.writeHeader(header0);
    writer.close();

    cybozu::util::File(tmpFile.fd()).lseek(0);

    SortedDiffReader reader(tmpFile.fd());
    reader.readHeader(header1);
    DiffRecord rec;
    AlignedArray data;
    CYBOZU_TEST_ASSERT(!reader.readDiff(rec, data));
}

CYBOZU_TEST_AUTO(NullIndexedDiffFile)
{
    cybozu::TmpFile tmpFile(".");

    IndexedDiffWriter writer;
    writer.setFd(tmpFile.fd());
    DiffFileHeader header0;
    writer.writeHeader(header0);
    writer.finalize();

    cybozu::util::File(tmpFile.fd()).lseek(0);

    IndexedDiffReader reader;
    IndexedDiffCache cache;
    reader.setFile(cybozu::util::File(tmpFile.fd()), cache);
    IndexedDiffRecord rec;
    AlignedArray data;
    CYBOZU_TEST_ASSERT(!reader.readDiff(rec, data));
}

IndexedDiffRecord makeIrec(uint64_t ioAddr, uint32_t ioBlocks, DiffRecType type)
{
    IndexedDiffRecord rec;
    rec.init();
    rec.io_address = ioAddr;
    rec.io_blocks = ioBlocks;
    if (type == DiffRecType::DISCARD) rec.setDiscard();
    else if (type == DiffRecType::ALLZERO) rec.setAllZero();
    return rec;
}

CYBOZU_TEST_AUTO(IndexedDiffMem)
{
    DiffIndexMem im;

    im.add(makeIrec(0, 4, DiffRecType::NORMAL));
    im.add(makeIrec(0, 4, DiffRecType::DISCARD));
    im.add(makeIrec(0, 4, DiffRecType::ALLZERO));
    std::vector<IndexedDiffRecord> recV = im.getAsVec();

    CYBOZU_TEST_EQUAL(recV.size(), 1);
    CYBOZU_TEST_ASSERT(recV[0].isAllZero());
}

void verifyRecIoEquality(const DiffRecord& rec0, const AlignedArray& data0, const DiffRecord& rec1, const AlignedArray& data1)
{
    CYBOZU_TEST_EQUAL(rec0.io_address, rec1.io_address);
    CYBOZU_TEST_EQUAL(rec0.io_blocks, rec1.io_blocks);
    CYBOZU_TEST_EQUAL(rec0.compression_type, rec1.compression_type);
    CYBOZU_TEST_EQUAL(rec0.flags, rec1.flags);
    CYBOZU_TEST_EQUAL(rec0.data_size, rec1.data_size);
    // do not check checksum equality.
    CYBOZU_TEST_EQUAL(data0.size(), data1.size());
    CYBOZU_TEST_ASSERT(::memcmp(data0.data(), data1.data(), data0.size()) == 0);
}

void testRandomDiffFile(int cmprType, size_t nrIos)
{
    cybozu::TmpFile tmpFile(".");

    DiffFileHeader header0, header1;
    std::vector<DiffRecord> recV0(nrIos), recV1(nrIos);
    std::vector<AlignedArray> dataV0(nrIos);
    std::vector<AlignedArray> ioV1(nrIos);
    std::list<Sio> sioList = generateSioList(nrIos, true);
    {
        // Prepare recV0 and dataV0.
        CYBOZU_TEST_ASSERT(sioList.size() <= recV0.size());
        size_t i = 0;
        for (const Sio& sio : sioList) {
            sio.copyTo(recV0[i], dataV0[i]);
            i++;
        }
    }
    {
        SortedDiffWriter writer(tmpFile.fd());
        writer.writeHeader(header0);
        for (size_t i = 0; i < nrIos; i++) {
            DiffRecord& rec = recV0[i];
            AlignedArray& data = dataV0[i];
            if (cmprType == ::WALB_DIFF_CMPR_NONE) {
                rec.checksum = rec.isNormal() ? calcDiffIoChecksum(data) : 0;
                writer.writeDiff(rec, data.data());
            } else {
                writer.compressAndWriteDiff(rec, data.data(), cmprType);
            }
        }
        writer.close();
    }
    cybozu::util::File(tmpFile.fd()).lseek(0);
    {
        SortedDiffReader reader(tmpFile.fd());
        reader.readHeader(header1);
        for (size_t i = 0; i < nrIos; i++) {
            const bool ret = reader.readAndUncompressDiff(recV1[i], ioV1[i], false);
            CYBOZU_TEST_ASSERT(ret);
            verifyRecIoEquality(recV0[i], dataV0[i], recV1[i], ioV1[i]);
        }
        DiffRecord rec;
        AlignedArray buf;
        CYBOZU_TEST_ASSERT(!reader.readDiff(rec, buf));
    }
}

void testRandomIndexedDiffFile(int cmprType, size_t nrIos)
{
    ::printf("cmprType %s\n", compressionTypeToStr(cmprType).c_str());
    cybozu::TmpFile tmpFile0(".");

    DiffFileHeader header0;
    std::vector<IndexedDiffRecord> recV0(nrIos);
    std::vector<AlignedArray> dataV0(nrIos);
    std::list<Sio> sioList0 = generateSioList(nrIos, false);
    {
        // Prepare recV0 and dataV0.
        CYBOZU_TEST_ASSERT(sioList0.size() <= recV0.size());
        size_t i = 0;
        for (const Sio& sio : sioList0) {
            sio.copyTo(recV0[i], dataV0[i]);
            i++;
        }
    }
    sioList0.sort();
    {
        IndexedDiffWriter iWriter;
        iWriter.setFd(tmpFile0.fd());
        iWriter.writeHeader(header0);
        for (size_t i = 0; i < nrIos; i++) {
            IndexedDiffRecord& iRec = recV0[i];
            AlignedArray& iData = dataV0[i];
            if (cmprType == ::WALB_DIFF_CMPR_NONE) {
                iRec.io_checksum = iRec.isNormal() ? calcDiffIoChecksum(iData) : 0;
                iWriter.writeDiff(iRec, iData.data());
            } else {
                iWriter.compressAndWriteDiff(iRec, iData.data(), cmprType);
            }
        }
        iWriter.finalize();
    }
    cybozu::util::File(tmpFile0.fd()).lseek(0);
    std::list<Sio> sioList1;
    {
        IndexedDiffReader iReader;
        IndexedDiffCache cache;
        cache.setMaxSize(32 * MEBI);
        iReader.setFile(cybozu::util::File(tmpFile0.fd()), cache);
        IndexedDiffRecord iRec;
        AlignedArray iData;
        while (iReader.readDiff(iRec, iData)) {
            Sio sio;
            sio.copyFrom(iRec, iData);
            mergeOrAddSioList(sioList1, std::move(sio));
        }
        compareSioList(sioList0, sioList1);
    }
}

CYBOZU_TEST_AUTO(SingleIoSortedDiffFile)
{
    testRandomDiffFile(::WALB_DIFF_CMPR_NONE, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_SNAPPY, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_GZIP, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZMA, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZ4, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_ZSTD, 1);
}

CYBOZU_TEST_AUTO(SingleIoIndexedDiffFile)
{
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_NONE, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_SNAPPY, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_GZIP, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_LZMA, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_LZ4, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_ZSTD, 1);
}

CYBOZU_TEST_AUTO(RandomSortedDiffFile)
{
    size_t nr = 100;
    testRandomDiffFile(::WALB_DIFF_CMPR_NONE, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_SNAPPY, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_GZIP, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZMA, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZ4, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_ZSTD, nr);
}

CYBOZU_TEST_AUTO(RandomIndexedDiffFile)
{
    size_t nr = 100;
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_NONE, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_SNAPPY, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_GZIP, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_LZMA, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_LZ4, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_ZSTD, nr);
}
