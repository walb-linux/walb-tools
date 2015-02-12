#include "cybozu/test.hpp"
#include "walb_diff_file.hpp"
#include "tmp_file.hpp"
#include "random.hpp"

using namespace walb;

cybozu::util::Random<size_t> rand_;

CYBOZU_TEST_AUTO(NullDiffFile)
{
    cybozu::TmpFile tmpFile(".");

    DiffWriter writer(tmpFile.fd());
    DiffFileHeader header0, header1;
    writer.writeHeader(header0);
    writer.close();

    cybozu::util::File(tmpFile.fd()).lseek(0);

    DiffReader reader(tmpFile.fd());
    reader.readHeader(header1);
    DiffRecord rec;
    DiffIo io;
    CYBOZU_TEST_ASSERT(!reader.readDiff(rec, io));
}

void setRandomRecIo(DiffRecord& rec, AlignedArray& data)
{
    rec.init();
    const size_t r = rand_() % 100;
    if (r < 80) {
        rec.setNormal();
    } else if (r < 90) {
        rec.setDiscard();
    } else {
        rec.setAllZero();
    }
    if (rec.isNormal()) {
        data.resize(LOGICAL_BLOCK_SIZE * (rand_() % 15 + 1));
        rand_.fill(data.data(), data.size());
    } else {
        data.resize(0);
    }
    rec.io_address = rand_.get64();
    rec.io_blocks = data.size() / LOGICAL_BLOCK_SIZE;
    rec.data_size = rec.isNormal() ? data.size() : 0;
    rec.compression_type = ::WALB_DIFF_CMPR_NONE;
    rec.checksum = 0; // not calculated.
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
    std::vector<DiffIo> ioV1(nrIos);
    {
        DiffWriter writer(tmpFile.fd());
        writer.writeHeader(header0);
        for (size_t i = 0; i < nrIos; i++) {
            DiffRecord& rec = recV0[i];
            AlignedArray& data = dataV0[i];
            setRandomRecIo(rec, data);
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
        DiffReader reader(tmpFile.fd());
        reader.readHeader(header1);
        for (size_t i = 0; i < nrIos; i++) {
            const bool ret = reader.readAndUncompressDiff(recV1[i], ioV1[i], false);
            CYBOZU_TEST_ASSERT(ret);
            verifyRecIoEquality(recV0[i], dataV0[i], recV1[i], ioV1[i].data);
        }
        DiffRecord rec;
        DiffIo io;
        CYBOZU_TEST_ASSERT(!reader.readDiff(rec, io));
    }
}

CYBOZU_TEST_AUTO(SingleIoDiffFile)
{
    testRandomDiffFile(::WALB_DIFF_CMPR_NONE, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_SNAPPY, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_GZIP, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZMA, 1);
}

CYBOZU_TEST_AUTO(RandomDiffFile)
{
    testRandomDiffFile(::WALB_DIFF_CMPR_NONE, 100);
    testRandomDiffFile(::WALB_DIFF_CMPR_SNAPPY, 100);
    testRandomDiffFile(::WALB_DIFF_CMPR_GZIP, 100);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZMA, 100);
}
