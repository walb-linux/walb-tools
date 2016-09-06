#include "cybozu/test.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_base.hpp"
#include "random.hpp"

using namespace walb;

enum IoType
{
    Normal, AllZero, Discard,
};

IoType getIoType(const DiffRecord &rec)
{
    if (rec.isAllZero()) return AllZero;
    if (rec.isNormal()) return Normal;
    if (rec.isDiscard()) return Discard;
    throw cybozu::Exception(__func__) << "bad ioType";
}

std::pair<DiffRecord, DiffIo> genRecIo(uint64_t ioAddr, uint32_t ioBlocks, IoType ioType = Normal)
{
    DiffRecord rec;
    rec.io_address = ioAddr;
    rec.io_blocks = ioBlocks;
    rec.compression_type = ::WALB_DIFF_CMPR_NONE;
    rec.data_size = (ioType == Normal ? ioBlocks * LOGICAL_BLOCK_SIZE : 0);
    switch (ioType) {
    case Normal: rec.setNormal(); break;
    case AllZero: rec.setAllZero(); break;
    case Discard: rec.setDiscard(); break;
    default: throw cybozu::Exception("bad ioType") << ioType;
    }
    DiffIo io;
    io.set(rec);
    cybozu::util::Random<uint64_t> rand;
    if (ioType == Normal) {
        rand.fill(io.get(), io.getSize());
    }
    rec.checksum = io.calcChecksum();
    return std::make_pair(rec, std::move(io));
}

void printRecIo(const DiffRecord &rec, const DiffIo &io)
{
    rec.printOneline();
    io.printOneline();
}

void getFromDiffMemory(DiffMemory &diffMem, std::vector<DiffRecord> &recV, std::vector<DiffIo> &ioV)
{
    recV.clear();
    ioV.clear();
    DiffMemory::Map &map = diffMem.getMap();
    DiffMemory::Map::iterator itr = map.begin();
    while (itr != map.end()) {
        recV.push_back(itr->second.record());
        ioV.push_back(itr->second.io());
        ++itr;
    }
}

void verifyDiffIoEquality(const DiffIo &io0, uint32_t startBlocks0, const DiffIo &io1, uint32_t startBlocks1, uint32_t ioBlocks)
{
    CYBOZU_TEST_ASSERT(!io0.isCompressed());
    const size_t off0 = startBlocks0 * LOGICAL_BLOCK_SIZE;
    const size_t off1 = startBlocks1 * LOGICAL_BLOCK_SIZE;
    const size_t size = ioBlocks * LOGICAL_BLOCK_SIZE;
    const char *p0 = io0.get();
    const char *p1 = io1.get();

    CYBOZU_TEST_ASSERT(off0 <= io0.getSize());
    CYBOZU_TEST_ASSERT(off1 <= io1.getSize());
    CYBOZU_TEST_ASSERT(off0 + size <= io0.getSize());
    CYBOZU_TEST_ASSERT(off1 + size <= io1.getSize());

    CYBOZU_TEST_ASSERT(::memcmp(p0 + off0, p1 + off1, size) == 0);
}

void verifyIoTypeEquality(const DiffRecord &rec0, const DiffRecord &rec1)
{
    CYBOZU_TEST_EQUAL(getIoType(rec0), getIoType(rec1));
}

void verifySplitPattern1(IoType type0, IoType type1)
{
    DiffMemory diffMem;
    DiffRecord rec0, rec1;
    DiffIo io0, io1;
    /**
     * __oo__ + xxxxxx = xxxxxx
     */
    std::tie(rec0, io0) = genRecIo(2, 2, type0);
    std::tie(rec1, io1) = genRecIo(0, 6, type1);
    diffMem.add(rec0, DiffIo(io0));
    diffMem.add(rec1, DiffIo(io1));
    diffMem.checkNoOverlappedAndSorted();

    CYBOZU_TEST_EQUAL(diffMem.getMap().size(), 1);
    std::vector<DiffRecord> recV;
    std::vector<DiffIo> ioV;
    getFromDiffMemory(diffMem, recV, ioV);

    verifyIoTypeEquality(rec1, recV[0]);
    CYBOZU_TEST_EQUAL(recV[0].io_address, 0);
    CYBOZU_TEST_EQUAL(recV[0].io_blocks, 6U);
    if (getIoType(recV[0]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[0].getSize(), 6 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io1, 0, ioV[0], 0, 6);
    }
#if 0
    printRecIo(rec0, io0);
    printRecIo(rec1, io1);
    printRecIo(recV[0], ioV[0]);
#endif
}

void verifySplitPattern2(IoType type0, IoType type1)
{
    DiffMemory diffMem;
    DiffRecord rec0, rec1;
    DiffIo io0, io1;
    /**
     * oooooo + __xx__ = ooxxoo
     */
    std::tie(rec0, io0) = genRecIo(0, 6, type0);
    std::tie(rec1, io1) = genRecIo(2, 2, type1);
    diffMem.add(rec0, DiffIo(io0));
    diffMem.add(rec1, DiffIo(io1));
    diffMem.checkNoOverlappedAndSorted();

    CYBOZU_TEST_EQUAL(diffMem.getMap().size(), 3);
    std::vector<DiffRecord> recV;
    std::vector<DiffIo> ioV;
    getFromDiffMemory(diffMem, recV, ioV);

    verifyIoTypeEquality(recV[0], rec0);
    verifyIoTypeEquality(recV[1], rec1);
    verifyIoTypeEquality(recV[2], rec0);
    CYBOZU_TEST_EQUAL(recV[0].io_address, 0);
    CYBOZU_TEST_EQUAL(recV[1].io_address, 2);
    CYBOZU_TEST_EQUAL(recV[2].io_address, 4);
    CYBOZU_TEST_EQUAL(recV[0].io_blocks, 2U);
    CYBOZU_TEST_EQUAL(recV[1].io_blocks, 2U);
    CYBOZU_TEST_EQUAL(recV[2].io_blocks, 2U);
    if (getIoType(recV[0]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[0].getSize(), 2 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io0, 0, ioV[0], 0, 2);
    }
    if (getIoType(recV[1]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[1].getSize(), 2 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io1, 0, ioV[1], 0, 2);
    }
    if (getIoType(recV[2]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[2].getSize(), 2 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io0, 4, ioV[2], 0, 2);
    }
}

void verifySplitPattern3(IoType type0, IoType type1)
{
    DiffMemory diffMem;
    DiffRecord rec0, rec1;
    DiffIo io0, io1;
    /**
     * oooo__ + __xxxx = ooxxxx
     */
    std::tie(rec0, io0) = genRecIo(0, 4, type0);
    std::tie(rec1, io1) = genRecIo(2, 4, type1);
    diffMem.add(rec0, DiffIo(io0));
    diffMem.add(rec1, DiffIo(io1));
    diffMem.checkNoOverlappedAndSorted();

    CYBOZU_TEST_EQUAL(diffMem.getMap().size(), 2);
    std::vector<DiffRecord> recV;
    std::vector<DiffIo> ioV;
    getFromDiffMemory(diffMem, recV, ioV);

    verifyIoTypeEquality(recV[0], rec0);
    verifyIoTypeEquality(recV[1], rec1);
    CYBOZU_TEST_EQUAL(recV[0].io_address, 0);
    CYBOZU_TEST_EQUAL(recV[1].io_address, 2);
    CYBOZU_TEST_EQUAL(recV[0].io_blocks, 2U);
    CYBOZU_TEST_EQUAL(recV[1].io_blocks, 4U);
    if (getIoType(recV[0]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[0].getSize(), 2 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io0, 0, ioV[0], 0, 2);
    }
    if (getIoType(recV[1]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[1].getSize(), 4 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io1, 0, ioV[1], 0, 4);
    }
}

void verifySplitPattern4(IoType type0, IoType type1)
{
    DiffMemory diffMem;
    DiffRecord rec0, rec1;
    DiffIo io0, io1;
    /**
     * __oooo + xxxx__ = xxxxoo
     */
    std::tie(rec0, io0) = genRecIo(2, 4, type0);
    std::tie(rec1, io1) = genRecIo(0, 4, type1);
    diffMem.add(rec0, DiffIo(io0));
    diffMem.add(rec1, DiffIo(io1));
    diffMem.checkNoOverlappedAndSorted();

    CYBOZU_TEST_EQUAL(diffMem.getMap().size(), 2);
    std::vector<DiffRecord> recV;
    std::vector<DiffIo> ioV;
    getFromDiffMemory(diffMem, recV, ioV);

    verifyIoTypeEquality(recV[0], rec1);
    verifyIoTypeEquality(recV[1], rec0);
    CYBOZU_TEST_EQUAL(recV[0].io_address, 0);
    CYBOZU_TEST_EQUAL(recV[1].io_address, 4);
    CYBOZU_TEST_EQUAL(recV[0].io_blocks, 4U);
    CYBOZU_TEST_EQUAL(recV[1].io_blocks, 2U);
    if (getIoType(recV[0]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[0].getSize(), 4 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io1, 0, ioV[0], 0, 4);
    }
    if (getIoType(recV[1]) == Normal) {
        CYBOZU_TEST_EQUAL(ioV[1].getSize(), 2 * LOGICAL_BLOCK_SIZE);
        verifyDiffIoEquality(io0, 2, ioV[1], 0, 2);
    }
}

CYBOZU_TEST_AUTO(splitPattern)
{
    for (auto type0 : {Normal, AllZero, Discard}) {
        for (auto type1 : {Normal, AllZero, Discard}) {
            verifySplitPattern1(type0, type1);
            verifySplitPattern2(type0, type1);
            verifySplitPattern3(type0, type1);
            verifySplitPattern4(type0, type1);
        }
    }
}
