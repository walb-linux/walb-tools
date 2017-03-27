#include "cybozu/test.hpp"
#include "walb_diff_file.hpp"
#include "tmp_file.hpp"
#include "random.hpp"
#include <sstream>

using namespace walb;

cybozu::util::Random<size_t> rand_;

/**
 * If you want to fix the seed, specify it to rand_.setSeed().
 */
CYBOZU_TEST_AUTO(Setup)
{
    rand_.setSeed(395019343);
    ::printf("random number generator seed: %zu\n", rand_.getSeed());
}

CYBOZU_TEST_AUTO(NullSortedDiffFile)
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

enum class RecType : uint8_t
{
    NORMAL, DISCARD, ALLZERO,
};

std::ostream& operator<<(std::ostream& os, RecType type)
{
    if (type == RecType::NORMAL) {
        os << "Normal";
    } else if (type == RecType::DISCARD) {
        os << "Discard";
    } else if (type == RecType::ALLZERO) {
        os << "Allzero";
    }
    return os;
}

IndexedDiffRecord makeIrec(uint64_t ioAddr, uint32_t ioBlocks, RecType type)
{
    IndexedDiffRecord rec;
    rec.init();
    rec.io_address = ioAddr;
    rec.io_blocks = ioBlocks;
    if (type == RecType::DISCARD) rec.setDiscard();
    else if (type == RecType::ALLZERO) rec.setAllZero();
    return rec;
}

CYBOZU_TEST_AUTO(IndexedDiffMem)
{
    DiffIndexMem im;

    im.add(makeIrec(0, 4, RecType::NORMAL));
    im.add(makeIrec(0, 4, RecType::DISCARD));
    im.add(makeIrec(0, 4, RecType::ALLZERO));
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

struct TmpIo
{
    uint64_t ioAddr;
    uint32_t ioBlocks;
    RecType type;
    AlignedArray data;

    void clear() {
        ioAddr = 0;
        ioBlocks = 0;
        type = RecType::NORMAL;
        data.clear();
    }
    bool operator<(const TmpIo& rhs) const {
        return ioAddr < rhs.ioAddr;
    }
    bool operator==(const TmpIo& rhs) const {
        if (ioAddr != rhs.ioAddr) return false;
        if (ioBlocks != rhs.ioBlocks) return false;
        if (type != rhs.type) return false;
        if (type != RecType::NORMAL) return true;
        if (data.size() != rhs.data.size()) return false;
        return ::memcmp(data.data(), rhs.data.data(), data.size()) == 0;
    }
    bool operator!=(const TmpIo& rhs) const { return !(*this == rhs); }
    bool isOverlapped(const TmpIo& rhs) const {
        return cybozu::isOverlapped(ioAddr, ioBlocks, rhs.ioAddr, rhs.ioBlocks);
    }
    void setRandomly() {
        const size_t r = rand_() % 100;
        if (r < 80) {
            type = RecType::NORMAL;
        } else if (r < 90) {
            type = RecType::DISCARD;
        } else {
            type = RecType::ALLZERO;
        }
        ioBlocks = rand_() % 15 + 1;
        if (type == RecType::NORMAL) {
            data.resize(LOGICAL_BLOCK_SIZE * ioBlocks);
            rand_.fill(data.data(), data.size());
        } else {
            data.resize(0);
        }
        ioAddr = rand_.get64() % (128 * MEBI / LOGICAL_BLOCK_SIZE);
    }
    template <typename Record>
    void copyTopHalfTo(Record& rec) const {
        rec.init();
        rec.io_address = ioAddr;
        rec.io_blocks = ioBlocks;
        if (type == RecType::NORMAL) {
            rec.setNormal();
        } else if (type == RecType::DISCARD) {
            rec.setDiscard();
        } else {
            CYBOZU_TEST_EQUAL(type, RecType::ALLZERO);
            rec.setAllZero();
        }
    }
    void copyTo(DiffRecord& rec, AlignedArray& data0) const {
        copyTopHalfTo(rec);
        rec.data_size = rec.isNormal() ? data.size() : 0;
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.checksum = 0; // not calculated now.
        data0.resize(data.size());
        ::memcpy(data0.data(), data.data(), data0.size());
    }
    void copyTo(IndexedDiffRecord& rec, AlignedArray& data0) const {
        copyTopHalfTo(rec);
        rec.data_size = rec.isNormal() ? data.size() : 0;
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.orig_blocks = rec.io_blocks;
        rec.io_checksum = 0; // not calculated now.
        rec.rec_checksum = 0; // not calculated now.
        data0.resize(data.size());
        ::memcpy(data0.data(), data.data(), data0.size());
    }
    void copyFrom(const IndexedDiffRecord& rec, const AlignedArray& data0) {
        ioAddr = rec.io_address;
        ioBlocks = rec.io_blocks;
        if (rec.isNormal()) type = RecType::NORMAL;
        else if (rec.isDiscard()) type = RecType::DISCARD;
        else if (rec.isAllZero()) type = RecType::ALLZERO;
        else throw cybozu::Exception("bad type") << type;
        data.resize(data0.size());
        ::memcpy(data.data(), data0.data(), data.size());
    }
    bool tryMerge(const TmpIo& rhs) {
        if (ioAddr + ioBlocks != rhs.ioAddr || type != rhs.type) return false;
        ioBlocks += rhs.ioBlocks;
        if (type == RecType::NORMAL) {
            size_t oldSize = data.size();
            data.resize(oldSize + rhs.data.size());
            ::memcpy(data.data() + oldSize, rhs.data.data(), rhs.data.size());
        }
        return true;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const TmpIo& tmpIo) {
        os << tmpIo.ioAddr << "\t" << tmpIo.ioBlocks << "\t" << tmpIo.type;
        const uint32_t csum =
            tmpIo.type == RecType::NORMAL ? calcDiffIoChecksum(tmpIo.data) : 0;
        os << cybozu::util::formatString("\t%08x", csum);
        return os;
    }
};

std::string dataToStr(const AlignedArray& data)
{
    std::stringstream ss;
    for (size_t i = 0; i < data.size(); i++) {
        ss << cybozu::util::formatString("%02x", uint8_t(data[i]));
    }
    return ss.str();
}

class RangeSet
{
    using Key = std::pair<uint64_t, bool>;
    using Set = std::set<Key>;
    Set set_;

    struct Deleter {
        Set& set_;
        Key key_;
        bool dontDelete_;
        Deleter(Set& set, Key key) : set_(set), key_(key), dontDelete_(false) {}
        void dontDelete() { dontDelete_ = true; }
        ~Deleter() noexcept {
            if (dontDelete_) return;
            set_.erase(key_);
        }
    };
public:
    bool tryAdd(uint64_t bgn, uint64_t end) {
        Set::iterator it0;
        Key key0{bgn, 0};
        bool ret;
        std::tie(it0, ret) = set_.insert(key0);
        if (!ret) return false;
        Deleter d0(set_, key0);
        Set::iterator it1;
        Key key1{end, 1};
        std::tie(it1, ret) = set_.insert(key1);
        if (!ret) return false;
        Deleter d1(set_, key1);
        if (++it0 != it1) return false;
        d0.dontDelete();
        d1.dontDelete();
        return true;
    }
};

void removeOverlapped(std::list<TmpIo>& list)
{
    if (list.empty()) return;
    RangeSet rset;
    auto it = list.begin();
    while (it != list.end()) {
        if (!rset.tryAdd(it->ioAddr, it->ioAddr + it->ioBlocks)) {
            it = list.erase(it);
        } else {
            ++it;
        }
    }
#if 0
    for (const TmpIo& tmpIo : list) {
        ::printf("%" PRIu64 " %" PRIu64 "\n", tmpIo.ioAddr, tmpIo.ioAddr + tmpIo.ioBlocks);
    }
#endif
}

std::list<TmpIo> generateTmpIoList(size_t nr, bool mustBeSorted)
{
    std::list<TmpIo> list;
    size_t prevNr = 0;
    while (list.size() < nr) {
        for (size_t i = 0; i < nr; i++) {
            list.emplace_back();
            list.back().setRandomly();
        }
        removeOverlapped(list);
        if (prevNr == list.size()) {
            throw cybozu::Exception("can not increase the number of IOs.");
        }
        prevNr = list.size();
    }
    if (mustBeSorted) list.sort();
    while (list.size() > nr) list.pop_back();
    return list;
}

void mergeOrAddTmpIoList(std::list<TmpIo>& list, TmpIo&& tmpIo)
{
    if (list.empty() || !list.back().tryMerge(tmpIo)) {
        list.push_back(std::move(tmpIo));
    }
}

void compareTmpIoList(const std::list<TmpIo>& lhs, const std::list<TmpIo>& rhs)
{
    CYBOZU_TEST_EQUAL(lhs.size(), rhs.size());
    auto it0 = lhs.begin();
    auto it1 = rhs.begin();
    bool differ = false;
    while (it0 != lhs.end() && it1 != rhs.end()) {
#if 1
        CYBOZU_TEST_EQUAL(*it0, *it1);
#endif
        if (*it0 != *it1) differ = true;
#if 0
        if (*it0 != *it1) {
            ::printf("%s\n", dataToStr(it0->data).c_str());
            ::printf("%s\n", dataToStr(it1->data).c_str());
        }
#endif
        ++it0;
        ++it1;
    }
    if (differ) {
        it0 = lhs.begin(); it1 = rhs.begin();
        while (it0 != lhs.end() && it1 != rhs.end()) {
            std::cout << *it0 << "\t---\t" << *it1 << std::endl;
            ++it0; ++it1;
        }
        while (it0 != lhs.end()) {
            std::cout << *it0 << "\t---" << std::endl;
            ++it0;
        }
        while (it1 != rhs.end()) {
            std::cout << "\t---\t" << *it1 << std::endl;
            ++it1;
        }
    }
}

void testRandomDiffFile(int cmprType, size_t nrIos)
{
    cybozu::TmpFile tmpFile(".");

    DiffFileHeader header0, header1;
    std::vector<DiffRecord> recV0(nrIos), recV1(nrIos);
    std::vector<AlignedArray> dataV0(nrIos);
    std::vector<DiffIo> ioV1(nrIos);
    std::list<TmpIo> tmpIoList = generateTmpIoList(nrIos, true);
    {
        // Prepare recV0 and dataV0.
        CYBOZU_TEST_ASSERT(tmpIoList.size() <= recV0.size());
        size_t i = 0;
        for (const TmpIo& tmpIo : tmpIoList) {
            tmpIo.copyTo(recV0[i], dataV0[i]);
            i++;
        }
    }
    {
        DiffWriter writer(tmpFile.fd());
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

void testRandomIndexedDiffFile(int cmprType, size_t nrIos)
{
    cybozu::TmpFile tmpFile0(".");

    DiffFileHeader header0;
    std::vector<IndexedDiffRecord> recV0(nrIos);
    std::vector<AlignedArray> dataV0(nrIos);
    std::list<TmpIo> tmpIoList0 = generateTmpIoList(nrIos, false);
    {
        // Prepare recV0 and dataV0.
        CYBOZU_TEST_ASSERT(tmpIoList0.size() <= recV0.size());
        size_t i = 0;
        for (const TmpIo& tmpIo : tmpIoList0) {
            tmpIo.copyTo(recV0[i], dataV0[i]);
            i++;
        }
    }
    tmpIoList0.sort();
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
    std::list<TmpIo> tmpIoList1;
    {
        IndexedDiffReader iReader;
        IndexedDiffCache cache;
        cache.setMaxSize(32 * MEBI);
        iReader.setFile(cybozu::util::File(tmpFile0.fd()), cache);
        IndexedDiffRecord iRec;
        AlignedArray iData;
        while (iReader.readDiff(iRec, iData)) {
            TmpIo tmpIo;
            tmpIo.copyFrom(iRec, iData);
            mergeOrAddTmpIoList(tmpIoList1, std::move(tmpIo));
        }
        compareTmpIoList(tmpIoList0, tmpIoList1);
    }
}

CYBOZU_TEST_AUTO(SingleIoSortedDiffFile)
{
    testRandomDiffFile(::WALB_DIFF_CMPR_NONE, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_SNAPPY, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_GZIP, 1);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZMA, 1);
}

CYBOZU_TEST_AUTO(SingleIoIndexedDiffFile)
{
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_NONE, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_SNAPPY, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_GZIP, 1);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_LZMA, 1);
}

CYBOZU_TEST_AUTO(RandomSortedDiffFile)
{
    size_t nr = 100;
    testRandomDiffFile(::WALB_DIFF_CMPR_NONE, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_SNAPPY, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_GZIP, nr);
    testRandomDiffFile(::WALB_DIFF_CMPR_LZMA, nr);
}

CYBOZU_TEST_AUTO(RandomIndexedDiffFile)
{
    size_t nr = 100;
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_NONE, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_SNAPPY, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_GZIP, nr);
    testRandomIndexedDiffFile(::WALB_DIFF_CMPR_LZMA, nr);
}
