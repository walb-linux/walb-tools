#include "cybozu/test.hpp"
#include "cybozu/array.hpp"
#include "walb_diff_merge.hpp"
#include "tmp_file.hpp"
#include "random.hpp"
#include <vector>

using namespace walb;
using Buffer = std::vector<char>;
class TmpDiffFile;
using TmpDiffFileVec = std::vector<TmpDiffFile>;

cybozu::util::Random<size_t> g_rand;

class TmpDiffFile
{
private:
    cybozu::TmpFile tmpFile;
    DiffWriter writer;
    uint64_t curAddr;

public:
    TmpDiffFile() : tmpFile("."), writer(tmpFile.fd()), curAddr(0) {
        writeHeader();
    }
    void writeHeader() {
        DiffFileHeader head;
        writer.writeHeader(head);
    }
    void writeIo(uint64_t addr, size_t len) {
        CYBOZU_TEST_ASSERT(addr >= curAddr);
        CYBOZU_TEST_ASSERT(len > 0);
        DiffRecord rec;
        DiffIo io;
        prepareIo(addr, len, rec, io);
#if 1
        writer.writeDiff(rec, std::move(io));
#else
        writer.compressAndWriteDiff(rec, io.data.data());
#endif
        curAddr = addr + len;
    }
    void writeEof() {
        writer.close();
    }
    std::string path() const {
        return tmpFile.path();
    }
private:
    void prepareIo(uint64_t addr, size_t len, DiffRecord &rec, DiffIo &io) {
        rec.init();
        rec.io_address = addr;
        rec.io_blocks = len;
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;

        const size_t r = g_rand.get<size_t>() % 100;
        if (r < 10) { // 10%
            rec.setDiscard();
            io.clear();
            return;
        }
        if (r < 20) { // 10%
            rec.setAllZero();
            io.clear();
            return;
        }
        // 80%
        rec.setNormal();
        io.data.resize(len * LBS);
#if 1
        g_rand.fill(io.data.data(), io.data.size());
#endif
        rec.data_size = io.data.size();
        io.set(rec);
        rec.checksum = calcDiffIoChecksum(io.data);
    }
};

class TmpDisk
{
private:
    Buffer buf_;
public:
    explicit TmpDisk(size_t len) : buf_(len * LBS) {
    }
    void clear() {
        ::memset(buf_.data(), 0, buf_.size());
    }
    void writeDiff(const DiffRecord &rec, const DiffIo &io) {
        verifyAddr(rec.io_address, rec.io_blocks);
        if (rec.isAllZero()) {
            writeAllZero(rec.io_address, rec.io_blocks);
            return;
        }
        if (rec.isDiscard()) {
            discard(rec.io_address, rec.io_blocks);
            return;
        }
        assert(rec.isNormal());
        if (rec.isCompressed()) {
            DiffRecord outRec;
            DiffIo outIo;
            uncompressDiffIo(rec, io.data.data(), outRec, outIo.data);
            write(rec.io_address, rec.io_blocks, outIo.data.data());
        } else {
            write(rec.io_address, rec.io_blocks, io.data.data());
        }
    }
    void verifyEquals(const TmpDisk &rhs) const {
        const size_t len = buf_.size() / LBS;
        Buffer buf0(LBS), buf1(LBS);
        size_t nr = 0;
        for (size_t i = 0; i < len; i++) {
            read(i, 1, buf0.data());
            rhs.read(i, 1, buf1.data());
            if (::memcmp(buf0.data(), buf1.data(), LBS) != 0) {
                ::printf("block differ %zu\n", i);
                nr++;
            }
        }
        if (nr > 0) throw cybozu::Exception(__func__) << nr;
    }
    void apply(const std::string &diffPath) {
        DiffReader reader(diffPath);
        DiffFileHeader head;
        reader.readHeader(head);
        DiffRecord rec;
        DiffIo io;
        while (reader.readDiff(rec, io)) {
            writeDiff(rec, io);
        }
    }
private:
    void read(uint64_t addr, size_t len, char *data) const {
        verifyAddr(addr, len);
        ::memcpy(data, &buf_[addr * LBS], len * LBS);
    }
    void write(uint64_t addr, size_t len, const char *data) {
        verifyAddr(addr, len);
        ::memcpy(&buf_[addr * LBS], data, len * LBS);
    }
    void writeAllZero(uint64_t addr, size_t len) {
        verifyAddr(addr, len);
        ::memset(&buf_[addr * LBS], 0, len * LBS);
    }
    void discard(uint64_t addr, size_t len) {
        writeAllZero(addr, len);
    }
    void verifyAddr(uint64_t addr, size_t len) const {
        CYBOZU_TEST_ASSERT(len > 0);
        CYBOZU_TEST_ASSERT(addr + len <= buf_.size() / LBS);
    }
};

void verifyMergedDiff(size_t len, TmpDiffFileVec &d)
{
    TmpDisk disk0(len), disk1(len);

    for (size_t i = 0; i < d.size(); i++) {
        disk0.apply(d[i].path());
    }

    cybozu::TmpFile merged(".");
    DiffMerger merger(0);
    for (size_t i = 0; i < d.size(); i++) {
        merger.addWdiff(d[i].path());
    }
    merger.mergeToFd(merged.fd());
    disk1.apply(merged.path());

    disk0.verifyEquals(disk1);
}

struct MetaIo
{
    uint64_t addr;
    size_t len;
};

using Recipe = std::vector<std::vector<MetaIo> >;

void execIoRecipe(TmpDiffFileVec &d, Recipe &&recipe)
{
    CYBOZU_TEST_EQUAL(d.size(), recipe.size());

    for (size_t i = 0; i < d.size(); i++) {
        for (const MetaIo &mio : recipe[i]) {
            d[i].writeIo(mio.addr, mio.len);
        }
        d[i].writeEof();
    }
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
        TmpDiffFileVec d(3);
        execIoRecipe(d, {
                {{0, 3}, {4, 3}, {8, 3}},
                {{0, 3}, {4, 3}, {8, 3}},
                {{0, 3}, {4, 3}, {8, 3}}});
        verifyMergedDiff(20, d);
    }

    /*
     * addr   01234567890123456789
     * diff2  ZZZZ
     * diff1    YYYY
     * diff0      XXXX
     */
    {
        TmpDiffFileVec d(3);
        execIoRecipe(d, {{{4, 4}}, {{2, 4}}, {{0, 4}}});
        verifyMergedDiff(10, d);
    }

    /*
     * addr   01234567890123456789
     * diff3  DDDD
     * diff2    ZZZZ
     * diff1        BBBB
     * diff0          AAAA
     */
    {
        TmpDiffFileVec d(4);
        execIoRecipe(d, {{{8, 4}}, {{6, 4}}, {{2, 4}}, {{0, 4}}});
        verifyMergedDiff(20, d);
    }

    /**
     * addr   01234567890123456789
     * diff2  CCCCCCCCCC
     * diff1    BBBB
     * diff0      AAAA
     */
    {
        TmpDiffFileVec d(3);
        execIoRecipe(d, {{{4, 4}}, {{2, 4}}, {{0, 10}}});
        verifyMergedDiff(20, d);
    }

    /**
     * addr   01234567890123456789
     * diff2  FFFFFFFFFFFFFFFF
     * diff1    DDDD EEEE
     * diff0      AAAA BBBB CCCC
     */
    {
        TmpDiffFileVec d(3);
        execIoRecipe(d, {
                {{4, 4}, {9, 4}, {14, 4}},
                {{2, 4}, {7, 4}},
                {{0, 16}}});
        verifyMergedDiff(20, d);
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
        TmpDiffFileVec d(5);
        execIoRecipe(d, {
                {{8, 4}, {13, 4}, {18, 4}},
                {{6, 4}, {11, 4}},
                {{4, 14}},
                {{2, 4}},
                {{0, 4}}
            });
        verifyMergedDiff(30, d);
    }

    /**
     * addr   01234567890123456789
     * diff2      XXX XXX XXX XXX
     * diff1    XXX XXX XXX XXX
     * diff0  XXX XXX XXX XXX
     */
    {
        TmpDiffFileVec d(3);
        execIoRecipe(d, {
                {{0, 3}, {4, 3}, {8, 3}, {12, 3}},
                {{2, 3}, {6, 3}, {10, 3}, {14, 3}},
                {{4, 3}, {8, 3}, {12, 3}, {16, 3}}});
        verifyMergedDiff(20, d);
    }

    /**
     * addr   01234567890123456789
     * diff2      XXX XXX XXX XXX
     * diff1    XXX XXX XXX XXX
     * diff0  XXX XXX XXX XXX
     */
    {
        TmpDiffFileVec d(3);
        execIoRecipe(d, {
                {{0, 3}, {4, 3}, {8, 3}, {12, 3}},
                {{2, 3}, {6, 3}, {10, 3}, {14, 3}},
                {{4, 3}, {8, 3}, {12, 3}, {16, 3}}});
        verifyMergedDiff(20, d);
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
        TmpDiffFileVec d(6);
        execIoRecipe(d, {
                {{12, 3}},
                {{6, 3}},
                {{4, 3}},
                {{12, 3}},
                {{2, 3}},
                {{0, 3}}
            });
        verifyMergedDiff(20, d);
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
        TmpDiffFileVec d(6);
        execIoRecipe(d, {
                {{2, 3}},
                {{0, 3}},
                {{2, 3}},
                {{0, 3}},
                {{2, 3}},
                {{0, 3}}
            });
        verifyMergedDiff(10, d);
    }
}
