#pragma once
#include "walb_diff_base.hpp"
#include "random.hpp"
#include <list>

namespace for_walb_diff_test_local {

cybozu::util::Random<size_t> *rand_ = nullptr;

} // namespace for_walb_diff_test_local


inline void setRandForTest(cybozu::util::Random<size_t>& rand)
{
    for_walb_diff_test_local::rand_ = &rand;
}

namespace walb {

/**
 * Simple IO for test.
 */
struct Sio
{
    uint64_t ioAddr;
    uint32_t ioBlocks;
    DiffRecType type;
    AlignedArray data;

    void clear() {
        ioAddr = 0;
        ioBlocks = 0;
        type = DiffRecType::NORMAL;
        data.clear();
    }
    bool operator<(const Sio& rhs) const {
        return ioAddr < rhs.ioAddr;
    }
    bool operator==(const Sio& rhs) const {
        if (ioAddr != rhs.ioAddr) return false;
        if (ioBlocks != rhs.ioBlocks) return false;
        if (type != rhs.type) return false;
        if (type != DiffRecType::NORMAL) return true;
        if (data.size() != rhs.data.size()) return false;
        return ::memcmp(data.data(), rhs.data.data(), data.size()) == 0;
    }
    bool operator!=(const Sio& rhs) const { return !(*this == rhs); }
    bool isOverlapped(const Sio& rhs) const {
        return cybozu::isOverlapped(ioAddr, ioBlocks, rhs.ioAddr, rhs.ioBlocks);
    }
    void setRandomly() {
        cybozu::util::Random<size_t> &rand_ = *for_walb_diff_test_local::rand_;
        ioAddr = rand_.get64() % (128 * MEBI / LOGICAL_BLOCK_SIZE);
        ioBlocks = rand_() % 15 + 1;
        setTypeRandomly();
        setDataRandomly();
    }
    void setRandomly(uint64_t ioAddr, uint32_t ioBlocks) {
        this->ioAddr = ioAddr;
        this->ioBlocks = ioBlocks;
        setTypeRandomly();
        setDataRandomly();
    }
    void setRandomly(uint64_t ioAddr, uint32_t ioBlocks, DiffRecType type) {
        this->ioAddr = ioAddr;
        this->ioBlocks = ioBlocks;
        this->type = type;
        setDataRandomly();
    }
    void setTypeRandomly() {
        cybozu::util::Random<size_t> &rand_ = *for_walb_diff_test_local::rand_;
        const size_t r = rand_() % 100;
        if (r < 80) {
            type = DiffRecType::NORMAL;
        } else if (r < 90) {
            type = DiffRecType::DISCARD;
        } else {
            type = DiffRecType::ALLZERO;
        }
    }
    void setDataRandomly() {
        cybozu::util::Random<size_t> &rand_ = *for_walb_diff_test_local::rand_;
        if (type == DiffRecType::NORMAL) {
            data.resize(LOGICAL_BLOCK_SIZE * ioBlocks);
            rand_.fill(data.data(), data.size());
        } else {
            data.resize(0);
        }
    }
    std::vector<Sio> split(const std::vector<uint32_t>& sizeLbV) const {
        std::vector<Sio> ret;
        uint64_t offLb = 0;
        uint32_t remainingLb = ioBlocks;
        size_t i = 0;
        while (remainingLb > 0) {
            const uint32_t lb = i < sizeLbV.size() ? std::min(sizeLbV[i], remainingLb) : remainingLb;
            Sio sio{ioAddr + offLb, lb, type, AlignedArray()};
            if (type == DiffRecType::NORMAL) {
                sio.data.resize(lb * LOGICAL_BLOCK_SIZE);
                ::memcpy(sio.data.data(), data.data() + (offLb * LOGICAL_BLOCK_SIZE), sio.data.size());
            }
            ret.push_back(std::move(sio));
            offLb += lb;
            remainingLb -= lb;
            i++;
        }
        return ret;
    }
    template <typename Record>
    void copyTopHalfTo(Record& rec) const {
        rec.init();
        rec.io_address = ioAddr;
        rec.io_blocks = ioBlocks;
        if (type == DiffRecType::NORMAL) {
            rec.setNormal();
        } else if (type == DiffRecType::DISCARD) {
            rec.setDiscard();
        } else {
            CYBOZU_TEST_EQUAL(type, DiffRecType::ALLZERO);
            rec.setAllZero();
        }
    }
    void copyTo(DiffRecord& rec, AlignedArray& data0) const {
        copyTopHalfTo(rec);
        rec.data_size = rec.isNormal() ? data.size() : 0;
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.checksum = 0; // not calculated now.
        data0.resize(rec.data_size);
        if (!data0.empty()) ::memcpy(data0.data(), data.data(), data0.size());
    }
    void copyTo(IndexedDiffRecord& rec, AlignedArray& data0) const {
        copyTopHalfTo(rec);
        rec.data_size = rec.isNormal() ? data.size() : 0;
        rec.compression_type = ::WALB_DIFF_CMPR_NONE;
        rec.orig_blocks = rec.io_blocks;
        rec.io_checksum = 0; // not calculated now.
        rec.rec_checksum = 0; // not calculated now.
        data0.resize(rec.data_size);
        if (!data0.empty()) ::memcpy(data0.data(), data.data(), data0.size());
    }
    template <typename Record>
    void copyTopHalfFrom(const Record& rec) {
        ioAddr = rec.io_address;
        ioBlocks = rec.io_blocks;
        if (rec.isNormal()) type = DiffRecType::NORMAL;
        else if (rec.isDiscard()) type = DiffRecType::DISCARD;
        else if (rec.isAllZero()) type = DiffRecType::ALLZERO;
        else throw cybozu::Exception("bad type") << type;
    }
    void copyBottomHalfFrom(const AlignedArray& data0) {
        if (type == DiffRecType::NORMAL) {
            data.resize(data0.size());
            ::memcpy(data.data(), data0.data(), data.size());
        } else {
            data.clear();
        }
    }
    void copyFrom(const DiffRecord& rec, const AlignedArray& data0) {
        CYBOZU_TEST_EQUAL(rec.compression_type, ::WALB_DIFF_CMPR_NONE);
        copyTopHalfFrom(rec);
        copyBottomHalfFrom(data0);
    }
    void copyFrom(const IndexedDiffRecord& rec, const AlignedArray& data0) {
        CYBOZU_TEST_EQUAL(rec.compression_type, ::WALB_DIFF_CMPR_NONE);
        copyTopHalfFrom(rec);
        copyBottomHalfFrom(data0);
    }
    bool tryMerge(const Sio& rhs) {
        if (ioAddr + ioBlocks != rhs.ioAddr || type != rhs.type) return false;
        ioBlocks += rhs.ioBlocks;
        if (type == DiffRecType::NORMAL) {
            size_t oldSize = data.size();
            data.resize(oldSize + rhs.data.size());
            ::memcpy(data.data() + oldSize, rhs.data.data(), rhs.data.size());
        }
        return true;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const Sio& sio) {
        os << sio.ioAddr << "\t" << sio.ioBlocks << "\t" << sio.type;
        const uint32_t csum =
            sio.type == DiffRecType::NORMAL ? calcDiffIoChecksum(sio.data) : 0;
        os << cybozu::util::formatString("\t%08x", csum);
        return os;
    }
};

using SioList = std::list<Sio>;
using SioVec = std::vector<Sio>;

inline std::string dataToStr(const AlignedArray& data)
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
        Set::iterator it0, it1, it;
        Key key0{bgn, 0};
        bool ret;
        std::tie(it0, ret) = set_.insert(key0);
        if (!ret) return false;
        Deleter d0(set_, key0);
        Key key1{end, 1};
        std::tie(it1, ret) = set_.insert(key1);
        if (!ret) return false;
        Deleter d1(set_, key1);

        /*
         * 1st check.
         *  |       |      |
         * it0   bgn/end  it1
         */
        it = it0;
        if (++it != it1) return false;

        /*
         * 2nd check.
         *  |     |    |    |
         * bgn   it0  it1  end
         **/
        it = it0;
        if (it != set_.begin() && (--it)->second == 0) return false;

        /* Now there are no overlapped ranges. */
        d0.dontDelete();
        d1.dontDelete();
        return true;
    }
};

inline void removeOverlapped(SioList& list)
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
    for (const Sio& sio : list) {
        ::printf("%" PRIu64 " %" PRIu64 "\n", sio.ioAddr, sio.ioAddr + sio.ioBlocks);
    }
#endif
}

inline SioList generateSioList(size_t nr, bool mustBeSorted)
{
    SioList list;
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

inline void mergeOrAddSioList(SioList& list, Sio&& sio)
{
    if (list.empty() || !list.back().tryMerge(sio)) {
        list.push_back(std::move(sio));
    }
}

inline void compareSioList(const SioList& lhs, const SioList& rhs)
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

class TmpDisk
{
private:
    AlignedArray buf_;
public:
    explicit TmpDisk(size_t len) : buf_(len * LBS) {
        clear();
    }
    void clear() {
        ::memset(buf_.data(), 0, buf_.size());
    }
    void writeDiff(const DiffRecord &rec, const AlignedArray &data) {
        if (writeDiffTopHalf(rec)) return;
        assert(!rec.isCompressed());
        write(rec.io_address, rec.io_blocks, data.data());
    }
    void writeDiff(const IndexedDiffRecord &rec, const AlignedArray &data) {
        if (writeDiffTopHalf(rec)) return;
        write(rec.io_address, rec.io_blocks, data.data());
    }
    void writeSio(const Sio& sio) {
        if (sio.type == DiffRecType::NORMAL) {
            write(sio.ioAddr, sio.ioBlocks, sio.data.data());
            return;
        }
        if (sio.type == DiffRecType::ALLZERO) {
            writeAllZero(sio.ioAddr, sio.ioBlocks);
            return;
        }
        if (sio.type == DiffRecType::DISCARD) {
            discard(sio.ioAddr, sio.ioBlocks);
            return;
        }
        throw cybozu::Exception("writeSio:bad diff rec type") << sio.type;
    }
    void verifyEquals(const TmpDisk &rhs) const {
        const size_t len = buf_.size() / LBS;
        AlignedArray buf0(LBS), buf1(LBS);
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
    void apply(const SioList& sioList) {
        for (const Sio& sio : sioList) {
            writeSio(sio);
        }
    }
    void apply(const std::string &diffPath) {
        cybozu::util::File file(diffPath, O_RDONLY);
        DiffFileHeader header;
        header.readFrom(file);
        if (header.isIndexed()) {
            IndexedDiffReader reader;
            IndexedDiffCache cache;
            cache.setMaxSize(4 * MEBI);
            reader.setFile(std::move(file), cache);
            IndexedDiffRecord rec;
            AlignedArray data;
            while (reader.readDiff(rec, data)) {
                writeDiff(rec, data);
            }
        } else {
            SortedDiffReader reader(std::move(file));
            reader.dontReadHeader();
            DiffRecord rec;
            AlignedArray data;
            while (reader.readAndUncompressDiff(rec, data, false)) {
                writeDiff(rec, data);
            }
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
        verifyAddr(addr, len);
        ::memset(&buf_[addr * LBS], 0xff, len * LBS); // This is for test.
    }
    void verifyAddr(uint64_t addr, size_t len) const {
        CYBOZU_TEST_ASSERT(len > 0);
        CYBOZU_TEST_ASSERT(addr + len <= buf_.size() / LBS);
    }
    template <typename Record>
    bool writeDiffTopHalf(const Record &rec) {
        verifyAddr(rec.io_address, rec.io_blocks);
        if (rec.isAllZero()) {
            writeAllZero(rec.io_address, rec.io_blocks);
            return true;
        }
        if (rec.isDiscard()) {
            discard(rec.io_address, rec.io_blocks);
            return true;
        }
        assert(rec.isNormal());
        return false;
    }
};

} // namespace walb
