#pragma once
#include "walb_diff_base.hpp"

namespace for_walb_diff_test_local {

cybozu::util::Random<size_t> *rand_ = nullptr;

} // namespace for_walb_diff_test_local


inline void setRandForTest(cybozu::util::Random<size_t>& rand)
{
    for_walb_diff_test_local::rand_ = &rand;
}

namespace walb {

enum class RecType : uint8_t
{
    NORMAL, DISCARD, ALLZERO,
};

inline std::ostream& operator<<(std::ostream& os, RecType type)
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

/**
 * Simple IO for test.
 */
struct Sio
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
    bool operator<(const Sio& rhs) const {
        return ioAddr < rhs.ioAddr;
    }
    bool operator==(const Sio& rhs) const {
        if (ioAddr != rhs.ioAddr) return false;
        if (ioBlocks != rhs.ioBlocks) return false;
        if (type != rhs.type) return false;
        if (type != RecType::NORMAL) return true;
        if (data.size() != rhs.data.size()) return false;
        return ::memcmp(data.data(), rhs.data.data(), data.size()) == 0;
    }
    bool operator!=(const Sio& rhs) const { return !(*this == rhs); }
    bool isOverlapped(const Sio& rhs) const {
        return cybozu::isOverlapped(ioAddr, ioBlocks, rhs.ioAddr, rhs.ioBlocks);
    }
    void setRandomly() {
        cybozu::util::Random<size_t> &rand_ = *for_walb_diff_test_local::rand_;

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
    bool tryMerge(const Sio& rhs) {
        if (ioAddr + ioBlocks != rhs.ioAddr || type != rhs.type) return false;
        ioBlocks += rhs.ioBlocks;
        if (type == RecType::NORMAL) {
            size_t oldSize = data.size();
            data.resize(oldSize + rhs.data.size());
            ::memcpy(data.data() + oldSize, rhs.data.data(), rhs.data.size());
        }
        return true;
    }
    friend inline std::ostream& operator<<(std::ostream& os, const Sio& sio) {
        os << sio.ioAddr << "\t" << sio.ioBlocks << "\t" << sio.type;
        const uint32_t csum =
            sio.type == RecType::NORMAL ? calcDiffIoChecksum(sio.data) : 0;
        os << cybozu::util::formatString("\t%08x", csum);
        return os;
    }
};

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

inline void removeOverlapped(std::list<Sio>& list)
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

inline std::list<Sio> generateSioList(size_t nr, bool mustBeSorted)
{
    std::list<Sio> list;
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

inline void mergeOrAddSioList(std::list<Sio>& list, Sio&& sio)
{
    if (list.empty() || !list.back().tryMerge(sio)) {
        list.push_back(std::move(sio));
    }
}

inline void compareSioList(const std::list<Sio>& lhs, const std::list<Sio>& rhs)
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

} // namespace walb
