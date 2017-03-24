#pragma once

#include <sstream>
#include "walb_diff_base.hpp"
#include "walb_diff_pack.hpp"

namespace walb {

/**
 * Set -1 of a field in order to invalidate the field in printing,
 */
struct DiffStatistics
{
    size_t wdiffNr; // Number of wdiff files.

    size_t normNr; // normal IOs.
    size_t zeroNr; // all zero IOs.
    size_t discNr; // discard IOs.

    uint64_t normLb; // normal IO total size. [logical block]
    uint64_t zeroLb; // all zero IO total size. [logical block]
    uint64_t discLb; // discard IO totalsize. [logical block]

    uint64_t dataSize; // total IO data size (compressed) [byte]

    constexpr static const char *NAME = "DiffStatistics";
    DiffStatistics() {
        clear();
    }
    void clear() {
        wdiffNr = 0;
        normNr = 0;
        zeroNr = 0;
        discNr = 0;
        normLb = 0;
        zeroLb = 0;
        discLb = 0;
        dataSize = 0;
    }
    void update(const DiffRecord& rec) {
        updateDetail(rec.isNormal(), rec.isDiscard(), rec.isAllZero(),
                     rec.io_blocks, rec.data_size, [&]() { return rec.toStr(); });
    }
    void update(const IndexedDiffRecord& rec) {
        // data size can not be updated here.
        // It will be set independently.
        updateDetail(rec.isNormal(), rec.isDiscard(), rec.isAllZero(),
                     rec.io_blocks, 0, [&]() { return rec.toStr(); });
    }
    void update(const DiffPackHeader& pack) {
        if (pack.isEnd()) return;
        for (size_t i = 0; i < pack.n_records; i++) {
            update(pack[i]);
        }
    }
    void update(const DiffStatistics& stat) {
        wdiffNr += stat.wdiffNr;
        normNr += stat.normNr;
        zeroNr += stat.zeroNr;
        discNr += stat.discNr;
        normLb += stat.normLb;
        zeroLb += stat.zeroLb;
        discLb += stat.discLb;
        dataSize += stat.dataSize;
    }
    void print(::FILE *f = ::stdout, const char *prefix = "") const {
        ::fprintf(f,
                  "%s""wdiffNr %s\n"
                  "%s""normalNr %s\n"
                  "%s""allZeroNr %s\n"
                  "%s""discardNr %s\n"
                  "%s""normalLb %s\n"
                  "%s""allZeroLb %s\n"
                  "%s""discardLb %s\n"
                  "%s""dataSize %s\n"
                  , prefix, toStr(wdiffNr).c_str()
                  , prefix, toStr(normNr).c_str()
                  , prefix, toStr(zeroNr).c_str()
                  , prefix, toStr(discNr).c_str()
                  , prefix, toStr(normLb).c_str()
                  , prefix, toStr(zeroLb).c_str()
                  , prefix, toStr(discLb).c_str()
                  , prefix, toStr(dataSize).c_str());
    }
    void printOneline(::FILE *f = ::stdout, const char *prefix = "") const {
        std::stringstream ss;
        ss << *this;
        ::fprintf(f, "%s%s\n", prefix, ss.str().c_str());
    }
    friend inline std::ostream& operator<<(std::ostream& os, const DiffStatistics& stat) {
        os << "wdiffNr " << toStr(stat.wdiffNr)
           << "  nrN " << toStr(stat.normNr)
           << " nrZ " << toStr(stat.zeroNr)
           << " nrD " << toStr(stat.discNr)
           << "  lbN " << toStr(stat.normLb)
           << " lbZ " << toStr(stat.zeroLb)
           << " lbD " << toStr(stat.discLb)
           << "  dataSize " << toStr(stat.dataSize);
        return os;
    }
    template <typename Uint>
    static std::string toStr(Uint i) {
        if (i == Uint(-1)) {
            return "---";
        } else {
            return cybozu::itoa(i);
        }
    }
private:
    void updateDetail(bool isNormal, bool isDiscard, bool isAllZero,
                      size_t ioBlocks, size_t dataSize,
                      std::function<std::string()> recStrGen) {
        if (isNormal) {
            normNr++;
            normLb += ioBlocks;
        } else if (isDiscard) {
            discNr++;
            discLb += ioBlocks;
        } else if (isAllZero) {
            zeroNr++;
            zeroLb += ioBlocks;
        } else {
            throw cybozu::Exception(NAME) << "invalid record" << recStrGen();
        }
        this->dataSize += dataSize;
    }
};

} // namespace walb
