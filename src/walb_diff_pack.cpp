#include "walb_diff_pack.hpp"

namespace walb {

void DiffPackHeader::verify() const
{
    const char *const NAME = "DiffPackHeader";
    if (cybozu::util::calcChecksum(data(), size(), 0) != 0) {
        throw cybozu::Exception(NAME) << "invalid checksum";
    }
    if (n_records > MAX_N_RECORDS_IN_WALB_DIFF_PACK) {
        throw cybozu::Exception(NAME) << "invalid n_recoreds" << n_records;
    }
    for (size_t i = 0; i < n_records; i++) {
        (*this)[i].verify();
    }
}

void DiffPackHeader::print(::FILE *fp) const
{
    ::fprintf(fp,
              "checksum: %08x\n"
              "n_records: %u\n"
              "total_size: %u\n"
              "isEnd: %d\n"
              , checksum
              , n_records
              , total_size
              , isEnd());
    for (size_t i = 0; i < n_records; i++) {
        ::fprintf(fp, "record %zu: ", i);
        (*this)[i].printOneline(fp);
    }
}

std::string DiffPackHeader::toStr() const
{
    std::stringstream ss;
    ss << cybozu::util::formatString(
        "checksum: %08x\n" "n_records: %u\n" "total_size: %u\n" "isEnd: %d\n"
        , checksum, n_records, total_size, isEnd());
    for (size_t i = 0; i < n_records; i++) {
        ss << (*this)[i] << "\n";
    }
    return ss.str();
}

bool DiffPackHeader::canAdd(uint32_t dataSize) const
{
    if (MAX_N_RECORDS_IN_WALB_DIFF_PACK <= n_records) {
        return false;
    }
    if (0 < n_records && WALB_DIFF_PACK_MAX_SIZE < total_size + dataSize) {
        return false;
    }
    return true;
}

bool DiffPackHeader::add(const DiffRecord &inRec)
{
#ifdef DEBUG
    assert(inRec.isValid());
#endif
    if (!canAdd(inRec.data_size)) return false;
    DiffRecord &outRec = (*this)[n_records];
    outRec = inRec;
    assert(::memcmp(&outRec, &inRec, sizeof(outRec)) == 0);
    outRec.data_offset = total_size;
    n_records++;
    total_size += inRec.data_size;
    assert(outRec.data_size == inRec.data_size);
    return true;
}

void MemoryDiffPack::reset(const char *p, size_t size)
{
    if (!p) throw std::runtime_error("The pointer must not be null.");
    p_ = p;
    const size_t observed = ::WALB_DIFF_PACK_SIZE + header().total_size;
    if (size != observed) {
        throw cybozu::Exception(__func__) << "invalid pack size" << observed << size;
    }
    size_ = size;
}

void MemoryDiffPack::verify(bool doChecksum) const
{
    const char *const NAME = "MemoryDiffPack";
    header().verify();
    for (size_t i = 0; i < header().n_records; i++) {
        const DiffRecord& rec = header()[i];
        if (offset(i) + rec.data_size > size_) {
            throw cybozu::Exception(NAME)
                << "data_size out of range" << i << rec.data_size;
        }
        if (!doChecksum || !rec.isNormal()) continue;
        const uint32_t csum = cybozu::util::calcChecksum(data(i), rec.data_size, 0);
        if (csum != rec.checksum) {
            throw cybozu::Exception(NAME)
                << "invalid checksum" << csum << rec.checksum;
        }
    }
}

bool DiffPacker::add(uint64_t ioAddr, uint32_t ioBlocks, const char *data)
{
    assert(ioBlocks != 0);
    assert(ioBlocks <= UINT16_MAX);
    uint32_t dSize = ioBlocks * LOGICAL_BLOCK_SIZE;
    if (!pack_->canAdd(dSize)) return false;

    bool isZero = isAllZero(data, dSize);
    DiffRecord rec;
    rec.io_address = ioAddr;
    rec.io_blocks = ioBlocks;
    rec.compression_type = ::WALB_DIFF_CMPR_NONE;
    rec.checksum = 0; // not calculated.
    if (isZero) {
        rec.setAllZero();
        rec.data_size = 0;
    } else {
        rec.setNormal();
        rec.data_size = dSize;
    }
    return add(rec, data);
}

bool DiffPacker::add(const DiffRecord &rec, const char *data)
{
    assert(rec.isValid());
    const uint32_t dSize = rec.data_size;
    if (!pack_->canAdd(dSize)) return false;

    bool isNormal = rec.isNormal();
    /* r must be true because we called canAdd() before. */
    bool r = pack_->add(rec);
    if (r && isNormal) extendAndCopy(data, dSize);
    return r;
}

bool DiffPacker::empty() const
{
    bool ret = pack_->n_records == 0;
    if (ret) {
        assert(abuf_.size() == 1);
        assert(abuf_[0].size() == ::WALB_DIFF_PACK_SIZE);
    }
    return ret;
}

} //namespace walb
