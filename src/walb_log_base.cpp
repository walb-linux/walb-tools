#include "walb_log_base.hpp"

namespace walb {


std::string WlogRecord::str() const
{
    return cybozu::util::formatString(
        "wlog_r:"
        " lsid %10" PRIu64 " %4u"
        " io %10" PRIu64 " %4u"
        " flags %c%c%c"
        " csum %08x"
        , lsid, lsid_local
        , offset, io_size
        , isExist() ? 'E' : '-'
        , isPadding() ? 'P' : '-'
        , isDiscard() ? 'D' : '-'
        , checksum);
}


std::string WlogRecord::strDetail() const
{
    return cybozu::util::formatString(
        "wlog_record:\n"
        "  checksum: %08x(%u)\n"
        "  lsid: %" PRIu64 "\n"
        "  lsid_local: %u\n"
        "  is_exist: %u\n"
        "  is_padding: %u\n"
        "  is_discard: %u\n"
        "  offset: %" PRIu64 "\n"
        "  io_size: %u"
        , checksum, checksum
        , lsid, lsid_local
        , isExist()
        , isPadding()
        , isDiscard()
        , offset, io_size);
}


std::string LogPackHeader::strHead() const
{
    if (!header_) throw cybozu::Exception(__func__) << "header_ is not set";
    return cybozu::util::formatString(
        "wlog_p:"
        " lsid %10" PRIu64 ""
        " total %4u"
        " n_rec %2u"
        " n_pad %2u"
        " csum %08x"
        , header_->logpack_lsid
        , header_->total_io_size
        , header_->n_records
        , header_->n_padding
        , header_->checksum);
}


std::string LogPackHeader::strHeadDetail() const
{
    if (!header_) throw cybozu::Exception(__func__) << "header_ is not set";
    return cybozu::util::formatString(
        "wlog_pack_header:\n"
        "  checksum: %08x(%u)\n"
        "  n_records: %u\n"
        "  n_padding: %u\n"
        "  total_io_size: %u\n"
        "  logpack_lsid: %" PRIu64 "\n",
        header_->checksum, header_->checksum,
        header_->n_records,
        header_->n_padding,
        header_->total_io_size,
        header_->logpack_lsid);
}


std::string LogPackHeader::str() const
{
    StrVec v;
    v.push_back(strHead());
    for (size_t i = 0; i < nRecords(); i++) {
        v.push_back(record(i).str());
    }
    return cybozu::util::concat(v, "\n");
}


std::string LogPackHeader::strDetail() const
{
    StrVec v;
    v.push_back(strHeadDetail());
    for (size_t i = 0; i < nRecords(); i++) {
        v.push_back(record(i).strDetail());
    }
    return cybozu::util::concat(v, "\n");
}


bool LogPackHeader::addNormalIo(uint64_t offset, uint16_t size)
{
    if (::max_n_log_record_in_sector(pbs()) <= nRecords()) {
        return false;
    }
    if (MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER <
        totalIoSize() + ::capacity_pb(pbs(), size)) {
        return false;
    }
    if (size == 0) {
        throw cybozu::Exception("Normal IO can not be zero-sized.");
    }
    size_t pos = nRecords();
    struct walb_log_record &rec = header_->record[pos];
    rec.flags = 0;
    ::set_bit_u32(LOG_RECORD_EXIST, &rec.flags);
    rec.offset = offset;
    rec.io_size = size;
    rec.lsid_local = header_->total_io_size + 1;
    rec.lsid = header_->logpack_lsid + rec.lsid_local;
    rec.checksum = 0; /* You must set this lator. */

    header_->n_records++;
    header_->total_io_size += capacity_pb(pbs(), size);
    assert(::is_valid_logpack_header_and_records(header_));
    return true;
}


bool LogPackHeader::addDiscardIo(uint64_t offset, uint32_t size)
{
    if (::max_n_log_record_in_sector(pbs()) <= nRecords()) {
        return false;
    }
    if (size == 0) {
        throw cybozu::Exception("Discard IO can not be zero-sized.");
    }
    size_t pos = nRecords();
    struct walb_log_record &rec = header_->record[pos];
    rec.flags = 0;
    ::set_bit_u32(LOG_RECORD_EXIST, &rec.flags);
    ::set_bit_u32(LOG_RECORD_DISCARD, &rec.flags);
    rec.offset = offset;
    rec.io_size = size;
    rec.lsid_local = header_->total_io_size + 1;
    rec.lsid = header_->logpack_lsid + rec.lsid_local;
    rec.checksum = 0; /* Not be used. */

    header_->n_records++;
    /* You must not update total_io_size. */
    assert(::is_valid_logpack_header_and_records(header_));
    return true;
}


bool LogPackHeader::addPadding(uint16_t size)
{
    if (::max_n_log_record_in_sector(pbs()) <= nRecords()) {
        return false;
    }
    if (MAX_TOTAL_IO_SIZE_IN_LOGPACK_HEADER <
        totalIoSize() + ::capacity_pb(pbs(), size)) {
        return false;
    }
    if (0 < nPadding()) {
        return false;
    }
    if (size % ::n_lb_in_pb(pbs()) != 0) {
        throw cybozu::Exception("Padding size must be pbs-aligned.");
    }

    size_t pos = nRecords();
    struct walb_log_record &rec = header_->record[pos];
    rec.flags = 0;
    ::set_bit_u32(LOG_RECORD_EXIST, &rec.flags);
    ::set_bit_u32(LOG_RECORD_PADDING, &rec.flags);
    rec.offset = 0; /* will not be used. */
    rec.io_size = size;
    rec.lsid_local = header_->total_io_size + 1;
    rec.lsid = header_->logpack_lsid + rec.lsid_local;
    rec.checksum = 0;  /* will not be used. */

    header_->n_records++;
    header_->total_io_size += ::capacity_pb(pbs(), size);
    header_->n_padding++;
    assert(::is_valid_logpack_header_and_records(header_));
    return true;
}



void LogPackHeader::updateLsid(uint64_t newLsid)
{
    assert(isValid(false));
    assert(newLsid != uint64_t(-1));
    if (header_->logpack_lsid == newLsid) return;

    header_->logpack_lsid = newLsid;
    for (size_t i = 0; i < header_->n_records; i++) {
        struct walb_log_record &rec = record(i);
        rec.lsid = newLsid + rec.lsid_local;
    }
    verify(false);
}



uint64_t LogPackHeader::shrink(size_t invalidIdx)
{
    assert(invalidIdx < nRecords());

    /* Invalidate records. */
    for (size_t i = invalidIdx; i < nRecords(); i++) {
        ::log_record_init(&record(i));
    }

    /* Set n_records and total_io_size. */
    header_->n_records = invalidIdx;
    header_->total_io_size = 0;
    header_->n_padding = 0;
    for (size_t i = 0; i < nRecords(); i++) {
        struct walb_log_record &rec = record(i);
        if (!::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags)) {
            header_->total_io_size += ::capacity_pb(pbs(), rec.io_size);
        }
        if (::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)) {
            header_->n_padding++;
        }
    }

    updateChecksum();
    assert(isValid());

    return nextLogpackLsid();
}



size_t LogPackHeader::invalidate(size_t idx)
{
    assert(idx < nRecords());
    WlogRecord &rec = record(idx);

    if (rec.isDiscard()) {
        const size_t n = nRecords();
        for (size_t i = idx; i < n - 1; i++) {
            record(idx) = record(idx + 1);
        }
        record(n - 1).clear();
        header_->n_records--;
        assert(isValid(false));
        return idx;
    }
    if (rec.isPadding()) {
        // do nothing.
        return idx + 1;
    }
    rec.setPadding();
    rec.offset = 0;
    rec.checksum = 0;
    return idx + 1;
}


bool LogBlockShared::isAllZero(size_t ioSizeLb) const
{
    checkPbs();
    checkSizeLb(ioSizeLb);
    size_t remaining = ioSizeLb * LOGICAL_BLOCK_SIZE;
    size_t i = 0;
    while (0 < remaining) {
        const size_t s = std::min<size_t>(pbs_, remaining);
        if (!cybozu::util::isAllZero(get(i), s)) return false;
        remaining -= s;
        i++;
    }
    return true;
}


uint32_t LogBlockShared::calcChecksum(size_t ioSizeLb, uint32_t salt) const
{
    checkPbs();
    checkSizeLb(ioSizeLb);
    ChecksumCalculator cc(ioSizeLb, salt);
    for (size_t i = 0; i < nBlocks(); i++) {
        cc.update(get(i), pbs_);
    }
    return cc.get();
}



std::string LogBlockShared::str() const
{
    std::string ret = cybozu::util::formatString(
        "block_s: pbs %u sizePb %u", pbs_, data_.size());
#if 0
    std::vector<std::string> v;
    for (size_t i = 0;i < data_.size(); i++) {
        v.push_back(cybozu::util::toUnitIntString(getBlock(i).size()));
    }
    ret += " [" + cybozu::util::concat(v, ", ") + "]";
#endif
    return ret;
}


void verifyWlogChecksum(const WlogRecord& rec, const LogBlockShared& blockS, uint32_t salt)
{
    if (!rec.hasDataForChecksum()) return;
    const uint32_t checksum = blockS.calcChecksum(rec.io_size, salt);
    if (checksum != rec.checksum) {
        throw cybozu::Exception(__func__)
            << "invalid checksum" << checksum << rec.checksum;
    }
}


bool isWlogChecksumValid(const WlogRecord& rec, const LogBlockShared& blockS, uint32_t salt)
{
    try {
        verifyWlogChecksum(rec, blockS, salt);
        return true;
    } catch (std::exception &) {
        return false;
    }
}


void LogStatistics::init(uint64_t bgnLsid, uint64_t endLsid)
{
    nrPacks = 0;
    this->bgnLsid = bgnLsid;
    this->endLsid = endLsid;
    lsid = bgnLsid;
    normalLb = 0;
    discardLb = 0;
    paddingPb = 0;
}


void LogStatistics::update(const LogPackHeader &packH)
{
    for (size_t i = 0; i < packH.nRecords(); i++) {
        const WlogRecord &rec = packH.record(i);
        if (rec.isDiscard()) {
            discardLb += rec.io_size;
        } else if (rec.isPadding()) {
            paddingPb += rec.ioSizePb(packH.pbs());
        } else {
            normalLb += rec.io_size;
        }
    }
    nrPacks++;
    lsid = packH.nextLogpackLsid();
}


std::string LogStatistics::str() const
{
    return cybozu::util::formatString(
        "bgnLsid %" PRIu64 "\n"
        "endLsid %" PRIu64 "\n"
        "reallyEndLsid %" PRIu64 "\n"
        "nrPacks %" PRIu64 "\n"
        "normalLb %" PRIu64 "\n"
        "discardLb %" PRIu64 "\n"
        "paddingPb %" PRIu64 ""
        , bgnLsid, endLsid, lsid
        , nrPacks, normalLb, discardLb
        , paddingPb);
}


} // namespace walb
