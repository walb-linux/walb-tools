#pragma once
/**
 * @file
 * @brief Walb log utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <cassert>
#include <memory>
#include <cstdlib>
#include <functional>
#include <type_traits>

#include "util.hpp"
#include "checksum.hpp"
#include "fileio.hpp"
#include "walb_logger.hpp"
#include "walb/super.h"
#include "walb/log_device.h"
#include "walb/log_record.h"
#include "walb_log.h"
#include "backtrace.hpp"
#include "walb_util.hpp"

namespace walb {

struct WlogRecord : public walb_log_record
{
    uint64_t packLsid() const { return lsid - lsid_local; }
    bool isExist() const {
        return ::test_bit_u32(LOG_RECORD_EXIST, &flags);
    }
    bool isPadding() const {
        return ::test_bit_u32(LOG_RECORD_PADDING, &flags);
    }
    bool isDiscard() const {
        return ::test_bit_u32(LOG_RECORD_DISCARD, &flags);
    }
    bool hasData() const {
        return isExist() && !isDiscard();
    }
    bool hasDataForChecksum() const {
        return isExist() && !isDiscard() && !isPadding();
    }
    void setExist() {
        ::set_bit_u32(LOG_RECORD_EXIST, &flags);
    }
    void setPadding() {
        ::set_bit_u32(LOG_RECORD_PADDING, &flags);
    }
    void setDiscard() {
        ::set_bit_u32(LOG_RECORD_DISCARD, &flags);
    }
    void clearExist() {
        ::clear_bit_u32(LOG_RECORD_EXIST, &flags);
    }
    void clearPadding() {
        ::clear_bit_u32(LOG_RECORD_PADDING, &flags);
    }
    void clearDiscard() {
        ::clear_bit_u32(LOG_RECORD_DISCARD, &flags);
    }
    bool isValid() const { return ::is_valid_log_record_const(this); }
    void verify() const {
        if (!isValid()) throw cybozu::Exception("WlogRecord:verify");
    }
    uint32_t ioSizePb(uint32_t pbs) const {
        return ::capacity_pb(pbs, io_size);
    }
    uint32_t ioSizeLb() const { return io_size; }

    std::string str() const {
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
    std::string strDetail() const {
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
    friend inline std::ostream& operator<<(std::ostream& os, const WlogRecord& rec) {
        os << rec.str();
        return os;
    }
    void clear() {
        ::memset(this, 0, sizeof(*this));
    }
};

/**
 * log pack header
 */
class LogPackHeader
{
    AlignedArray block_;
    walb_logpack_header *header_;
    uint32_t pbs_;
    uint32_t salt_;
public:
    static constexpr const char *NAME() { return "LogPackHeader"; }
    LogPackHeader(const void *header = 0, uint32_t pbs = 0, uint32_t salt = 0)
        : header_((walb_logpack_header*)header), pbs_(pbs), salt_(salt) {
    }
    LogPackHeader(AlignedArray &&block, uint32_t pbs, uint32_t salt)
        : header_(nullptr), pbs_(pbs), salt_(salt) {
        setBlock(std::move(block));
    }
    LogPackHeader(uint32_t pbs, uint32_t salt)
        : header_(nullptr), pbs_(pbs), salt_(salt) {
        setBlock(AlignedArray(pbs, true));
    }
    const walb_logpack_header &header() const { checkBlock(); return *header_; }
    walb_logpack_header &header() { checkBlock(); return *header_; }
    uint32_t pbs() const { return pbs_; }
    uint32_t salt() const { return salt_; }
    void setPbs(uint32_t pbs) { pbs_ = pbs; }
    void setSalt(uint32_t salt) { salt_ = salt; }
    void setBlock(AlignedArray &&block) {
        block_ = std::move(block);
        header_ = (walb_logpack_header*)block_.data();
    }
    void init(uint32_t pbs, uint32_t salt) {
        setPbs(pbs);
        setSalt(salt);
        setBlock(AlignedArray(pbs_, true));
    }

    /*
     * Fields.
     */
    uint32_t checksum() const { return header_->checksum; }
    uint16_t sectorType() const { return header_->sector_type; }
    uint16_t totalIoSize() const { return header_->total_io_size; }
    uint64_t logpackLsid() const { return header_->logpack_lsid; }
    uint16_t nRecords() const { return header_->n_records; }
    uint16_t nPadding() const { return header_->n_padding; }

    /*
     * Utilities.
     */
    const uint8_t *rawData() const { return (const uint8_t*)header_; }
    const WlogRecord &record(size_t pos) const {
        checkIndexRange(pos);
        return static_cast<const WlogRecord &>(header_->record[pos]);
    }
    struct WlogRecord &record(size_t pos) {
        checkIndexRange(pos);
        return static_cast<WlogRecord &>(header_->record[pos]);
    }
    uint64_t nextLogpackLsid() const {
        if (nRecords() > 0) {
            return logpackLsid() + 1 + totalIoSize();
        } else {
            return logpackLsid();
        }
    }
    bool isEnd() const {
        return nRecords() == 0 && logpackLsid() == uint64_t(-1);
    }
    bool isValid(bool isChecksum = true) const {
        if (!header_) return false;
        if (pbs() == 0) return false;
        if (isChecksum) {
            return ::is_valid_logpack_header_and_records_with_checksum(header_, pbs(), salt()) != 0;
        } else {
            return ::is_valid_logpack_header_and_records(header_) != 0;
        }
    }
    void verify(bool isChecksum = true) const {
        if (!isValid(isChecksum)) throw cybozu::Exception(NAME()) << "invalid" << *this;
    }
    void copyFrom(const void *data, size_t size) {
        if (pbs_ != size) {
            throw cybozu::Exception(NAME()) << "invalid size" << size << pbs_;
        }
        ::memcpy(header_, data, size);
        verify();
    }
    void copyFrom(const LogPackHeader &rhs) {
        if (pbs_ != rhs.pbs() || salt_ != rhs.salt()) {
            init(rhs.pbs(), rhs.salt());
        }
        copyFrom(rhs.rawData(), rhs.pbs());
    }
    size_t nRecordsHavingData() const {
        size_t s = 0;
        for (size_t i = 0; i < nRecords(); i++) {
            if (record(i).hasData()) {
                s++;
            }
        }
        return s;
    }
    /*
     * to string.
     */
    std::string strHead() const {
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
    std::string strHeadDetail() const {
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
    std::string str() const {
        StrVec v;
        v.push_back(strHead());
        for (size_t i = 0; i < nRecords(); i++) {
            v.push_back(record(i).str());
        }
        return cybozu::util::concat(v, "\n");
    }
    std::string strDetail() const {
        StrVec v;
        v.push_back(strHeadDetail());
        for (size_t i = 0; i < nRecords(); i++) {
            v.push_back(record(i).strDetail());
        }
        return cybozu::util::concat(v, "\n");
    }
    friend inline std::ostream& operator<<(std::ostream& os, const LogPackHeader &packH) {
        os << packH.str();
        return os;
    }
    /**
     * Update checksum field.
     */
    void updateChecksum() {
        header_->checksum = 0;
        header_->checksum = ::checksum((const uint8_t*)header_, pbs(), salt());
    }

    template <typename Reader>
    void rawReadFrom(Reader &reader) {
        reader.read(header_, pbs_);
    }
    /**
     * RETURN:
     *   false if read failed or invalid.
     */
    template <typename Reader>
    bool readFrom(Reader &reader) {
        try {
            rawReadFrom<Reader>(reader);
        } catch (std::exception &e) {
            LOGs.debug() << NAME() << e.what();
            return false;
        }
        return isValid(true);
    }
    /**
     * Write the logpack header block.
     */
    template <typename Writer>
    void updateChecksumAndWriteTo(Writer &writer) {
        updateChecksum();
        writeTo(writer);
    }

    template <typename Writer>
    void writeTo(Writer &writer) const {
        if (!isValid(true)) {
            throw cybozu::Exception(NAME()) << "write:invalid";
        }
        writer.write(header_, pbs());
    }

    /**
     * Initialize logpack header block.
     */
    void init(uint64_t lsid) {
        ::memset(header_, 0, pbs());
        header_->logpack_lsid = lsid;
        header_->sector_type = SECTOR_TYPE_LOGPACK;
        /*
          header_->total_io_size = 0;
          header_->n_records = 0;
          header_->n_padding = 0;
          header_->checksum = 0;
        */
    }
    /**
     * Make the header block terminator.
     * Do not forget to call updateChecksum() before write it.
     */
    void setEnd() {
        init(uint64_t(-1));
    }
    /**
     * Add a normal IO.
     *
     * @offset [logical block]
     * @size [logical block]
     *   can not be 0.
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addNormalIo(uint64_t offset, uint16_t size) {
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
    /**
     * Add a discard IO.
     *
     * @offset [logical block]
     * @size [logical block]
     *   can not be 0.
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addDiscardIo(uint64_t offset, uint16_t size) {
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
    /**
     * Add a padding.
     *
     * @size [logical block]
     *   can be 0.
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addPadding(uint16_t size) {
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
    /**
     * Update all lsid entries in the logpack header.
     * Checksum will not be updated.
     *
     * @newLsid new logpack lsid.
     *   If -1, nothing will be changed.
     *
     * RETURN:
     *   true in success.
     *   false if lsid overlap ocurred.
     */
    void updateLsid(uint64_t newLsid) {
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
    /**
     * Shrink.
     * Delete records from rec[invalidIdx] to the last.
     * If you specify invalidIdx = 0, then the pack will be empty.
     * RETURN:
     *   next logpack lsid.
     */
    uint64_t shrink(size_t invalidIdx) {
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
    /**
     * Invalidate a record.
     * If the record is discard, really remove it from the header.
     * If the record is normal, make it padding.
     * This will not change total_io_size.
     *
     * You must call updateChecksum() by yourself after calling this.
     *
     * RETURN:
     *   index of next item.
     */
    size_t invalidate(size_t idx) {
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
protected:
    void checkIndexRange(size_t pos) const {
        if (pos >= nRecords()) {
            throw cybozu::Exception("LogPackHeader:index out of range.") << pos;
        }
    }
    void checkBlock() const {
        if (header_ == nullptr) {
            throw cybozu::Exception("LogPackHeader:header is null.");
        }
    }
};

struct ChecksumCalculator {
    size_t remaining;
    uint32_t csum;
    ChecksumCalculator(size_t ioSizeLb, uint32_t salt) : remaining(ioSizeLb * LOGICAL_BLOCK_SIZE), csum(salt) {}
    void update(const char *data, size_t size)
    {
        const size_t s = std::min<size_t>(size, remaining);
        csum = cybozu::util::checksumPartial(data, s, csum);
        remaining -= s;
    }
    uint32_t get() const {
        if (remaining) throw cybozu::Exception("ChecksumCalculator") << remaining;
        return cybozu::util::checksumFinish(csum);
    }
};

/**
 * Helper class to manage multiple IO blocks.
 * This is copyable and movable.
 */
class LogBlockShared
{
private:
    static constexpr const char *NAME() { return "LogBlockShared"; }
    uint32_t pbs_; /* physical block size. */
    std::vector<AlignedArray> data_; /* Each block's size must be pbs. */
public:
    explicit LogBlockShared(uint32_t pbs = 0) : pbs_(pbs) {}
    void init(size_t pbs) {
        pbs_ = pbs;
        checkPbs();
        resize(0);
    }
    size_t nBlocks() const { return data_.size(); }
    const char *get(size_t idx) const {
        checkRange(idx);
        return data_[idx].data();
    }
    char *get(size_t idx) {
        checkRange(idx);
        return data_[idx].data();
    }
    uint32_t pbs() const {
        return pbs_;
    }
    void clear() {
        pbs_ = 0;
        data_.clear();
    }
    void resize(size_t nBlocks0) {
        if (nBlocks0 < data_.size()) {
            data_.resize(nBlocks0);
        }
        while (data_.size() < nBlocks0) {
            data_.emplace_back(pbs_, false);
        }
    }
    void addBlock(AlignedArray &&block) {
        data_.push_back(std::move(block));
    }
    AlignedArray& getBlock(size_t idx) { return data_[idx]; }
    const AlignedArray& getBlock(size_t idx) const { return data_[idx]; }
    template <typename Reader>
    void read(Reader &reader, uint32_t nBlocks0) {
        checkPbs();
        resize(nBlocks0);
        for (size_t i = 0; i < nBlocks0; i++) {
            reader.read(get(i), pbs_);
        }
    }
    template <typename Writer>
    void write(Writer &writer) const {
        checkPbs();
        for (size_t i = 0; i < nBlocks(); i++) {
            writer.write(get(i), pbs_);
        }
    }
    bool isAllZero(size_t ioSizeLb) const {
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
    uint32_t calcChecksum(size_t ioSizeLb, uint32_t salt) const {
        checkPbs();
        checkSizeLb(ioSizeLb);
        ChecksumCalculator cc(ioSizeLb, salt);
        for (size_t i = 0; i < nBlocks(); i++) {
            cc.update(get(i), pbs_);
        }
        return cc.get();
    }
    std::string str() const {
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
    friend inline std::ostream& operator<<(std::ostream& os, const LogBlockShared &blockS) {
        os << blockS.str();
        return os;
    }
private:
    void checkPbs() const {
        if (pbs_ == 0) throw cybozu::Exception(NAME()) << "pbs is zero";
    }
    void checkSizeLb(size_t ioSizeLb) const {
        if (nBlocks() * pbs_ < ioSizeLb * LOGICAL_BLOCK_SIZE) {
            throw cybozu::Exception(NAME())
                << "checkSizeLb:ioSizeLb too large"
                << ioSizeLb << ::capacity_lb(pbs_, nBlocks());
        }
    }
    void checkRange(size_t idx) const {
        checkPbs();
        if (nBlocks() <= idx) {
            throw cybozu::Exception(NAME()) << "index out of range" << idx;
        }
    }
};

inline void verifyWlogChecksum(const WlogRecord& rec, const LogBlockShared& blockS, uint32_t salt)
{
    if (!rec.hasDataForChecksum()) return;
    const uint32_t checksum = blockS.calcChecksum(rec.io_size, salt);
    if (checksum != rec.checksum) {
        throw cybozu::Exception(__func__)
            << "invalid checksum" << checksum << rec.checksum;
    }
}

inline bool isWlogChecksumValid(const WlogRecord& rec, const LogBlockShared& blockS, uint32_t salt)
{
    try {
        verifyWlogChecksum(rec, blockS, salt);
        return true;
    } catch (std::exception &) {
        return false;
    }
}

template <typename Reader>
inline bool readLogPackHeader(Reader &reader, LogPackHeader &packH, uint64_t lsid = uint64_t(-1))
{
    if (!packH.readFrom(reader)) {
        return false; // invalid header.
    }
    if (lsid != uint64_t(-1) && packH.header().logpack_lsid != lsid) {
        return false; // invalid, or an end block.
    }
    return true;
}

template <typename Reader>
inline bool readLogIo(Reader &reader, const LogPackHeader &packH, size_t idx, LogBlockShared &blockS)
{
    const WlogRecord &lrec = packH.record(idx);
    if (!lrec.hasData()) return true;

    const uint32_t pbs = packH.pbs();
    const size_t ioSizePb = lrec.ioSizePb(pbs);
    if (blockS.pbs() != pbs) blockS.init(pbs);
    blockS.read(reader, ioSizePb);
    return isWlogChecksumValid(lrec, blockS, packH.salt());
}

/**
 * Read all lob IOs corresponding to a logpack.
 * PackH will be shrinked (and may be empty) if a read IO data is invalid.
 * If not shrinked, packH will not be changed.
 *
 * RETURN:
 *   true if not shrinked.
 */
template <typename Reader>
inline bool readAllLogIos(
    Reader &reader, LogPackHeader &packH, std::queue<LogBlockShared> &ioQ,
    bool doShrink = true)
{
    bool isNotShrinked = true;
    for (size_t i = 0; i < packH.nRecords(); i++) {
        const WlogRecord &rec = packH.record(i);
        if (!rec.hasData()) continue;

        LogBlockShared blockS;
        const bool valid = readLogIo(reader, packH, i, blockS);
        if (doShrink && !valid) {
            packH.shrink(i);
            isNotShrinked = false;
            break;
        }
        ioQ.push(std::move(blockS));
    }
    assert(packH.nRecordsHavingData() == ioQ.size());
    return isNotShrinked;
}

/**
 * Skip to read log IOs corresponding to a logpack.
 * This does not verify the log IO data, or really seek in a walb log device.
 */
template <typename Reader>
inline void skipAllLogIos(Reader &reader, const LogPackHeader &packH)
{
    reader.skip(packH.totalIoSize() * packH.pbs());
}

/**
 * Statistics.
 */
struct LogStatistics
{
    uint64_t nrPacks;
    uint64_t bgnLsid;
    uint64_t endLsid;
    uint64_t lsid;
    uint64_t normalLb;
    uint64_t discardLb;
    uint64_t paddingPb;

    void init(uint64_t bgnLsid, uint64_t endLsid) {
        nrPacks = 0;
        this->bgnLsid = bgnLsid;
        this->endLsid = endLsid;
        lsid = bgnLsid;
        normalLb = 0;
        discardLb = 0;
        paddingPb = 0;
    }
    void update(const LogPackHeader &packH) {
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
    std::string str() const {
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
    friend inline std::ostream& operator<<(std::ostream& os, const LogStatistics &logStat) {
        os << logStat.str();
        return os;
    }
};

} // walb
