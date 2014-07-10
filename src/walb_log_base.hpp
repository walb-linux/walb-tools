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

struct LogRecord : public walb_log_record
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
        if (!isValid()) throw cybozu::Exception("LogRecord:verify");
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
            , isExist() ? 'E' : '_'
            , isPadding() ? 'P' : '_'
            , isDiscard() ? 'D' : '_'
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
    friend inline std::ostream& operator<<(std::ostream& os, const LogRecord& rec) {
        os << rec.str();
        return os;
    }
};

namespace log {

// QQQ: DEPRECATED.
class InvalidIo : public std::exception
{
public:
    const char *what() const noexcept override { return "invalid logpack IO."; }
};

} // log

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
        setBlock(AlignedArray(pbs));
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
        setBlock(AlignedArray(pbs_));
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
    const LogRecord &record(size_t pos) const {
        checkIndexRange(pos);
        return static_cast<const LogRecord &>(header_->record[pos]);
    }
    struct LogRecord &record(size_t pos) {
        checkIndexRange(pos);
        return static_cast<LogRecord &>(header_->record[pos]);
    }
    uint64_t nextLogpackLsid() const {
        if (nRecords() > 0) {
            return logpackLsid() + 1 + totalIoSize();
        } else {
            return logpackLsid();
        }
    }
    uint64_t totalPaddingPb() const {
        if (nPadding() == 0) {
            return 0;
        }
        uint64_t t = 0;
        for (size_t i = 0; i < nRecords(); i++) {
            const LogRecord &rec = record(i);
            if (rec.isPadding()) {
                t += ::capacity_pb(pbs(), rec.io_size);
            }
        }
        return t;
    }
    bool isEnd() const {
        return nRecords() == 0 && logpackLsid() == uint64_t(-1);
    }
    bool isValid(bool isChecksum = true) const {
        if (isChecksum) {
            return ::is_valid_logpack_header_and_records_with_checksum(header_, pbs(), salt()) != 0;
        } else {
            return ::is_valid_logpack_header_and_records(header_) != 0;
        }
    }
    void verify() const {
        if (!isValid(true)) throw cybozu::Exception(NAME()) << "invalid";
    }
    void copyFrom(const void *data, size_t size)
    {
        if (pbs_ != size) {
            throw cybozu::Exception(NAME()) << "invalid size" << size << pbs_;
        }
        ::memcpy(header_, data, size);
        verify();
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

    /**
     * RETURN:
     *   false if read failed or invalid.
     */
    template <typename Reader>
    bool readFrom(Reader &reader) {
        try {
            reader.read(header_, pbs_);
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
     *
     * @newLsid new logpack lsid.
     *   If -1, nothing will be changed.
     *
     * RETURN:
     *   true in success.
     *   false if lsid overlap ocurred.
     */
    bool updateLsid(uint64_t newLsid) {
        assert(isValid(false));
        if (newLsid == uint64_t(-1)) {
            return true;
        }
        if (header_->logpack_lsid == newLsid) {
            return true;
        }

        header_->logpack_lsid = newLsid;
        for (size_t i = 0; i < header_->n_records; i++) {
            struct walb_log_record &rec = record(i);
            rec.lsid = newLsid + rec.lsid_local;
        }
        return isValid(false);
    }
    /**
     * Shrink.
     * Delete records from rec[invalidIdx] to the last.
     * If you specify invalidIdx = 0, then the pack header will be set end.
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
            data_.emplace_back(pbs_);
        }
    }
    void addBlock(AlignedArray &&block) {
        data_.push_back(std::move(block));
    }
    AlignedArray& getBlock(size_t idx) { return data_[idx]; }
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
    bool calcIsAllZero(size_t ioSizeLb) const {
        checkPbs();
        checkSizeLb(ioSizeLb);
        size_t remaining = ioSizeLb * LOGICAL_BLOCK_SIZE;
        size_t i = 0;
        while (0 < remaining) {
            const size_t s = std::min<size_t>(pbs_, remaining);
            if (!cybozu::util::calcIsAllZero(get(i), s)) return false;
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

namespace log {

inline void verifyLogChecksum(const LogRecord& rec, const LogBlockShared& blockS, uint32_t salt)
{
    if (!rec.hasDataForChecksum()) return;
    const uint32_t checksum = blockS.calcChecksum(rec.io_size, salt);
    if (checksum != rec.checksum) {
        throw cybozu::Exception(__func__)
            << "invalid checksum" << checksum << rec.checksum;
    }
}

inline bool isLogChecksumValid(const LogRecord& rec, const LogBlockShared& blockS, uint32_t salt)
{
    try {
        verifyLogChecksum(rec, blockS, salt);
        return true;
    } catch (std::exception &) {
        return false;
    }
}

} // walb::log

/**
 * Logpack record and IO data.
 *
 * QQQ: DEPRECATED.
 */
struct LogPackIo
{
private:
    uint32_t salt;
public:
    LogRecord rec;
    LogBlockShared blockS;

    void set(const LogPackHeader &logh, size_t pos) {
        rec = logh.record(pos);
        salt = logh.salt();
        blockS.init(logh.pbs());
    }

    bool isValid(bool isChecksum = true) const {
        if (!rec.isValid()) return false;
        if (!isChecksum) return true;
        return log::isLogChecksumValid(rec, blockS, salt);
    }
    uint32_t calcIoChecksumWithZeroSalt() const {
        return blockS.calcChecksum(rec.ioSizeLb(), 0);
    }
    uint32_t calcIoChecksum() const {
        return blockS.calcChecksum(rec.ioSizeLb(), salt);
    }
};

template <typename Reader>
inline bool readLogPackHeader(Reader &reader, LogPackHeader &packH, uint64_t lsid)
{
    if (!packH.readFrom(reader)) {
        return false; // invalid header.
    }
    if (packH.header().logpack_lsid != lsid) {
        return false; // also invalid.
    }
    return true;
}

template <typename Reader>
inline bool readLogIo(Reader &reader, const LogPackHeader &packH, size_t idx, LogBlockShared &blockS)
{
    const LogRecord &lrec = packH.record(idx);
    if (!lrec.hasData()) return true;

    const uint32_t pbs = packH.pbs();
    const size_t ioSizePb = lrec.ioSizePb(pbs);
    if (blockS.pbs() != pbs) blockS.init(pbs);
    blockS.read(reader, ioSizePb);
    return log::isLogChecksumValid(lrec, blockS, packH.salt());
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
inline bool readAllLogIos(Reader &reader, LogPackHeader &packH, std::queue<LogBlockShared> &ioQ)
{
    bool isNotShrinked = true;
    for (size_t i = 0; i < packH.nRecords(); i++) {
        const LogRecord &rec = packH.record(i);
        if (!rec.hasData()) continue;

        LogBlockShared blockS;
        if (!readLogIo(reader, packH, i, blockS)) {
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

} // walb
