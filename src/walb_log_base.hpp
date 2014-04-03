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
#include "memory_buffer.hpp"
#include "walb_logger.hpp"
#include "walb/super.h"
#include "walb/log_device.h"
#include "walb/log_record.h"
#include "walb_log.h"
#include "backtrace.hpp"

namespace walb {

template<class T>
struct LogRecord : walb_log_record
{
public:
//    uint64_t lsid() const { return lsid; }
    uint16_t lsidLocal() const { return lsid_local; }
//    uint32_t checksum() const { return checksum; }
//    uint64_t offset() const { return offset; }
    uint64_t packLsid() const { return lsid - lsidLocal(); }
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
    unsigned int ioSizeLb() const { return io_size; }
    unsigned int ioSizePb() const { return ::capacity_pb(static_cast<const T&>(*this).pbs(), ioSizeLb()); }
    bool isValid() const { return ::is_valid_log_record_const(this); }

    virtual void print(::FILE *fp = ::stdout) const {
        printLogRecord(fp, static_cast<const T&>(*this).pos(), this);
    }
    virtual void printOneline(::FILE *fp = ::stdout) const {
        printLogRecordOneline(fp, static_cast<const T&>(*this).pos(), this);
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
};

namespace log {

class InvalidIo : public std::exception
{
public:
    const char *what() const noexcept override { return "invalid logpack IO."; }
};

inline void printRecord(
    ::FILE *fp, size_t idx, const struct walb_log_record &rec)
{
    ::fprintf(fp,
              "record %zu\n"
              "  checksum: %08x(%u)\n"
              "  lsid: %" PRIu64 "\n"
              "  lsid_local: %u\n"
              "  is_exist: %u\n"
              "  is_padding: %u\n"
              "  is_discard: %u\n"
              "  offset: %" PRIu64 "\n"
              "  io_size: %u\n"
              , idx
              , rec.checksum, rec.checksum
              , rec.lsid, rec.lsid_local
              , ::test_bit_u32(LOG_RECORD_EXIST, &rec.flags)
              , ::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)
              , ::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags)
              , rec.offset, rec.io_size);
}

inline void printRecordOneline(
    ::FILE *fp, size_t idx, const struct walb_log_record &rec)
{
    ::fprintf(fp,
              "wlog_rec %2zu:\t"
              "lsid %" PRIu64 " %u\tio %10" PRIu64 " %4u\t"
              "flags %u%u%u\tcsum %08x %u\n"
              , idx
              , rec.lsid, rec.lsid_local
              , rec.offset, rec.io_size
              , ::test_bit_u32(LOG_RECORD_EXIST, &rec.flags)
              , ::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)
              , ::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags)
              , rec.checksum, rec.checksum);
}

} // log

/**
 * log pack header
 */
class LogPackHeader
{
    using Block = std::shared_ptr<uint8_t>;
    Block block_;
    walb_logpack_header *header_;
    unsigned int pbs_;
    uint32_t salt_;
public:
    LogPackHeader(const void *header = 0, unsigned int pbs = 0, uint32_t salt = 0)
        : header_((walb_logpack_header*)header), pbs_(pbs), salt_(salt) {
    }
    LogPackHeader(const Block &block, unsigned int pbs, uint32_t salt)
        : header_(nullptr), pbs_(pbs), salt_(salt) {
        setBlock(block);
    }
    const walb_logpack_header &header() const { checkBlock(); return *header_; }
    walb_logpack_header &header() { checkBlock(); return *header_; }
    unsigned int pbs() const { return pbs_; }
    uint32_t salt() const { return salt_; }
    void setPbs(unsigned int pbs) { pbs_ = pbs; }
    void setSalt(uint32_t salt) { salt_ = salt; }
    void setBlock(const Block &block) {
        block_ = block;
        header_ = (walb_logpack_header*)block_.get();
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
    const struct walb_log_record &record(size_t pos) const {
        checkIndexRange(pos);
        return header_->record[pos];
    }
    struct walb_log_record& record(size_t pos) {
        checkIndexRange(pos);
        return header_->record[pos];
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
            const struct walb_log_record &rec = record(i);
            if (::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)) {
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
    void copyFrom(const void *data, size_t size)
    {
        // QQQ veryf size
        memcpy(header_, data, size);
    }

    /*
     * Print.
     */
    void printRecord(size_t pos, ::FILE *fp = ::stdout) const {
        const struct walb_log_record &rec = record(pos);
        log::printRecord(fp, pos, rec);
    }
    void printRecordOneline(size_t pos, ::FILE *fp = ::stdout) const {
        const struct walb_log_record &rec = record(pos);
        log::printRecordOneline(fp, pos, rec);
    }
    void printHeader(::FILE *fp = ::stdout) const {
        ::fprintf(fp,
                  "*****logpack header*****\n"
                  "checksum: %08x(%u)\n"
                  "n_records: %u\n"
                  "n_padding: %u\n"
                  "total_io_size: %u\n"
                  "logpack_lsid: %" PRIu64 "\n",
                  header_->checksum, header_->checksum,
                  header_->n_records,
                  header_->n_padding,
                  header_->total_io_size,
                  header_->logpack_lsid);
    }
    void print(::FILE *fp = ::stdout) const {
        printHeader(fp);
        for (size_t i = 0; i < nRecords(); i++) {
            printRecord(i, fp);
        }
    }
    /**
     * Print each IO oneline.
     * logpack_lsid, mode(W, D, or P), offset[lb], io_size[lb].
     */
    void printShort(::FILE *fp = ::stdout) const {
        for (size_t i = 0; i < nRecords(); i++) {
            const struct walb_log_record &rec = record(i);
            assert(::test_bit_u32(LOG_RECORD_EXIST, &rec.flags));
            char mode = 'W';
            if (::test_bit_u32(LOG_RECORD_DISCARD, &rec.flags)) { mode = 'D'; }
            if (::test_bit_u32(LOG_RECORD_PADDING, &rec.flags)) { mode = 'P'; }
            ::fprintf(fp,
                      "%" PRIu64 "\t%c\t%" PRIu64 "\t%u\n",
                      header_->logpack_lsid,
                      mode, rec.offset, rec.io_size);
        }
    }
    /**
     * Update checksum field.
     */
    void updateChecksum() {
        header_->checksum = 0;
        header_->checksum = ::checksum((const uint8_t*)header_, pbs(), salt());
    }
    /**
     * Write the logpack header block.
     */
    void write(int fd) {
        cybozu::util::FdWriter fdw(fd);
        write(fdw);
    }
    /**
     * Write the logpack header block.
     */
    void write(cybozu::util::FdWriter &fdw) {
        updateChecksum();
        if (!isValid(true)) {
            throw RT_ERR("logpack header invalid.");
        }
        fdw.write(header_, pbs());
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
            throw RT_ERR("Normal IO can not be zero-sized.");
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
            throw RT_ERR("Discard IO can not be zero-sized.");
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
            throw RT_ERR("Padding size must be pbs-aligned.");
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
     */
    void shrink(size_t invalidIdx) {
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

namespace log {

/**
 * Interface.
 */
class Record
{
public:
    Record() = default;
    Record(const Record &) = delete;
    Record &operator=(const Record &rhs) {
        record() = rhs.record();
        setPbs(rhs.pbs());
        setSalt(rhs.salt());
        return *this;
    }
    DISABLE_MOVE(Record);
    virtual ~Record() noexcept {}

    /* Interface to access a record */
    virtual struct walb_log_record &record() = 0;
    virtual const struct walb_log_record &record() const = 0;

    virtual size_t pos() const = 0;
    virtual unsigned int pbs() const = 0;
    virtual uint32_t salt() const = 0;
    virtual void setPos(size_t) = 0;
    virtual void setPbs(unsigned int) = 0;
    virtual void setSalt(uint32_t) = 0;


    /* default implementation. */
    uint64_t lsid() const { return record().lsid; }
    uint16_t lsidLocal() const { return record().lsid_local; }
    uint64_t packLsid() const { return lsid() - lsidLocal(); }
    bool isExist() const {
        return ::test_bit_u32(LOG_RECORD_EXIST, &record().flags);
    }
    bool isPadding() const {
        return ::test_bit_u32(LOG_RECORD_PADDING, &record().flags);
    }
    bool isDiscard() const {
        return ::test_bit_u32(LOG_RECORD_DISCARD, &record().flags);
    }
    bool hasData() const {
        return isExist() && !isDiscard();
    }
    bool hasDataForChecksum() const {
        return isExist() && !isDiscard() && !isPadding();
    }
    unsigned int ioSizeLb() const { return record().io_size; }
    unsigned int ioSizePb() const { return ::capacity_pb(pbs(), ioSizeLb()); }
    uint64_t offset() const { return record().offset; }
    bool isValid() const { return ::is_valid_log_record_const(&record()); }
    uint32_t checksum() const { return record().checksum; }

    virtual void print(::FILE *fp = ::stdout) const {
        printRecord(fp, pos(), record());
    }
    virtual void printOneline(::FILE *fp = ::stdout) const {
        printRecordOneline(fp, pos(), record());
    }

    void setExist() {
        ::set_bit_u32(LOG_RECORD_EXIST, &record().flags);
    }
    void setPadding() {
        ::set_bit_u32(LOG_RECORD_PADDING, &record().flags);
    }
    void setDiscard() {
        ::set_bit_u32(LOG_RECORD_DISCARD, &record().flags);
    }
    void clearExist() {
        ::clear_bit_u32(LOG_RECORD_EXIST, &record().flags);
    }
    void clearPadding() {
        ::clear_bit_u32(LOG_RECORD_PADDING, &record().flags);
    }
    void clearDiscard() {
        ::clear_bit_u32(LOG_RECORD_DISCARD, &record().flags);
    }
};

/**
 * Wrapper of a raw walb log record.
 */
class RecordRaw : public Record
{
private:
    size_t pos_;
    unsigned int pbs_;
    uint32_t salt_;
    struct walb_log_record rec_;
public:
    RecordRaw() : Record(), pos_(0), pbs_(0), salt_(0), rec_() {}
    RecordRaw(
        const struct walb_log_record &rec, size_t pos,
        unsigned int pbs, uint32_t salt)
        : Record(), pos_(pos), pbs_(pbs), salt_(salt), rec_(rec) {}
    RecordRaw(const LogPackHeader &logh, size_t pos)
        : RecordRaw(logh.record(pos), pos, logh.pbs(), logh.salt()) {}
    ~RecordRaw() noexcept override = default;
    RecordRaw(const RecordRaw &rhs)
        : RecordRaw(static_cast<const Record &>(rhs)) {}
    RecordRaw(const Record &rhs)
        : Record(), pos_(rhs.pos()), pbs_(rhs.pbs()), salt_(rhs.salt()), rec_(rhs.record()) {}
    RecordRaw &operator=(const RecordRaw &rhs) {
        return operator=(static_cast<const Record &>(rhs));
    }
    RecordRaw &operator=(const Record &rhs) {
        setPos(rhs.pos());
        setPbs(rhs.pbs());
        setSalt(rhs.salt());
        record() = rhs.record();
        return *this;
    }
    DISABLE_MOVE(RecordRaw);

    void copyFrom(const LogPackHeader &logh, size_t pos) {
        setPos(pos);
        setPbs(logh.pbs());
        setSalt(logh.salt());
        record() = logh.record(pos);
    }

    size_t pos() const override { return pos_; }
    unsigned int pbs() const override { return pbs_; }
    uint32_t salt() const override { return salt_; }

    void setPos(size_t pos0) override { pos_ = pos0; }
    void setPbs(unsigned int pbs0) override { pbs_ = pbs0; }
    void setSalt(uint32_t salt0) override { salt_ = salt0; }

    const struct walb_log_record &record() const override { return rec_; }
    struct walb_log_record &record() override { return rec_; }
};

/**
 * Log data of an IO.
 * Log record is a reference.
 */
class RecordWrap : public Record
{
    LogPackHeader *logh_;
    size_t pos_;
public:
    RecordWrap(const LogPackHeader *logh, size_t pos)
        : logh_(const_cast<LogPackHeader *>(logh)) , pos_(pos) {
        assert(pos < logh->nRecords());
    }
    DISABLE_COPY_AND_ASSIGN(RecordWrap);
    DISABLE_MOVE(RecordWrap);

    size_t pos() const { return pos_; }
    unsigned int pbs() const { return logh_->pbs(); }
    uint32_t salt() const { return logh_->salt(); }

    void setPos(size_t pos0) { pos_ = pos0; }
    void setPbs(unsigned int pbs0) { if (pbs() != pbs0) throw RT_ERR("pbs differs."); }
    void setSalt(uint32_t salt0) { if (salt() != salt0) throw RT_ERR("salt differs."); }

    const struct walb_log_record &record() const { return logh_->record(pos_); }
    struct walb_log_record &record() {
        return *const_cast<struct walb_log_record *>(&logh_->record(pos_));
    }
};

/**
 * Interface.
 */
class BlockData
{
public:
    using Block = std::shared_ptr<uint8_t>;

    virtual ~BlockData() noexcept = default;

    /*
     * You must implement these member functions.
     */
    virtual unsigned int pbs() const = 0;
    virtual void setPbs(unsigned int pbs) = 0;
    virtual size_t nBlocks() const = 0;
    virtual const uint8_t *get(size_t idx) const = 0;
    virtual uint8_t *get(size_t idx) = 0;
    virtual void resize(size_t nBlocks) = 0;
    virtual void addBlock(const Block &block) = 0;
    virtual Block getBlock(size_t idx) const = 0;

    /*
     * Utilities.
     */
    uint32_t calcChecksum(size_t ioSizeLb, uint32_t salt) const {
        checkPbs();
        checkSizeLb(ioSizeLb);
        uint32_t csum = salt;
        size_t remaining = ioSizeLb * LOGICAL_BLOCK_SIZE;
        size_t i = 0;
        while (0 < remaining) {
            assert(i < nBlocks());
            size_t s = pbs();
            if (remaining < pbs()) s = remaining;
            csum = cybozu::util::checksumPartial(get(i), s, csum);
            remaining -= s;
            i++;
        }
        return cybozu::util::checksumFinish(csum);
    }
    bool calcIsAllZero(size_t ioSizeLb) const {
        checkPbs();
        checkSizeLb(ioSizeLb);
        size_t remaining = ioSizeLb * LOGICAL_BLOCK_SIZE;
        size_t i = 0;
        while (0 < remaining) {
            assert(i < nBlocks());
            size_t s = pbs();
            if (remaining < pbs()) s = remaining;
            if (!cybozu::util::calcIsAllZero(get(i), s)) return false;
            remaining -= s;
            i++;
        }
        return true;
    }
    void write(int fd) const {
        cybozu::util::FdWriter fdw(fd);
        write(fdw);
    }
    void write(cybozu::util::FdWriter &fdw) const {
        checkPbs();
        for (size_t i = 0; i < nBlocks(); i++) {
            fdw.write(get(i), pbs());
        }
    }
protected:
    void checkPbs() const {
        if (pbs() == 0) throw RT_ERR("pbs must not be zero.");
    }
    void checkSizeLb(size_t ioSizeLb) const {
        if (nBlocks() * pbs() < ioSizeLb * LOGICAL_BLOCK_SIZE) {
            throw RT_ERR("ioSizeLb too large. specified %zu, should be <= %zu."
                         , ioSizeLb, ::capacity_lb(pbs(), nBlocks()));
        }
    }
};

/**
 * Block data using a vector.
 */
class BlockDataVec : public BlockData
{
private:
    using Block = std::shared_ptr<uint8_t>;
    unsigned int pbs_; /* physical block size. */
    std::vector<uint8_t> data_;

public:
    BlockDataVec() : pbs_(0), data_() {}
    explicit BlockDataVec(unsigned int pbs) : pbs_(pbs), data_() {}
    BlockDataVec(const BlockDataVec &) = default;
    BlockDataVec(BlockDataVec &&) = default;
    BlockDataVec &operator=(const BlockDataVec &) = default;
    BlockDataVec &operator=(BlockDataVec &&) = default;

    unsigned int pbs() const override { return pbs_; }
    void setPbs(unsigned int pbs) override {
        pbs_ = pbs;
        checkPbs();
    }
    size_t nBlocks() const override { return data_.size() / pbs(); }
    const uint8_t *get(size_t idx) const override {
        check(idx);
        return &data_[idx * pbs()];
    }
    uint8_t *get(size_t idx) override {
        check(idx);
        return &data_[idx * pbs()];
    }
    void resize(size_t nBlocks0) override {
        data_.resize(nBlocks0 * pbs());
    }
    void addBlock(const Block &block) override {
        checkPbs();
        size_t s0 = data_.size();
        size_t s1 = s0 + pbs();
        data_.resize(s1);
        ::memcpy(&data_[s0], block.get(), pbs());
    }
    Block getBlock(size_t idx) const override {
        check(idx);
        Block b = cybozu::util::allocateBlocks<uint8_t>(pbs(), pbs());
        ::memcpy(b.get(), get(idx), pbs());
        return b;
    }

    void moveFrom(std::vector<uint8_t> &&data) {
        data_ = std::move(data);
    }
    void copyFrom(const uint8_t *data, size_t size) {
        data_.resize(size);
        ::memcpy(&data_[0], data, size);
    }
private:
    void check(size_t idx) const {
        checkPbs();
        if (data_.size() % pbs() != 0) {
            throw RT_ERR("data size is not multiples of pbs.");
        }
        if (nBlocks() <= idx) {
            throw RT_ERR("BlockDataVec: index out of range.");
        }
    }
};

/**
 * Helper class to manage multiple IO blocks.
 * This is copyable and movable.
 */
class BlockDataShared : public BlockData
{
private:
    using Block = std::shared_ptr<uint8_t>;

    unsigned int pbs_; /* physical block size [byte]. */
    std::vector<Block> data_; /* Each block's size must be pbs_. */

public:
    BlockDataShared() : pbs_(0), data_() {}
    explicit BlockDataShared(unsigned int pbs) : pbs_(pbs), data_() {}
    BlockDataShared(const BlockDataShared &) = default;
    BlockDataShared(BlockDataShared &&) = default;
    BlockDataShared &operator=(const BlockDataShared &) = default;
    BlockDataShared &operator=(BlockDataShared &&) = default;

    unsigned int pbs() const override { return pbs_; }
    void setPbs(unsigned int pbs) override { pbs_ = pbs; }
    size_t nBlocks() const override { return data_.size(); }
    const uint8_t *get(size_t idx) const override {
        check(idx);
        return data_[idx].get();
    }
    uint8_t *get(size_t idx) override {
        check(idx);
        return data_[idx].get();
    }
    void resize(size_t nBlocks0) override {
        if (nBlocks0 < data_.size()) {
            data_.resize(nBlocks0);
        }
        while (data_.size() < nBlocks0) {
            data_.push_back(allocPb());
        }
    }
    void addBlock(const Block &block) override {
        assert(block);
        data_.push_back(block);
    }
    Block getBlock(size_t idx) const override { return data_[idx]; }

private:
    void check(size_t idx) const {
        checkPbs();
        if (nBlocks() <= idx) {
            throw RT_ERR("BlockDataShared: index out of range.");
        }
    }
    Block allocPb() const {
        return cybozu::util::allocateBlocks<uint8_t>(pbs(), pbs());
    }
};

/**
 * Logpack record and IO data.
 * This is just a wrapper of a record and a block data.
 *
 * Record: Record
 * BlockData: BlockData
 */
struct PackIoWrap
{
    Record *recP_;
    BlockData *blockD_;

    const Record &record() const { return *recP_; }
    Record &record() { return *recP_; }
    const BlockData &blockData() const { return *blockD_; }
    BlockData &blockData() { return *blockD_; }

    // not use blockD_
    bool isValid(bool isChecksum = true) const {
        if (!recP_->isValid()) {
            LOGd("invalid record.");
            return false; }
        if (isChecksum && recP_->hasDataForChecksum() &&
            calcIoChecksum() != recP_->record().checksum) {
            LOGd("invalid checksum expected %08x calculated %08x."
                 , recP_->record().checksum, calcIoChecksum());
            return false;
        }
        return true;
    }
    // not use blockD_
    void printOneline(::FILE *fp = ::stdout) const {
        recP_->printOneline(fp);
    }
    // not use blockD_
    uint32_t calcIoChecksum() const {
        return calcIoChecksum(recP_->salt());
    }

    // use blockD_
    void print(::FILE *fp = ::stdout) const {
        recP_->print(fp);
        if (recP_->hasDataForChecksum() && recP_->ioSizePb() == blockD_->nBlocks()) {
            ::fprintf(fp, "record_checksum: %08x\n"
                      "calculated_checksum: %08x\n",
                      recP_->record().checksum, calcIoChecksum());
            for (size_t i = 0; i < recP_->ioSizePb(); i++) {
                ::fprintf(fp, "----------block %zu----------\n", i);
                cybozu::util::printByteArray(fp, blockD_->get(i), recP_->pbs());
            }
        }
    }

    // use blockD_
    bool setChecksum() {
        if (!recP_->hasDataForChecksum()) { return false; }
        if (recP_->ioSizePb() != blockD_->nBlocks()) { return false; }
        recP_->record().checksum = calcIoChecksum();
        return true;
    }
    // use blockD_
    uint32_t calcIoChecksum(uint32_t salt) const {
        assert(recP_->hasDataForChecksum());
        assert(0 < recP_->ioSizeLb());
        if (blockD_->nBlocks() < recP_->ioSizePb()) {
            throw RT_ERR("There is not sufficient data block.");
        }
        return blockD_->calcChecksum(recP_->ioSizeLb(), salt);
    }
};

template<class R>
uint32_t calcIoChecksum(const R& rec, const BlockData& blockD)
{
    const uint32_t salt = rec.salt();
    if (blockD.nBlocks() < rec.ioSizePb()) {
        throw cybozu::Exception("calcIoChecksum:There is not sufficient data block.") << blockD.nBlocks() << rec.ioSizePb();
    }
    return blockD.calcChecksum(rec.ioSizeLb(), salt);
}

template<class R>
bool isValidRecordAndBlockData(const R& rec, const BlockData& blockD)
{
    if (!rec.isValid()) {
        LOGd("invalid record.");
        return false; }
    if (!rec.hasDataForChecksum()) return true;
    const uint32_t checksum = calcIoChecksum(rec, blockD);
    if (checksum != rec.record().checksum) {
        LOGd("invalid checksum expected %08x calculated %08x."
             , rec.record().checksum, checksum);
        return false;
    }
    return true;
}

/**
 * Logpack record and IO data.
 * This is copyable and movable.
 * BlockDataT: BlockDataVec or BlockDataShared.
 */
template <class BlockDataT>
class PackIoRaw
{
private:
    RecordRaw rec_;
    BlockDataT blockD_;
public:
    PackIoRaw() {}
    PackIoRaw(const LogPackHeader &logh, size_t pos)
        : rec_(logh, pos), blockD_(logh.pbs()) {}
    PackIoRaw(PackIoRaw &&rhs)
        : rec_(rhs.rec_), blockD_(std::move(rhs.blockD_)) {}
    PackIoRaw &operator=(PackIoRaw &&rhs) {
        rec_ = rhs.rec_;
        blockD_ = std::move(rhs.blockD_);
        return *this;
    }
    /*
        QQQ copy these following method from  PackIoWrap to avoid inheritance
        the following methods are all called
    */
    Record &record() { return rec_; }
    BlockData &blockData() { return blockD_; }

    // not use blockD_
    bool isValid(bool isChecksum = true) const {
        if (!rec_.isValid()) {
            LOGd("invalid record.");
            return false; }
        if (isChecksum && rec_.hasDataForChecksum() &&
            calcIoChecksum() != rec_.record().checksum) {
            LOGd("invalid checksum expected %08x calculated %08x."
                 , rec_.record().checksum, calcIoChecksum());
            return false;
        }
        return true;
    }
    // not use blockD_
    void printOneline(::FILE *fp = ::stdout) const {
        rec_.printOneline(fp);
    }
    // not use blockD_
    uint32_t calcIoChecksum() const {
        return calcIoChecksum(rec_.salt());
    }

    // use blockD_
    void print(::FILE *fp = ::stdout) const {
        rec_.print(fp);
        if (rec_.hasDataForChecksum() && rec_.ioSizePb() == blockD_.nBlocks()) {
            ::fprintf(fp, "record_checksum: %08x\n"
                      "calculated_checksum: %08x\n",
                      rec_.record().checksum, calcIoChecksum());
            for (size_t i = 0; i < rec_.ioSizePb(); i++) {
                ::fprintf(fp, "----------block %zu----------\n", i);
                cybozu::util::printByteArray(fp, blockD_.get(i), rec_.pbs());
            }
        }
    }

    // use blockD_
    uint32_t calcIoChecksum(uint32_t salt) const {
        assert(rec_.hasDataForChecksum());
        assert(0 < rec_.ioSizeLb());
        if (blockD_.nBlocks() < rec_.ioSizePb()) {
            throw RT_ERR("There is not sufficient data block.");
        }
        return blockD_.calcChecksum(rec_.ioSizeLb(), salt);
    }
};

}} //namespace walb::log
