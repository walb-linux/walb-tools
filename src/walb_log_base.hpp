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
#include "linux/walb/super.h"
#include "linux/walb/log_device.h"
#include "linux/walb/log_record.h"
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

    std::string str() const;
    std::string strDetail() const;

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
    std::string strHead() const;
    std::string strHeadDetail() const;
    std::string str() const;
    std::string strDetail() const;

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
    bool addNormalIo(uint64_t offset, uint16_t size);
    /**
     * Add a discard IO.
     *
     * @offset [logical block]
     * @size [logical block]
     *   can not be 0.
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addDiscardIo(uint64_t offset, uint32_t size);
    /**
     * Add a padding.
     *
     * @size [logical block]
     *   can be 0.
     * RETURN:
     *   true in success, or false (you must create a new header).
     */
    bool addPadding(uint16_t size);
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
    void updateLsid(uint64_t newLsid);
    /**
     * Shrink.
     * Delete records from rec[invalidIdx] to the last.
     * If you specify invalidIdx = 0, then the pack will be empty.
     * RETURN:
     *   next logpack lsid.
     */
    uint64_t shrink(size_t invalidIdx);
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
    size_t invalidate(size_t idx);
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


/**
 * data size will be multiples of physical blocks.
 * padding IO data will also be set.
 */
template <typename Reader>
inline bool readLogIo(Reader &reader, const LogPackHeader &packH, size_t idx, AlignedArray &data)
{
    const WlogRecord &lrec = packH.record(idx);
    if (!lrec.hasData()) return true;

    // Read.
    const uint32_t pbs = packH.pbs();
    const size_t ioSizePb = lrec.ioSizePb(pbs);
    data.resize(ioSizePb * pbs);
    reader.read(data.data(), data.size()); // physical blocks.
    if (!lrec.hasDataForChecksum()) {
        assert(lrec.isPadding());
        // keep padding data.
        return true;
    }

    // checksum.
    const size_t ioSizeB = lrec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
    const uint32_t csum = cybozu::util::calcChecksum(data.data(), ioSizeB, packH.salt());
    return lrec.checksum == csum;
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
bool readAllLogIos(
    Reader &reader, LogPackHeader &packH, std::queue<AlignedArray> &ioQ,
    bool doShrink = true)
{
    bool isNotShrinked = true;
    for (size_t i = 0; i < packH.nRecords(); i++) {
        const WlogRecord &rec = packH.record(i);
        if (!rec.hasData()) continue;

        AlignedArray buf;
        const bool valid = readLogIo(reader, packH, i, buf);
        if (doShrink && !valid) {
            packH.shrink(i);
            isNotShrinked = false;
            break;
        }
        ioQ.push(std::move(buf));
    }
    assert(packH.nRecordsHavingData() == ioQ.size());
    return isNotShrinked;
}

/**
 * Skip to read log IOs corresponding to a logpack.
 * This does not verify the log IO data, or really seek in a walb log device.
 */
template <typename Reader>
void skipAllLogIos(Reader &reader, const LogPackHeader &packH)
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

    void init(uint64_t bgnLsid, uint64_t endLsid);
    void update(const LogPackHeader &packH);
    std::string str() const;

    friend inline std::ostream& operator<<(std::ostream& os, const LogStatistics &logStat) {
        os << logStat.str();
        return os;
    }
};

} // walb
