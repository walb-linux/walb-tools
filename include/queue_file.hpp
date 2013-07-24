/**
 * @file
 * @brief Simple permanent queue.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <exception>
#include <algorithm>
#include <cstdio>
#include <limits>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include "fileio.hpp"
#include "mmap_file.hpp"
#include "checksum.hpp"
#include "random.hpp"
#include "flock.hpp"

#ifndef QUEUE_FILE_HPP
#define QUEUE_FILE_HPP

namespace cybozu {
namespace util {

static const uint32_t QUEUE_PREAMBLE = 0x119d83a7;

struct QueueFileHeader
{
    uint32_t preamble; /* Preamble of the queue file. */
    uint32_t checksum; /* Checksum of header block. the salt is 0. */
    uint32_t salt; /* Salt for record checksum. */
    uint64_t beginOffset; /* This preserve there is no valid record before the offset.
                             Just use it for hint. [byte] */
    uint64_t endOffset; /* This preserve there are more than totalSize [byte] area and
                           the offset (sizeof(QueueFileHeader) + endOffset) indicated
                           the next record header or end of file. */
} __attribute__((packed));

struct QueueRecordHeader
{
    uint32_t preamble; /* Preample of the queue record. */
    uint32_t checksum; /* checksum of data. The salt is stored in the queue file header. */
    uint32_t size; /* data size [byte].
                    * The highest bit menas delete flag.
                      0 means end mark. */
} __attribute__((packed));

/**
 * QueueRecordHeader operator.
 */
class QueueRecordHeaderOp
{
private:
    QueueRecordHeader *rec_;

public:
    QueueRecordHeaderOp(QueueRecordHeader *rec)
        : rec_(nullptr) {
        reset(rec);
    }
    QueueRecordHeaderOp(const QueueRecordHeader *rec)
        : QueueRecordHeaderOp(const_cast<QueueRecordHeader*>(rec)) {
    }
    virtual ~QueueRecordHeaderOp() noexcept {
    }
    void print() const {
        ::printf("preamble %" PRIu32 " (%08x)\n"
                 "checksum %" PRIu32 " (%08x)\n"
                 "size     %" PRIu32 " (%08x)\n"
                 , rec_->preamble, rec_->preamble
                 , rec_->checksum, rec_->checksum
                 , rec_->size, rec_->size);
    }
    void reset(QueueRecordHeader *rec) {
        if (rec == nullptr) {
            throw std::runtime_error("rec is null.");
        }
        rec_ = rec;
    }
    void init() {
        rec_->preamble = QUEUE_PREAMBLE;
        rec_->checksum = 0;
        rec_->size = 0;
    }
    bool isValid() const {
        return rec_->preamble == QUEUE_PREAMBLE;
    }
    bool isEndMark() const {
        return rec_->size == 0;
    }
    bool isDeleted() const {
        return (rec_->size & 0x80000000) != 0;
    }
    uint32_t dataSize() const {
        return rec_->size & ~0x80000000;
    }
    void setDeleted() {
        rec_->size |= 0x80000000;
    }
    void setDataSize(uint32_t size) {
        if ((size & 0x80000000) != 0) {
            throw std::runtime_error("size must be < 2^31.");
        }
        if (size == 0) {
            throw std::runtime_error("size must not be 0.");
        }
        rec_->size &= 0x80000000;
        rec_->size |= size;
    }
    void setChecksum(const void *data, uint32_t salt) {
        rec_->checksum = calcChecksum(data, dataSize(), salt);
    }
    uint32_t checksum() const {
        return rec_->checksum;
    }
    QueueRecordHeader* getNext() {
        if (isEndMark()) {
            return nullptr;
        }
        return reinterpret_cast<QueueRecordHeader*>(
            reinterpret_cast<uint8_t*>(rec_) + totalSize());
    }
    const QueueRecordHeader* getNext() const {
        if (isEndMark()) {
            return nullptr;
        }
        return reinterpret_cast<const QueueRecordHeader*>(
            reinterpret_cast<const uint8_t*>(rec_) + totalSize());
    }
    bool goNext() {
        QueueRecordHeader *rec = getNext();
        if (rec == nullptr) {
            return false;
        }
        rec_ = rec;
        return true;
    }
    template <typename T>
    T* dataPtr() {
        return reinterpret_cast<T*>(
            reinterpret_cast<uint8_t*>(rec_) + headerSize());
    }
    template <typename T>
    const T* dataPtr() const {
        return reinterpret_cast<const T*>(
            reinterpret_cast<const uint8_t*>(rec_) + headerSize());
    }
    uint32_t headerSize() const {
        return sizeof(QueueRecordHeader);
    }
    uint32_t totalSize() const {
        return headerSize() + dataSize();
    }
};

/**
 * A queue file with fixed-size records.
 *
 * Queue file format:
 *   1st QueueFileHeader
 *   2nd QueueFileHeader
 *   [QueueRecordHeader, byte array (record data)] * n
 *   QueueRecordHeader (end mark)
 */
class QueueFile
{
private:
    mutable QueueFileHeader header_;
    MmappedFile mmappedFile_;
    cybozu::file::Lock lock_;

public:
    QueueFile(const std::string& filePath, int flags)
        : mmappedFile_(0, filePath, flags)
        , lock_(filePath) {
        init(false);
    }
    QueueFile(const std::string& filePath, int flags, int mode)
        : mmappedFile_(sizeof(QueueFileHeader) * 2 + sizeof(QueueRecordHeader),
                       filePath, flags, mode)
        , lock_(filePath){
        init(true);
    }
    virtual ~QueueFile() noexcept {
        try {
            sync();
        } catch (...) {
        }
    }
    void push(const void *data, uint32_t size) {
        if (size == 0) {
            throw std::runtime_error("can not push zero-size data.");
        }
        mmappedFile_.resize(getRequiredFileSize(size));
        QueueRecordHeaderOp recOp(&record(header_.endOffset));
        recOp.init();
        recOp.setDataSize(size);
        recOp.setChecksum(data, header_.salt);
        ::memcpy(recOp.dataPtr<void>(), data, size);
        header_.endOffset += recOp.totalSize();
        if (!recOp.goNext()) assert(false);
        recOp.init(); /* end mark. */
    }
    void push(const std::string& s) {
        if (s.empty()) {
            std::runtime_error("can not push empty strings.");
        }
        push(&s[0], s.size());
    }
    bool empty() const {
        gcFront();
        return header_.beginOffset == header_.endOffset;
    }
    template <typename CharT>
    void front(std::vector<CharT>& v) const {
        gcFront();
        assert(!empty());
        const QueueRecordHeaderOp recOp(&record(header_.beginOffset));
        assert(!recOp.isEndMark());
        v.resize(recOp.dataSize());
        ::memcpy(&v[0], recOp.dataPtr<void>(), recOp.dataSize());
    }
    void front(std::string& s) const {
        gcFront();
        assert(!empty());
        const QueueRecordHeaderOp recOp(&record(header_.beginOffset));
        assert(!recOp.isEndMark());
        s.resize(recOp.dataSize());
        ::memcpy(&s[0], recOp.dataPtr<void>(), recOp.dataSize());
    }
    void pop() {
        QueueRecordHeaderOp recOp(&record(header_.beginOffset));
        recOp.setDeleted();
        gcFront();
    }
    void printOffsets() const {
        ::printf("begin %" PRIu64 " end %" PRIu64 "\n"
                 , header_.beginOffset, header_.endOffset); //debug
    }
    void sync() {
        updateFileHeaderChecksum();
        /* Double write for atomicity. */
        for (int i = 0; i < 2; i++) {
            ::memcpy(ptr<uint8_t>() + sizeof(header_) * i, &header_, sizeof(header_));
            mmappedFile_.sync();
        }
    }
    /**
     * Garbage collect.
     * All records will be moved to the front area.
     * Then the file will be truncated.
     * There must be enough space of the front area to copy whole data.
     */
    void gc() {
        gcFront();
        uint64_t bgn = header_.beginOffset;
        uint64_t end = header_.endOffset;
        assert(bgn <= end);
        uint64_t len = end - bgn;

        if (bgn < len + sizeof(QueueRecordHeader)) {
            /* There is not enough space. */
            return;
        }

        /* Sync current begin/end offset. */
        sync();
        /* Copy data to the front. */
        ::memcpy(&record(0), &record(bgn), len);
        QueueRecordHeaderOp recOp(&record(len));
        recOp.init(); /* end mark */
        sync();
        /* Update the file header. */
        header_.beginOffset = 0;
        header_.endOffset = len;
        sync();
        /* Truncate the file. */
        mmappedFile_.resize(getRequiredFileSize(0));
        sync();
    }

    class ConstIterator {
    protected:
        const QueueFile& qf_;
        uint64_t offset_;
    public:
        ConstIterator(const QueueFile& qf, uint64_t offset)
            : qf_(qf), offset_(offset) {}
        virtual ~ConstIterator() noexcept {}
        ConstIterator& operator++() {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            offset_ += recOp.totalSize();
            return *this;
        }
        ConstIterator operator++(int) {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            ConstIterator ret(qf_, offset_);
            offset_ += recOp.totalSize();
            return ret;
        }
        bool operator==(const ConstIterator& rhs) const {
            return offset_ == rhs.offset_;
        }
        bool operator!=(const ConstIterator& rhs) const {
            return offset_ != rhs.offset_;
        }
        bool isValid() const {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            if (!recOp.isValid()) {
                return false;
            }
            if (calcChecksum(recOp.dataPtr<void>(), recOp.dataSize(), qf_.header_.salt)
                != recOp.checksum()) {
                return false;
            }
            return true;
        }
        template <typename CharT>
        void get(std::vector<CharT>& v) const {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            assert(isValid());
            v.resize(recOp.dataSize());
            ::memcpy(&v[0], recOp.dataPtr<void>(), recOp.dataSize());
        }
        void get(std::string& s) const {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            assert(isValid());
            s.resize(recOp.dataSize());
            ::memcpy(&s[0], recOp.dataPtr<void>(), recOp.dataSize());
        }
        bool isEndMark() const {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            return recOp.isEndMark();
        }
        bool isDeleted() const {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            return recOp.isDeleted();
        }
    };

    class Iterator : public ConstIterator {
    protected:
        QueueFile& qfw_;
    public:
        Iterator(QueueFile& qf, uint64_t offset)
            : ConstIterator(qf, offset)
            , qfw_(qf) {}
        ~Iterator() noexcept {}
        Iterator& operator++() {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            offset_ += recOp.totalSize();
            return *this;
        }
        Iterator operator++(int) {
            QueueRecordHeaderOp recOp(&qf_.record(offset_));
            Iterator ret(qfw_, offset_);
            offset_ += recOp.totalSize();
            return ret;
        }
        /**
         * After calling this functions,
         * You must call gc() for readers not to see the deleted records.
         */
        void setDeleted() {
            QueueRecordHeaderOp recOp(&qfw_.record(offset_));
            recOp.setDeleted();
        }
    };

    ConstIterator cbegin() const {
        return ConstIterator(*this, header_.beginOffset);
    }
    ConstIterator cend() const {
        return ConstIterator(*this, header_.endOffset);
    }
    Iterator begin() {
        return Iterator(*this, header_.beginOffset);
    }
    Iterator end() {
        return Iterator(*this, header_.endOffset);
    }

private:
    template <typename T>
    T* ptr() { return mmappedFile_.ptr<T>(); }
    template <typename T>
    const T* ptr() const { return mmappedFile_.ptr<T>(); }
    void init(bool isCreated) {
        if (isCreated) {
            header_.preamble = QUEUE_PREAMBLE;
            Random<uint32_t> rand;
            header_.salt = rand();
            header_.beginOffset = 0;
            header_.endOffset = 0;
            QueueRecordHeaderOp recOp(&record(0));
            recOp.init();
            sync();
            return;
        }
        for (int i = 0; i < 2; i++) {
            /* Read i'th header data. */
            ::memcpy(&header_, ptr<uint8_t>() + sizeof(header_) * i, sizeof(header_));
            if (isValidFileHeader()) {
                break;
            }
        }
        if (!isValidFileHeader()) {
            std::runtime_error("Both header data broken.");
        }
        header_.endOffset = calcEndOffset(); /* The order is important. end -> begin. */
        header_.beginOffset = calcBeginOffset();
        sync();
    }
    bool isValidFileHeader() const {
        if (header_.preamble != QUEUE_PREAMBLE) {
            return false;
        }
        if (calcChecksum(&header_, sizeof(header_), 0) != 0) {
            return false;
        }
        return true;
    }
    void updateFileHeaderChecksum() {
        header_.checksum = 0;
        header_.checksum = calcChecksum(&header_, sizeof(header_), 0);
    }
    const QueueRecordHeader& record(uint64_t offset) const {
        return *reinterpret_cast<const QueueRecordHeader*>(
            ptr<uint8_t>() + sizeof(QueueFileHeader) * 2 + offset);
    }
    QueueRecordHeader& record(uint64_t offset) {
        return *reinterpret_cast<QueueRecordHeader*>(
            ptr<uint8_t>() + sizeof(QueueFileHeader) * 2 + offset);
    }
    uint64_t calcBeginOffset() const {
        uint64_t offset = header_.beginOffset;
        while (offset < header_.endOffset) {
            const QueueRecordHeaderOp recOp(&record(offset));
            if (!recOp.isValid() ||
                calcChecksum(recOp.dataPtr<void>(), recOp.dataSize(), header_.salt)
                != recOp.checksum()) {
                throw std::runtime_error("Found invalid record.");
            }
            if (!recOp.isDeleted()) {
                break;
            }
            offset += recOp.totalSize();
        }
        return offset;
    }
    uint64_t calcEndOffset() const {
        uint64_t offset = header_.endOffset;
        while (offset + sizeof(QueueRecordHeader) < mmappedFile_.size()) {
            const QueueRecordHeaderOp recOp(&record(offset));
            if (recOp.isEndMark()) {
                break;
            }
            if (!recOp.isValid() ||
                calcChecksum(recOp.dataPtr<void>(), recOp.dataSize(), header_.salt)
                != recOp.checksum()) {
                throw std::runtime_error("Found invalid record.");
            }
            offset += recOp.totalSize();
        }
        return offset;
    }
    uint64_t getRequiredFileSize(uint64_t size) const {
        return sizeof(QueueFileHeader) * 2 + header_.endOffset /* current size except end mark. */
            + ((size == 0) ? 0 : (sizeof(QueueRecordHeader) + size)) /* for a record. */
            + sizeof(QueueRecordHeader); /* for end mark. */
    }
    /**
     * GC deleted records at the front of the queue.
     * After calling this, the front().isDeleted() must be false if exists.
     */
    void gcFront() const {
        if (header_.beginOffset == header_.endOffset) return;
        QueueRecordHeaderOp recOp(&record(header_.beginOffset));
        while (!recOp.isEndMark() && recOp.isDeleted()) {
            header_.beginOffset += recOp.totalSize();
            recOp.goNext();
        }
    }
};

}} //namespace cybozu::util

#endif /* QUEUE_FILE_HPP */
