#pragma once
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
#include <sstream>
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
    uint32_t dataSize; /* data size [byte]. */
    uint32_t toPrev; /* offset difference to previous record [byte]. */

    void init() {
        preamble = QUEUE_PREAMBLE;
        checksum = 0;
        dataSize = 0;
        toPrev = 0;
    }
    std::string str(bool verbose = false) const {
        std::string s = cybozu::util::formatString(
            "preamble %" PRIu32 " (%08x) "
            "checksum %" PRIu32 " (%08x) "
            "dataSize %" PRIu32 " (%08x) "
            "toPrev   %" PRIu32 " (%08x) "
            , preamble, preamble
            , checksum, checksum
            , dataSize, dataSize
            , toPrev, toPrev);
        if (verbose) s += cybozu::util::byteArrayToStr(data(), dataSize);
        return s;
    }
    bool isValid() const {
        return preamble == QUEUE_PREAMBLE;
    }
    void verify() const {
        if (!isValid()) {
            throw std::runtime_error(std::string("invalid record:") + str());
        }
    }
    uint32_t headerSize() const {
        return sizeof(QueueRecordHeader);
    }
    uint32_t totalSize() const {
        return headerSize() + dataSize;
    }
    QueueRecordHeader& prev() { return *(QueueRecordHeader *)(prevPtr()); }
    const QueueRecordHeader& prev() const { return *(const QueueRecordHeader *)(prevPtr()); }
    QueueRecordHeader& next() { return *(QueueRecordHeader *)(nextPtr()); }
    const QueueRecordHeader& next() const { return *(const QueueRecordHeader *)(nextPtr()); }
    void *data() { return (uint8_t *)this + headerSize(); }
    const void *data() const { return (const uint8_t *)this + headerSize(); }

private:
    uintptr_t nextPtr() const {
        return (uintptr_t)this + totalSize();
    }
    uintptr_t prevPtr() const {
        return (uintptr_t)this - toPrev;
    }
} __attribute__((packed));

/**
 * A queue file with fixed-size records.
 *
 * Queue file format:
 *   1st QueueFileHeader
 *   2nd QueueFileHeader
 *   [QueueRecordHeader, byte array (record data)] * n
 *   QueueRecordHeader (end stub. for toPrev only)
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
        : mmappedFile_(getFileHeaderSize() + sizeof(QueueRecordHeader),
                       filePath, flags, mode)
        , lock_(filePath) {
        init(true);
    }
    ~QueueFile() noexcept {
        try {
            sync();
        } catch (...) {
        }
    }
    static uint64_t getFileHeaderSize() {
        return sizeof(QueueFileHeader) * 2;
    }

    void pushBack(const void *data, uint32_t size) {
        mmappedFile_.resize(getRequiredFileSize(size));
        QueueRecordHeader &rec = record(header_.endOffset);
        const uint32_t toPrev = rec.toPrev;
        rec.init();
        rec.dataSize = size;
        rec.checksum = calcChecksum(data, size, header_.salt);
        rec.toPrev = toPrev;
        rec.next().init();
        rec.next().toPrev = rec.totalSize();
        ::memcpy(rec.data(), data, size);
        header_.endOffset += rec.totalSize();
    }
    /**
     * T must be copyable.
     */
    template <typename T>
    void pushBack(const T& t) { pushBack(&t, sizeof(t)); }
    void pushBack(const std::string& s) { pushBack(&s[0], s.size()); }
    void pushBack(const char *s) { pushBack(std::string(s)); }
    void pushFront(const void *data, uint32_t size) {
        const uint32_t totalSize = sizeof(QueueRecordHeader) + size;
        reserveFrontSpace(totalSize);
        QueueRecordHeader &rec = record(header_.beginOffset);
        QueueRecordHeader &prev = record(header_.beginOffset - totalSize);
        prev.init();
        prev.dataSize = size;
        prev.checksum = calcChecksum(data, size, header_.salt);
        ::memcpy(prev.data(), data, size);
        rec.toPrev = totalSize;
        header_.beginOffset -= totalSize;
    }
    template <typename T>
    void pushFront(const T& t) { pushFront(&t, sizeof(t)); }
    void pushFront(const std::string& s) { pushFront(&s[0], s.size()); }
    void pushFront(const char *s) { pushFront(std::string(s)); }
    bool empty() const {
        return header_.beginOffset == header_.endOffset;
    }
    void front(void *data, uint32_t size) const {
        verifyNotEmpty(__func__);
        const QueueRecordHeader &rec = record(header_.beginOffset);
        verifySizeEquality(rec.dataSize, size, __func__);
        ::memcpy(data, rec.data(), size);
    }
    template <typename T>
    void front(T& data) const {
        front(&data, sizeof(data));
    }
    void front(std::string& s) const {
        const QueueRecordHeader &rec = record(header_.beginOffset);
        s.resize(rec.dataSize);
        ::memcpy(&s[0], rec.data(), s.size());
    }
    void back(void *data, uint32_t size) const {
        verifyNotEmpty(__func__);
        const QueueRecordHeader &rec = record(header_.endOffset).prev();
        verifySizeEquality(rec.dataSize, size, __func__);
        ::memcpy(data, rec.data(), size);
    }
    template <typename T>
    void back(T& data) const {
        back(&data, sizeof(data));
    }
    void back(std::string& s) const {
        const QueueRecordHeader &rec = record(header_.endOffset).prev();
        s.resize(rec.dataSize);
        ::memcpy(&s[0], rec.data(), s.size());
    }
    void popFront() {
        verifyNotEmpty(__func__);
        QueueRecordHeader &rec = record(header_.beginOffset);
        header_.beginOffset += rec.totalSize();
    }
    void popBack() {
        verifyNotEmpty(__func__);
        header_.endOffset -= record(header_.endOffset).toPrev;
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
        const uint64_t bgn = header_.beginOffset;
        const uint64_t end = header_.endOffset + sizeof(QueueRecordHeader);
        assert(bgn <= end);
        const uint64_t len = end - bgn;

        if (bgn < len) {
            /* There is not enough space. */
            return;
        }

        /* Sync current begin/end offset. */
        sync();
        /* Copy data. */
        ::memcpy(&record(0), &record(bgn), len);
        sync();
        /* Update the file header. */
        {
            // These must be atomic.
            header_.beginOffset = 0;
            header_.endOffset = len - sizeof(QueueRecordHeader);
        }
        sync();
        /* Truncate the file. */
        mmappedFile_.resize(getRequiredFileSize(0));
        sync();
    }
    void reserveFrontSpace(uint64_t size) {
        if (size <= header_.beginOffset) {
            /* Nothing to do */
            return;
        }

        const uint64_t bgn = header_.beginOffset;
        const uint64_t end = header_.endOffset + sizeof(QueueRecordHeader);
        assert(bgn <= end);
        const uint64_t len = end - bgn;
        const uint64_t newBgn = std::max(end, size);

        /* Sync current begin/end offset. */
        sync();
        /* Grow the file */
        mmappedFile_.resize(getFileHeaderSize() + newBgn + len);
        /* Copy data. */
        ::memcpy(&record(newBgn), &record(bgn), len);
        sync();
        /* Update the file header. */
        {
            // These must be atomic.
            header_.beginOffset = newBgn;
            header_.endOffset = newBgn + len - sizeof(QueueRecordHeader);
        }
        sync();
    }

    template <class It, class Qf, class RecHeader>
    class IteratorT
    {
    protected:
        Qf *qf_;
        uint64_t offset_;
    public:
        IteratorT(Qf *qf, uint64_t offset)
            : qf_(qf), offset_(offset) {
            assert(qf);
        }
        IteratorT(const It &rhs)
            : qf_(rhs.qf_), offset_(rhs.offset_) {}
        It &operator=(const It &rhs) const {
            qf_ = rhs.qf_;
            offset_ = rhs.offset_;
        }
        It &operator++() {
            RecHeader &rec = qf_->record(offset_);
            offset_ += rec.totalSize();
            return static_cast<It&>(*this);
        }
        It operator++(int) {
            It ret(qf_, offset_);
            ++(*this);
            return ret;
        }
        It &operator--() {
            RecHeader &rec = qf_->record(offset_);
            offset_ -= rec.toPrev;
            return static_cast<It&>(*this);
        }
        It operator--(int) {
            It ret(qf_, offset_);
            --(*this);
            return ret;
        }
        bool operator==(const It &rhs) const {
            return offset_ == rhs.offset_;
        }
        bool operator!=(const It &rhs) const {
            return offset_ != rhs.offset_;
        }
        bool isValid() const {
            const RecHeader &rec = qf_->record(offset_);
            if (!rec.isValid()) return false;
            const uint32_t csum = calcChecksum(rec.data(), rec.dataSize, qf_->header_.salt);
            if (csum != rec.checksum) return false;
            return true;
        }
        void get(std::string& s) const {
            const RecHeader &rec = qf_->record(offset_);
            s.resize(rec.dataSize);
            ::memcpy(&s[0], rec.data(), s.size());
        }
        void get(void *data, uint32_t size) const {
            const RecHeader &rec = qf_->record(offset_);
            qf_->verifySizeEquality(rec.dataSize, size, __func__);
            ::memcpy(data, rec.data(), size);
        }
        /* T must be copyable. */
        template <typename T>
        void get(T& t) const { get(&t, sizeof(t)); }
    };

    struct ConstIterator : public IteratorT<ConstIterator, const QueueFile, const QueueRecordHeader>
    {
        using IteratorT :: IteratorT;
    };
    ConstIterator cbegin() const { return ConstIterator(this, header_.beginOffset); }
    ConstIterator cend() const { return ConstIterator(this, header_.endOffset); }
    ConstIterator begin() const { return cbegin(); }
    ConstIterator end() const { return cend(); }

    /**
     * for test.
     */
    void verifyAll() const {
        // Forward
        uint64_t off = header_.beginOffset;
        while (off < header_.endOffset) {
            const QueueRecordHeader &rec = record(off);
            verifyRec(rec);
            off += rec.totalSize();
        }
        // Backward
        off = header_.endOffset;
        for (;;) {
            const QueueRecordHeader &rec = record(off);
            if (off != header_.endOffset) verifyRec(rec);
            if (off == header_.beginOffset) break;
            off -= rec.toPrev;
        }
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
            QueueRecordHeader &rec = record(0);
            rec.init();
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
    uint64_t getRequiredFileSize(uint64_t size) const {
        return sizeof(QueueFileHeader) * 2 + header_.endOffset /* current size except end mark. */
            + sizeof(QueueRecordHeader) + size /* for a record. */
            + sizeof(QueueRecordHeader); /* for end stub. */
    }
    void verifySizeEquality(uint32_t x, uint32_t y, const char *msg) const {
        if (x != y) {
            std::stringstream ss;
            ss << msg << ":data size differs:" << x << ":" << y;
            throw std::runtime_error(ss.str());
        }
    }
    void verifyNotEmpty(const std::string& msg) const {
        if (empty()) {
            throw std::runtime_error(msg + ":empty");
        }
    }
    void verifyRec(const QueueRecordHeader& rec) const {
        rec.verify();
        const uint32_t csum = calcChecksum(rec.data(), rec.dataSize, header_.salt);
        if (rec.checksum != csum) {
            std::stringstream ss;
            ss << "invalid checksum checksum:" << rec.checksum << ":" << csum;
            throw std::runtime_error(ss.str());
        }
    }
};

}} //namespace cybozu::util
