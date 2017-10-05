#pragma once
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
#include "serializer.hpp"

namespace walb {
namespace queue_local {

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
    QueueRecordHeader& prev() { return *reinterpret_cast<QueueRecordHeader *>(prevPtr()); }
    const QueueRecordHeader& prev() const { return *reinterpret_cast<const QueueRecordHeader *>(prevPtr()); }
    QueueRecordHeader& next() { return *reinterpret_cast<QueueRecordHeader *>(nextPtr()); }
    const QueueRecordHeader& next() const { return *reinterpret_cast<const QueueRecordHeader *>(nextPtr()); }
    void *data() { return (uint8_t *)this + headerSize(); }
    const void *data() const { return (const uint8_t *)this + headerSize(); }

private:
    uintptr_t nextPtr() const {
        return ((uintptr_t)this) + totalSize();
    }
    uintptr_t prevPtr() const {
        return ((uintptr_t)this) - toPrev;
    }
} __attribute__((packed));

} // namespace queue_local

/**
 * Queue file format:
 *   1st QueueFileHeader
 *   2nd QueueFileHeader
 *   [QueueRecordHeader, byte array (record data)] * n
 *   QueueRecordHeader (end stub. for toPrev only)
 *
 * T must be serializable with cybozu::save() and cybozu::load().
 */
template <typename T>
class QueueFile /*final*/
{
private:
    mutable queue_local::QueueFileHeader header_;
    cybozu::util::MmappedFile mmappedFile_;
    cybozu::file::Lock lock_;

public:
    QueueFile(const std::string& filePath, int flags)
        : mmappedFile_(0, filePath, flags)
        , lock_(filePath) {
        init(false);
    }
    QueueFile(const std::string& filePath, int flags, int mode)
        : mmappedFile_(getFileHeaderSize() + sizeof(queue_local::QueueRecordHeader),
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
    void pushFront(const T& t) {
        std::string s;
        cybozu::saveToStr(s, t);
        pushFrontRaw(&s[0], s.size());
    }
    void pushBack(const T& t) {
        std::string s;
        cybozu::saveToStr(s, t);
        pushBackRaw(&s[0], s.size());
    }
    void front(T& t) {
        std::string s;
        frontRaw(s);
        cybozu::loadFromStr(t, s);
    }
    void back(T& t) {
        std::string s;
        backRaw(s);
        cybozu::loadFromStr(t, s);
    }
    void popFront() {
        verifyNotEmpty(__func__);
        queue_local::QueueRecordHeader &rec = record(header_.beginOffset);
        header_.beginOffset += rec.totalSize();
    }
    void popBack() {
        verifyNotEmpty(__func__);
        header_.endOffset -= record(header_.endOffset).toPrev;
    }
    bool empty() const {
        return header_.beginOffset == header_.endOffset;
    }
    void printOffsets() const {
        ::printf("begin %" PRIu64 " end %" PRIu64 "\n"
                 , header_.beginOffset, header_.endOffset); //debug
    }
    void clear() {
        header_.beginOffset = header_.endOffset;
        record(header_.endOffset).toPrev = 0;
    }
    void sync() {
        updateFileHeaderChecksum();
        /* Double write for atomicity. */
        for (int i = 0; i < 2; i++) {
            ::memcpy(ptrInFile(sizeof(header_) * i), &header_, sizeof(header_));
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
        const uint64_t end = header_.endOffset + sizeof(queue_local::QueueRecordHeader);
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
            header_.endOffset = len - sizeof(queue_local::QueueRecordHeader);
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
        const uint64_t end = header_.endOffset + sizeof(queue_local::QueueRecordHeader);
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
            header_.endOffset = newBgn + len - sizeof(queue_local::QueueRecordHeader);
        }
        sync();
    }

    template <class It, class Qf, class RecHeader>
    class IteratorBase
    {
    protected:
        Qf *qf_;
        uint64_t offset_;
    public:
        IteratorBase() : qf_(nullptr), offset_(-1) {}
        IteratorBase(Qf *qf, uint64_t offset)
            : qf_(qf), offset_(offset) {
            assert(qf);
        }
        IteratorBase(const It &rhs)
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
        It &operator--() {
            RecHeader &rec = qf_->record(offset_);
            offset_ -= rec.toPrev;
            return static_cast<It&>(*this);
        }
        bool operator==(const It &rhs) const {
            return qf_ == rhs.qf_ && offset_ == rhs.offset_;
        }
        bool operator!=(const It &rhs) const {
            return qf_ != rhs.qf_ || offset_ != rhs.offset_;
        }
        T operator*() const {
            T t;
            get(t);
            return t;
        }
        void get(T& t) const {
            std::string s;
            getRaw(s);
            cybozu::loadFromStr(t, s);
        }
        bool isValid() const {
            const RecHeader &rec = qf_->record(offset_);
            if (!rec.isValid()) return false;
            const uint32_t csum = cybozu::util::calcChecksum(rec.data(), rec.dataSize, qf_->header_.salt);
            if (csum != rec.checksum) return false;
            return true;
        }
    private:
        void getRaw(std::string& s) const {
            qf_->assignString(s, qf_->record(offset_));
        }
    };

    class ConstIterator : public IteratorBase<ConstIterator, const QueueFile, const queue_local::QueueRecordHeader>
    {
        using IteratorBase<ConstIterator, const QueueFile, const queue_local::QueueRecordHeader>::IteratorBase;
    };
    ConstIterator cbegin() const { return ConstIterator(this, header_.beginOffset); }
    ConstIterator cend() const { return ConstIterator(this, header_.endOffset); }

    /**
     * for test.
     */
    void verifyAll() const {
        // Forward
        uint64_t off = header_.beginOffset;
        while (off < header_.endOffset) {
            const queue_local::QueueRecordHeader &rec = record(off);
            verifyRec(rec);
            off += rec.totalSize();
        }
        // Backward
        off = header_.endOffset;
        for (;;) {
            const queue_local::QueueRecordHeader &rec = record(off);
            if (off != header_.endOffset) verifyRec(rec);
            if (off == header_.beginOffset) break;
            off -= rec.toPrev;
        }
    }
private:
    uint64_t getFileHeaderSize() const {
        return sizeof(queue_local::QueueFileHeader) * 2;
    }
    uint8_t *ptrInFile(size_t offset) {
        return mmappedFile_.ptr<uint8_t>() + offset;
    }
    const uint8_t *ptrInFile(size_t offset) const {
        return mmappedFile_.ptr<const uint8_t>() + offset;
    }
    void init(bool isCreated) {
        if (isCreated) {
            header_.preamble = queue_local::QUEUE_PREAMBLE;
            cybozu::util::Random<uint32_t> rand;
            header_.salt = rand();
            header_.beginOffset = 0;
            header_.endOffset = 0;
            queue_local::QueueRecordHeader &rec = record(0);
            rec.init();
            sync();
            return;
        }
        for (int i = 0; i < 2; i++) {
            /* Read i'th header data. */
            ::memcpy(&header_, ptrInFile(sizeof(header_) * i), sizeof(header_));
            if (isValidFileHeader()) {
                sync();
                return;
            }
        }
        throw std::runtime_error("Both header data broken.");
    }
    bool isValidFileHeader() const {
        if (header_.preamble != queue_local::QUEUE_PREAMBLE) {
            return false;
        }
        if (cybozu::util::calcChecksum(&header_, sizeof(header_), 0) != 0) {
            return false;
        }
        return true;
    }
    void updateFileHeaderChecksum() {
        header_.checksum = 0;
        header_.checksum = cybozu::util::calcChecksum(&header_, sizeof(header_), 0);
    }
    const queue_local::QueueRecordHeader& record(uint64_t offset) const {
        return *reinterpret_cast<const queue_local::QueueRecordHeader*>(
            ptrInFile(sizeof(queue_local::QueueFileHeader) * 2 + offset));
    }
    queue_local::QueueRecordHeader& record(uint64_t offset) {
        return *reinterpret_cast<queue_local::QueueRecordHeader*>(
            ptrInFile(sizeof(queue_local::QueueFileHeader) * 2 + offset));
    }
    uint64_t getRequiredFileSize(uint64_t size) const {
        return sizeof(queue_local::QueueFileHeader) * 2
            + header_.endOffset /* current size except end mark. */
            + sizeof(queue_local::QueueRecordHeader) + size /* for a record. */
            + sizeof(queue_local::QueueRecordHeader); /* for end stub. */
    }
    void verifyNotEmpty(const std::string& msg) const {
        if (empty()) {
            throw std::runtime_error(msg + ":empty");
        }
    }
    void verifyRec(const queue_local::QueueRecordHeader& rec) const {
        rec.verify();
        const uint32_t csum = cybozu::util::calcChecksum(rec.data(), rec.dataSize, header_.salt);
        if (rec.checksum != csum) {
            std::stringstream ss;
            ss << "invalid checksum checksum:" << rec.checksum << ":" << csum;
            throw std::runtime_error(ss.str());
        }
    }
    void assignString(std::string& s, const queue_local::QueueRecordHeader& rec) const {
        s.assign((const char *)rec.data(), rec.dataSize);
    }
    void pushBackRaw(const void *data, uint32_t size) {
        mmappedFile_.resize(getRequiredFileSize(size));
        queue_local::QueueRecordHeader &rec = record(header_.endOffset);
        const uint32_t toPrev = rec.toPrev;
        rec.init();
        rec.dataSize = size;
        rec.checksum = cybozu::util::calcChecksum(data, size, header_.salt);
        rec.toPrev = toPrev;
        rec.next().init();
        rec.next().toPrev = rec.totalSize();
        ::memcpy(rec.data(), data, size);
        header_.endOffset += rec.totalSize();
    }
    void pushFrontRaw(const void *data, uint32_t size) {
        const uint32_t totalSize = sizeof(queue_local::QueueRecordHeader) + size;
        reserveFrontSpace(totalSize);
        queue_local::QueueRecordHeader &rec = record(header_.beginOffset);
        queue_local::QueueRecordHeader &prev = record(header_.beginOffset - totalSize);
        prev.init();
        prev.dataSize = size;
        prev.checksum = cybozu::util::calcChecksum(data, size, header_.salt);
        ::memcpy(prev.data(), data, size);
        rec.toPrev = totalSize;
        header_.beginOffset -= totalSize;
    }
    void frontRaw(std::string& s) const {
        verifyNotEmpty(__func__);
        assignString(s, record(header_.beginOffset));
    }
    void backRaw(std::string& s) const {
        verifyNotEmpty(__func__);
        assignString(s, record(header_.endOffset).prev());
    }
};

} // namespace walb
