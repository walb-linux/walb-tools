/**
 * @file
 * @brief Meta snapshot and diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <time.h>
#include "queue_file.hpp"

#ifndef WALB_TOOLS_WORKER_DATA_HPP
#define WALB_TOOLS_WORKER_DATA_HPP

struct snapshot_record
{
    /* The record covers gid0_ <= gid < gid1_ range. */
    uint64_t gid0;
    uint64_t gid1;
    uint64_t lsid;
    uint64_t timestamp; /* unix time (sec). */
    uint8_t can_merge; /* zero means that gid0 must be kept. */
    uint8_t reserved0;
    uint16_t reserved1;
    uint32_t reserved2;
} __attribute__((packed));

class SnapshotRecord
{
private:
    struct snapshot_record rec_;

public:
    size_t rawSize() const { return sizeof(rec_); }
    struct void *rawData() { return reinterpret_cast<void *>(&rec_); }
    const void *rawData() const { return reinterpret_cast<const void *>(&rec_); }
    struct snapshot_record *raw() { return &rec_; }
    const struct snapshot_record *raw() const { return &rec_; }
    bool isValid() const {
        if (!(raw().gid0 <= raw().gid1)) {
            return false;
        }
        return true;
    }
    /**
     * If empty() is true, the snapshot record has no meaning.
     */
    bool empty() const { return raw().gid0 == raw().gid1; }
    /**
     * If canMerge() is false, we must separate wdiffs with gid0.
     */
    bool canMerge() const { return raw().can_merge; }
    bool load(const void *data, size_t size) {
        if (sizeof(rec_) != size) return false;
        ::memcpy(&rec_, data, sizeof(rec));
        return true;
    }
};

/**
 * Persistent data for a volume managed by a worker.
 */
class WorkerData
{
private:
    std::string baseDir_; /* base directory. */
    std::string name_; /* volume identifier. */
    uint64_t nextGid_;
    uint64_t lsid_;
    uint64_t sizePb_;

public:
    /**
     * @sizePb [physical block].
     */
    WorkerData(const std::string &baseDir, const std::string &name, uint64_t uint64_t sizePb)
        : baseDir_(baseDir)
        , name_(name)
        , nextGid_(0)
        , lsid_(0)
        , sizePb_(sizePb) {
        assert(0 < sizePb);
    }
    /**
     * You must call this before using.
     * @lsid the oldest lsid of the walb device.
     */
    void init(uint64_t lsid) {
        cybozu::FilePath qp = queuePath();
        if (!qp.stat().exists()) {
            cybozu::util::QueueFile qf(qp.str(), O_CREAT | O_TRUNC | O_RDRW, 0644);
            initAddQueue(qf, lsid);
        } else {
            cybozu::util::QueueFile qf(qp.str(), O_RDRW);
            initScanQueue(qf);
        }
    }
    /**
     * @sizePb [physical block]
     */
    void setLogSizePerDiff(uint64_t sizePb) {
        assert(0 < sizePb);
        sizePb_ = sizePb;
    }
    /**
     * take a snapshot.
     *
     * RETURN:
     *   gid range.
     */
    std::pair<uint64_t, uint64_t> takeSnapshot(uint64_t lsid, bool canMerge) {
        /* Determine gid range. */
        if (lsid < lsid_) {
            throw std::runtime_error("bad lsid.");
        }
        uint64_t n = 1 + (lsid - lsid_) / sizePb_;
        uint64_t gid0 = nextGid_;
        uint64_t gid1 = nextGid_ + n;
        nextGid_ += gid1;
        lsid_ = lsid;

        /* Timestamp */
        time_t ts = ::time(nullptr);
        if (ts == time_t(-1)) {
            std::runtime_error("time() failed.");
        }

        /* Generate a record. */
        SnapshotRecord rec;
        rec.raw().gid0 = gid0;
        rec.raw().gid1 = gid1;
        rec.raw().lsid = lsid;
        rec.raw().timestamp = ts;
        rec.raw().can_merge = canMerge;

        if (!addRecord(rec)) {
            throw std::runtime_error("addRecord() failed.");
        }
        return std::make_pair(gid0, gid1);
    }
    /**
     * Add a snapshot record.
     */
    bool addRecord(const SnapshotRecord &rec) {
        assert(rec.isValid());
        if (nextGid_ != rec.raw().gid0) {
            return false;
        }
        if (rec.raw().lsid < lsid_) {
            return false;
        }
        cybozu::util::QueueFile qf(queuePath().str(), O_RDRW);
        qf.push(rec.rawData(), rec.rawSize());
        nextGid_ = rec.raw().gid1;
        lsid_ = rec.raw().lsid;
        return true;
    }
    std::string dirPath() const {
        return (cybozu::FilePath(baseDir_) + cybozu::FilePath(name_)).str();
    }
    bool empty() const {
        cybozu::QueueFile qf(queuePath().str(), O_RDRW);
        return qf.empty();
    }
    SnapshotRecord front() const {
        cybozu::QueueFile qf(queuePath().str(), O_RDRW);
        std::vector<uint8_t> v;
        qf.front(v);
        SnapshotRecord rec;
        rec.load(&v[0], v.size());
        return rec;
    }
    void pop() {
        cybozu::QueueFile qf(queuePath().str(), O_RDRW);
        qf.pop();
    }
    /**
     * Remove old records which corresponding wlogs has been transferred.
     */
    void removeBefore(uint64_t gid) {
        cybozu::QueueFile qf(queuePath().str(), O_RDRW);
        cybozu::QueueFile::Iterator it = qf.begin();
        while (!it.isEndMark()) {
            if (!it.isDeleted()) {
                SnapshotRecord rec;
                getRecordFromIterator(rec, it);
                if (gid <= rec.raw().gid0) {
                    break;
                }
                it.setDeleted();
            }
            ++it;
        }
        qf.gc();
    }
    void getRecordFromIterator(SnapshotRecord &rec, cybozu::QueueFile::Iterator &it) {
        assert(!it.isEndMark());
        std::vector<uint8_t> v;
        it.get(v);
        rec.load(&v[0], v.size());
    }
private:
    cybozu::FilePath queuePath() const {
        return cybozu::FilePath(dirPath()) + cybozu::FilePath("queue");
    }
    /**
     * Scan the queue and set nextGid_ and lsid_.
     */
    void initScanQueue(cybozu::util::QueueFile &qf) {
        cybozu::util::QueueFile qf(qp.str(), O_RDRW);
        qf.gc();
        cybozu::util::QueueFile::ConstIterator it = qf.begin();
        nextGid_ = 0;
        lsid_ = 0;
        while (it != qf.cend()) {
            std::vector<uint8_t> v;
            SnapshotRecord rec;
            if (it.isValid() && !it.isDeleted()) {
                it.get(v);
                rec.load(&v[0], v.size());
                if (!rec.isValid()) {
                    throw std::runtime_error("snapshot record invalid.");
                }
                if (!rec.empty()) {
                    nextGid_ = std::max(nextGid_, rec.raw().gid1);
                    lsid_ = std::max(lsid_, rec.raw().lsid);
                }
            }
            ++it;
        }
    }
    /**
     * Add an initial record.
     */
    void initAddQueue(cybozu::util::QueueFile &qf, uint64_t lsid) {
        SnapshotRecord rec;
        rec.raw().gid0 = 0;
        rec.raw().gid1 = 0;
        rec.raw().timestamp = ::time(nullptr);
        rec.raw().lsid = lsid;
        rec.raw().can_merge = true;
        nextGid_ = 0;
        lsid_ = lsid;
        qf.push(rec.rawData(), rec.rawSize());
        qf.sync();
    }
};

#endif /* WALB_TOOLS_WORKER_DATA_HPP */
