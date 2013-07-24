/**
 * @file
 * @brief Worker data management.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"

#ifndef WALB_TOOLS_WORKER_DATA_HPP
#define WALB_TOOLS_WORKER_DATA_HPP

namespace walb {

/**
 * Snapshot record.
 */
struct snapshot_record
{
    /* The record covers gid0_ <= gid < gid1_ range.
     * A worker can split wlogs using the gid range.
     * wlog-to-wdiff transfer requires wlog
     */
    uint64_t gid0;
    uint64_t gid1;
    uint64_t lsid;
    uint64_t timestamp; /* unix time (sec). */
    uint8_t can_merge; /* zero means that gid0 must be kept. */
    uint8_t reserved0;
    uint16_t reserved1;
    uint32_t reserved2;
} __attribute__((packed));

/**
 * Snapshot record wrapper.
 */
class SnapshotRecord
{
private:
    struct snapshot_record rec_;

public:
    size_t rawSize() const { return sizeof(rec_); }
    void *rawData() { return reinterpret_cast<void *>(&rec_); }
    const void *rawData() const { return reinterpret_cast<const void *>(&rec_); }
    struct snapshot_record &raw() { return rec_; }
    const struct snapshot_record &raw() const { return rec_; }
    bool isValid() const {
        return raw().gid0 <= raw().gid1;
    }
    /**
     * If empty() is true, the snapshot record has no meaning.
     */
    bool empty() const {
        return raw().gid0 == raw().gid1;
    }
    bool load(const void *data, size_t size) {
        if (sizeof(rec_) != size) return false;
        ::memcpy(&rec_, data, sizeof(rec_));
        return true;
    }
    template <class InputStream>
    void load(InputStream &in) {
        cybozu::loadPod(raw(), in);
    }
    template <class OutputStream>
    void save(OutputStream &out) const {
        cybozu::savePod(out, raw());
    }
};

/**
 * Persistent data for a volume managed by a worker.
 *
 * queue file:
 *   must have at least one record.
 */
class WorkerData
{
private:
    std::string baseDir_; /* base directory. */
    std::string name_; /* volume identifier. */
    SnapshotRecord doneRec_;
    uint64_t nextGid_;
    uint64_t nextLsid_;
    uint64_t sizePb_;

public:
    /**
     * @sizePb [physical block].
     */
    WorkerData(const std::string &baseDir, const std::string &name, uint64_t sizePb)
        : baseDir_(baseDir)
        , name_(name)
        , doneRec_()
        , nextGid_(0)
        , nextLsid_(0)
        , sizePb_(sizePb) {
        cybozu::FilePath fp(baseDir_);
        if (!fp.isFull()) {
            throw std::runtime_error("base directory must be full path.");
        }
        if (!fp.stat().isDirectory()) {
            throw std::runtime_error("base directory does not exist.");
        }
        if (sizePb == 0) {
            throw std::runtime_error("sizePb must be positive.");
        }
    }
    /**
     * You must call this before using.
     * @lsid the oldest lsid of the walb device.
     */
    void init(uint64_t lsid) {
        cybozu::FilePath rp = doneRecordPath();
        cybozu::FilePath qp = queuePath();
        if (!rp.stat().exists() || !qp.stat().exists()) {
            initDoneRecord(lsid);
            cybozu::util::QueueFile qf(qp.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
            qf.sync();
        } else {
            loadDoneRecord();
            cybozu::util::QueueFile qf(qp.str(), O_RDWR);
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
        if (lsid < nextLsid_) {
            throw std::runtime_error("bad lsid.");
        }
        uint64_t n = 1 + (lsid - nextLsid_) / sizePb_;
        uint64_t gid0 = nextGid_;
        uint64_t gid1 = nextGid_ + n;
        nextGid_ += gid1;
        nextLsid_ = lsid;

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
        if (rec.raw().lsid < nextLsid_) {
            return false;
        }
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        qf.push(rec.rawData(), rec.rawSize());
        nextGid_ = rec.raw().gid1;
        nextLsid_ = rec.raw().lsid;
        return true;
    }
    std::string dirPath() const {
        return (cybozu::FilePath(baseDir_) + cybozu::FilePath(name_)).str();
    }
    bool empty() const {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        return qf.empty();
    }
    SnapshotRecord front() const {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        std::vector<uint8_t> v;
        qf.front(v);
        SnapshotRecord rec;
        rec.load(&v[0], v.size());
        return rec;
    }
    void pop() {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        qf.pop();
    }
    /**
     * Remove old records which corresponding wlogs has been transferred.
     */
    void removeBefore(uint64_t gid) {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        cybozu::util::QueueFile::Iterator it = qf.begin();
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
    void getRecordFromIterator(SnapshotRecord &rec, cybozu::util::QueueFile::Iterator &it) {
        assert(!it.isEndMark());
        std::vector<uint8_t> v;
        it.get(v);
        rec.load(&v[0], v.size());
    }
private:
    cybozu::FilePath doneRecordPath() const {
        return cybozu::FilePath(dirPath()) + cybozu::FilePath("done");
    }
    cybozu::FilePath queuePath() const {
        return cybozu::FilePath(dirPath()) + cybozu::FilePath("queue");
    }
    /**
     * Scan the queue and set nextGid_ and nextLsid_.
     */
    void initScanQueue(cybozu::util::QueueFile &qf) {
        qf.gc();
        cybozu::util::QueueFile::ConstIterator it = qf.begin();
        SnapshotRecord prev;
        while (!it.isEndMark()) {
            std::vector<uint8_t> v;
            SnapshotRecord rec;
            if (it.isValid() && !it.isDeleted()) {
                it.get(v);
                rec.load(&v[0], v.size());
                if (!rec.isValid()) {
                    throw std::runtime_error("queue broken.");
                }
                if (it != qf.begin()) {
                    if (!(prev.raw().gid1 == rec.raw().gid0)) {
                        throw std::runtime_error("queue broken.");
                    }
                    if (!(prev.raw().lsid <= rec.raw().lsid)) {
                        throw std::runtime_error("queue broken.");
                    }
                }
                assert(!rec.empty());
                nextGid_ = std::max(nextGid_, rec.raw().gid1);
                nextLsid_ = std::max(nextLsid_, rec.raw().lsid);
            }
            ++it;
            prev = rec;
        }
    }
    void initDoneRecord(uint64_t lsid) {
        doneRec_.raw().gid0 = 0;
        doneRec_.raw().gid1 = 0;
        doneRec_.raw().timestamp = ::time(nullptr);
        doneRec_.raw().lsid = lsid;
        doneRec_.raw().can_merge = true;
        saveDoneRecord();
        nextGid_ = 0;
        nextLsid_ = lsid;
    }
    void loadDoneRecord() {
        cybozu::util::FileOpener fo(doneRecordPath().str(), O_RDONLY);
        cybozu::util::FdReader fdr(fo.fd());
        fdr.read(doneRec_.rawData(), doneRec_.rawSize());
        if (!doneRec_.isValid()) {
            throw std::runtime_error("read done record is not valid.");
        }
    }
    void saveDoneRecord() const {
        cybozu::TmpFile tmpF(dirPath());
        cybozu::util::FdWriter fdw(tmpF.fd());
        fdw.write(doneRec_.rawData(), doneRec_.rawSize());
        tmpF.save(doneRecordPath().str());
    }
};

} //namespace walb

#endif /* WALB_TOOLS_WORKER_DATA_HPP */
