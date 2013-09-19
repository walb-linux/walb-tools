#pragma once
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
#include "tmp_file_serializer.hpp"
#include "meta.hpp"

namespace walb {

/**
 * Persistent data for a volume managed by a worker.
 *
 * queue file:
 *   must have at least one record.
 */
class WorkerData
{
private:
    cybozu::FilePath baseDir_; /* base directory. */
    std::string name_; /* volume identifier. */
    MetaSnap doneRec_;
    uint64_t nextGid_;
    uint64_t nextLsid_;
    uint64_t sizePb_;

public:
    /**
     * @sizePb [physical block].
     */
    WorkerData(const std::string &baseDirStr, const std::string &name, uint64_t sizePb)
        : baseDir_(baseDirStr)
        , name_(name)
        , doneRec_()
        , nextGid_(0)
        , nextLsid_(0)
        , sizePb_(sizePb) {
        if (!baseDir_.isFull()) {
            throw std::runtime_error("base directory must be full path.");
        }
        if (!baseDir_.stat().isDirectory()) {
            throw std::runtime_error("base directory does not exist.");
        }
        cybozu::FilePath dir = dirPath();
        if (!dir.stat().exists() && !dir.mkdir()) {
            throw std::runtime_error("target directory creation failed.");
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
        if (lsid < nextLsid_) throw std::runtime_error("bad lsid.");
        uint64_t n = 1 + (lsid - nextLsid_) / sizePb_;
        uint64_t gid0 = nextGid_;
        uint64_t gid1 = nextGid_ + n;

        /* Timestamp */
        time_t ts = ::time(nullptr);
        if (ts == time_t(-1)) std::runtime_error("time() failed.");

        /* Generate a record. */
        MetaSnap rec;
        rec.raw().gid0 = gid0;
        rec.raw().gid1 = gid1;
        rec.raw().lsid = lsid;
        rec.raw().timestamp = ts;
        rec.raw().can_merge = canMerge;

        addRecord(rec);
        return std::make_pair(gid0, gid1);
    }
    cybozu::FilePath dirPath() const {
        return baseDir_ + cybozu::FilePath(name_);
    }
    bool empty() const {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        return qf.empty();
    }
    MetaSnap front() const {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        MetaSnap rec;
        qf.front(rec.raw());
        return rec;
    }
    void pop() {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        qf.front(doneRec_.raw());
        qf.pop();
        saveDoneRecord();
    }
    /**
     * Remove old records which corresponding wlogs has been transferred.
     */
    void removeBefore(uint64_t gid) {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        cybozu::util::QueueFile::Iterator it = qf.begin();
        while (!it.isEndMark()) {
            if (it.isDeleted()) {
                ++it;
                continue;
            }
            MetaSnap rec;
            getRecordFromIterator(rec, it);
            if (gid <= rec.raw().gid0) break;
            it.setDeleted();
            doneRec_ = rec;
            ++it;
        }
        qf.gc();
        saveDoneRecord();
    }
    /**
     * for debug.
     */
    std::vector<MetaSnap> getAllRecords() const {
        std::vector<MetaSnap> v;
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        cybozu::util::QueueFile::ConstIterator it = qf.cbegin();
        while (!it.isEndMark()) {
            if (it.isDeleted()) {
                ++it;
                continue;
            }
            MetaSnap rec;
            getRecordFromIterator(rec, it);
            v.push_back(rec);
            ++it;
        }
        return std::move(v);
    }
private:
    cybozu::FilePath doneRecordPath() const {
        return dirPath() + cybozu::FilePath("done");
    }
    cybozu::FilePath queuePath() const {
        return dirPath() + cybozu::FilePath("queue");
    }
    /**
     * Scan the queue and set nextGid_ and nextLsid_.
     */
    void initScanQueue(cybozu::util::QueueFile &qf) {
        qf.gc();
        cybozu::util::QueueFile::ConstIterator it = qf.begin();
        MetaSnap prev;
        while (!it.isEndMark()) {
            std::vector<uint8_t> v;
            MetaSnap rec;
            if (it.isValid() && !it.isDeleted()) {
                getRecordFromIterator(rec, it);
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
                assert(rec.raw().gid0 < rec.raw().gid1);
                nextGid_ = std::max(nextGid_, rec.raw().gid1);
                nextLsid_ = std::max(nextLsid_, rec.raw().lsid);
            }
            ++it;
            prev = rec;
        }
    }
    void initDoneRecord(uint64_t lsid) {
        doneRec_.init();
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
        cybozu::util::FileReader reader(doneRecordPath().str(), O_RDONLY);
        cybozu::load(doneRec_, reader);
        if (!doneRec_.isValid()) {
            throw std::runtime_error("read done record is not valid.");
        }
    }
    void saveDoneRecord() const {
        cybozu::TmpFile tmpFile(dirPath().str());
        cybozu::save(tmpFile, doneRec_);
        tmpFile.save(doneRecordPath().str());
    }
    void addRecord(const MetaSnap &rec) {
        assert(rec.isValid());
        if (nextGid_ != rec.raw().gid0) {
            throw std::runtime_error("addRecord: gid0 invalid.");
        }
        if (rec.raw().lsid < nextLsid_) {
            throw std::runtime_error("addREcord: lsid invalid.");
        }
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        qf.push(rec.rawData(), rec.rawSize());
        nextGid_ = rec.raw().gid1;
        nextLsid_ = rec.raw().lsid;
    }
    template <class QueueIterator>
    void getRecordFromIterator(MetaSnap &rec, QueueIterator &it) const {
        assert(!it.isEndMark());
        std::vector<uint8_t> v;
        it.get(v);
        rec.load(&v[0], v.size());
    }
};

} //namespace walb
