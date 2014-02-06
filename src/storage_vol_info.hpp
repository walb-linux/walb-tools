#pragma once
/**
 * @file
 * @brief Storage information management.
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
 * Persistent data for a volume managed by a storage daemon.
 *
 * queue file:
 *   must have at least one record.
 *
 * TODO: mutex.
 */
class StorageVolInfo
{
private:
    cybozu::FilePath volDir_; /* volume directory. */
    std::string volId_; /* volume identifier. */
    cybozu::FilePath wdevPath_; /* wdev path. */

public:
    /**
     * Volume directory must exist.
     */
    StorageVolInfo(const std::string &baseDirStr, const std::string &volId, const std::string &wdevPathName)
        : volDir_(cybozu::FilePath(baseDirStr) + volId)
        , volId_(volId)
        , wdevPath_(wdevPathName) {
        cybozu::FilePath basePath(baseDirStr);
        if (basePath.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:already exists") << basePath.str();
        }
        if (!wdevPath_.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:not found") << wdevPathName;
        }
        if (!basePath.mkdir()) {
            throw cybozu::Exception("StorageVolInfo:can't make directory") << basePath.str();
        }
    }
    /**
     * Volume directory must exist.
     */
    StorageVolInfo(const std::string &baseDirStr, const std::string &volId)
        : volDir_(cybozu::FilePath(baseDirStr) + volId)
        , volId_(volId)
        , wdevPath_() {
        cybozu::FilePath basePath(baseDirStr);
        if (!basePath.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:not exists") << basePath.str();
        }
        if (!basePath.stat().isDirectory()) {
            throw cybozu::Exception("StorageVolInfo:not directory") << basePath.str();
        }
        std::string s;
        loadFile("path", s);
        wdevPath_ = cybozu::FilePath(s);
        if (wdevPath_.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:not found") << wdevPath_.str();
        }
    }
    /**
     * You must call this before using.
     */
    void init() {
        if (volDir_.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:already exists") << volDir_.str();
        }
        if (!volDir_.mkdir()) {
            throw cybozu::Exception("StorageVolInfo:create directory failed") << volDir_.str();
        }

        {
            cybozu::FilePath queueFile = volDir_ + "queue";
            cybozu::util::QueueFile qf(queueFile.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
            qf.sync();
        }
        saveFile("path", wdevPath_.str());
        saveFile("state", "SyncReady");
        saveFile("done", ""); // TODO
        cybozu::Uuid uuid = generateUuid();
        saveFile("uuid", uuid);
    }
    /**
     * get status as a string vector.
     */
    std::vector<std::string> getStatusAsStrVec() const {
        std::vector<std::string> v;
        auto &fmt = cybozu::util::formatString;
        v.push_back(fmt("volId %s", volId_.c_str()));
        v.push_back(fmt("wdevPath %s", wdevPath_.cStr()));
        uint64_t sizeLb = 0; // TODO
        v.push_back(fmt("size %" PRIu64 "", sizeLb));
        std::string stateStr;
        loadFile("state", stateStr);
        v.push_back(fmt("state %s", stateStr.c_str()));
        uint64_t logFreeSpacePb = 0; // TODO
        v.push_back(fmt("logFreeSpace %" PRIu64 "", logFreeSpacePb));
        uint64_t logCapacityPb = 0; // TODO
        v.push_back(fmt("logCapacity %" PRIu64 "", logCapacityPb));
        cybozu::Uuid uuid;
        loadFile("uuid", uuid);
        v.push_back(fmt("uuid %s", uuid.str().c_str()));
        uint32_t pbs = 0;
        v.push_back(fmt("pbs %" PRIu32 "", pbs));
        uint32_t salt = 0; // TODO
        v.push_back(fmt("salt %" PRIu32 "", salt));

        // TODO

        // base <lsid> <gidB> <gidE> <canMerge> <timestamp>
        // snapshot <lsid> <gid> <gid> <canMerge> <timestamp>

        return v;
    }

private:
    template <typename T>
    void saveFile(const std::string &fname, const T &t) const {
        cybozu::TmpFile tmp(volDir_.str());
        cybozu::save(tmp, t);
        cybozu::FilePath path = volDir_ + fname;
        tmp.save(path.str());
    }
    template <typename T>
    void loadFile(const std::string &fname, T &t) const {
        cybozu::FilePath path = volDir_ + fname;
        cybozu::util::FileReader r(path.str(), O_RDONLY);
        cybozu::load(t, r);
    }
    cybozu::Uuid generateUuid() const {
        cybozu::Uuid uuid;
        cybozu::util::Random<uint64_t> rand;
        rand.fill(uuid.rawData(), uuid.rawSize());
        return uuid;
    }
#if 0
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
        rec.setSnap(gid0, gid1);
        rec.setLsid(lsid);
        rec.setTimestamp(ts);
        rec.setCanMerge(canMerge);

        addRecord(rec);
        return {gid0, gid1};
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
        qf.front(rec.raw);
        return rec;
    }
    void pop() {
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        qf.front(doneRec_.rawData(), doneRec_.rawSize());
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
            if (gid <= rec.gid0()) break;
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
        return v;
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
        cybozu::util::QueueFile::ConstIterator it = qf.cbegin();
        MetaSnap prev;
        while (!it.isEndMark()) {
            std::vector<uint8_t> v;
            MetaSnap rec;
            if (it.isValid() && !it.isDeleted()) {
                getRecordFromIterator(rec, it);
                if (!rec.isValid()) {
                    throw std::runtime_error("queue broken.");
                }
                if (it != qf.cbegin()) {
                    if (!(prev.gid1() == rec.gid0())) {
                        throw std::runtime_error("queue broken.");
                    }
                    if (!(prev.lsid() <= rec.lsid())) {
                        throw std::runtime_error("queue broken.");
                    }
                }
                assert(rec.gid0() < rec.gid1());
                nextGid_ = std::max(nextGid_, rec.gid1());
                nextLsid_ = std::max(nextLsid_, rec.lsid());
            }
            ++it;
            prev = rec;
        }
    }
    void initDoneRecord(uint64_t lsid) {
        doneRec_.init();
        doneRec_.setSnap(0);
        doneRec_.setTimestamp(::time(nullptr));
        doneRec_.setLsid(lsid);
        doneRec_.setCanMerge(true);
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
        if (nextGid_ != rec.gid0()) {
            throw std::runtime_error("addRecord: gid0 invalid.");
        }
        if (rec.lsid() < nextLsid_) {
            throw std::runtime_error("addREcord: lsid invalid.");
        }
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        qf.push(rec.rawData(), rec.rawSize());
        nextGid_ = rec.gid1();
        nextLsid_ = rec.lsid();
    }
    template <class QueueIterator>
    void getRecordFromIterator(MetaSnap &rec, QueueIterator &it) const {
        assert(!it.isEndMark());
        it.get(rec.rawData(), rec.rawSize());
    }
#endif
};

} //namespace walb
