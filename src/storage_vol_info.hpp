#pragma once
#include <cassert>
#include <cstring>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "meta.hpp"
#include "uuid.hpp"
#include "walb_util.hpp"
#include "wdev_util.hpp"

namespace walb {

const char *const sClear = "Clear";
const char *const sSyncReady = "SyncReady";
const char *const sStopped = "Stopped";
const char *const sMaster = "Master";
const char *const sSlave = "Slave";

// temporary state
const char *const stInitVol = "InitVol";
const char *const stClearVol = "ClearVol";
const char *const stStartSlave = "StartSlave";
const char *const stStopSlave = "StopSlave";
const char *const stFullSync = "FullSync";
const char *const stHashSync = "HashSync";
const char *const stStartMaster = "StartMaster";
const char *const stStopMaster = "StopMaster";
const char *const stReset = "Reset";
const char *const stWlogSend = "WlogSend";
const char *const stWlogRemove = "WlogRemove";


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
     * For initialization.
     */
    StorageVolInfo(const std::string &baseDirStr, const std::string &volId, const std::string &wdevPathName)
        : volDir_(cybozu::FilePath(baseDirStr) + volId)
        , volId_(volId)
        , wdevPath_(wdevPathName) {
        verifyBaseDirExistance(baseDirStr);
        verifyWdevPathExistance();
    }
    /**
     * If volume directory does not exist, only existsVolDir() can be called.
     */
    StorageVolInfo(const std::string &baseDirStr, const std::string &volId)
        : volDir_(cybozu::FilePath(baseDirStr) + volId)
        , volId_(volId)
        , wdevPath_() {
        verifyBaseDirExistance(baseDirStr);
        if (!existsVolDir()) return;
        loadWdevPath();
        verifyWdevPathExistance();
    }
    /**
     * Initialize the volume information directory.
     */
    void init() {
        LOGd("volDir %s volId %s", volDir_.cStr(), volId_.c_str());
        util::makeDir(volDir_.str(), "StorageVolInfo", true);
        {
            cybozu::FilePath queueFile = volDir_ + "queue";
            cybozu::util::QueueFile qf(queueFile.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
            qf.sync();
        }
        util::saveFile(volDir_, "path", wdevPath_.str());
        setState(sSyncReady);
        util::saveFile(volDir_, "done", ""); // TODO
        util::saveFile(volDir_, "uuid", cybozu::Uuid());
    }
    /**
     * Clear all the volume information.
     * The directory will be deleted completely.
     * The instance will be invalid after calling this.
     */
    void clear() {
        if (!volDir_.rmdirRecursive()) {
            throw cybozu::Exception("StorageVolInfo::clear:rmdir recursively failed.");
        }
    }
    bool existsVolDir() const {
        return volDir_.stat(true).isDirectory();
    }
    /**
     * get status as a string vector.
     */
    std::vector<std::string> getStatusAsStrVec() const {
        std::vector<std::string> v;
        if (!existsVolDir()) return v;

        auto &fmt = cybozu::util::formatString;
        v.push_back(fmt("volId %s", volId_.c_str()));
        v.push_back(fmt("wdevPath %s", wdevPath_.cStr()));
        uint64_t sizeLb = 0; // TODO
        v.push_back(fmt("size %" PRIu64 "", sizeLb));
        const std::string stateStr = getState();
        v.push_back(fmt("state %s", stateStr.c_str()));
        uint64_t logFreeSpacePb = 0; // TODO
        v.push_back(fmt("logFreeSpace %" PRIu64 "", logFreeSpacePb));
        uint64_t logCapacityPb = 0; // TODO
        v.push_back(fmt("logCapacity %" PRIu64 "", logCapacityPb));
        const cybozu::Uuid uuid = getUuid();
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
    std::string getState() const {
        std::string ret;
        util::loadFile(volDir_, "state", ret);
        return ret;
    }
    void setState(const std::string& newState)
    {
        const char *tbl[] = {
            sSyncReady,
            sStopped,
            sMaster,
            sSlave,
        };
        for (const char *p : tbl) {
            if (newState == p) {
                util::saveFile(volDir_, "state", newState);
                return;
            }
        }
        throw cybozu::Exception("StorageVolInfo::setState:bad state") << newState;
    }
    void resetWlog(uint64_t gid)
    {
        cybozu::disable_warning_unused_variable(gid);
        // TODO
        resetWal(wdevPath_.str());
    }
    cybozu::Uuid getUuid() const {
        cybozu::Uuid uuid;
        util::loadFile(volDir_, "uuid", uuid);
        return uuid;
    }
    std::string getWdevPath() const { return wdevPath_.str(); }

private:
    void loadWdevPath() {
        std::string s;
        util::loadFile(volDir_, "path", s);
        wdevPath_ = cybozu::FilePath(s);
    }
    void verifyWdevPathExistance() {
        if (!wdevPath_.stat(true).exists()) {
            throw cybozu::Exception("StorageVolInfo:not found") << wdevPath_.str();
        }
    }
    void verifyBaseDirExistance(const std::string &baseDirStr) {
        cybozu::FilePath baseDir(baseDirStr);
        if (!baseDir.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:not exists") << baseDir.str();
        }
        if (!baseDir.stat().isDirectory()) {
            throw cybozu::Exception("StorageVolInfo:not directory") << baseDir.str();
        }
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
