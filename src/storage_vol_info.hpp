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
#include "wdev_log.hpp"

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
    StorageVolInfo(const std::string &baseDirStr, const std::string &volId, const std::string &wdevPath)
        : volDir_(cybozu::FilePath(baseDirStr) + volId)
        , volId_(volId)
        , wdevPath_(wdevPath) {
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
            cybozu::util::QueueFile qf(queuePath().str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
            qf.sync();
        }
        util::saveFile(volDir_, "path", wdevPath_.str());
        setState(sSyncReady);
        const uint64_t lsid = 0; // TODO
        const uint64_t gid = 0; // TODO
        MetaLsidGid doneRec(lsid, gid, false, 0);
        setDoneRecord(doneRec);
        setUuid(cybozu::Uuid());
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
        return volDir_.stat().isDirectory();
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
        device::resetWal(wdevPath_.str());
        setDoneRecord(MetaLsidGid(0, gid, false, ::time(0)));
        {
            cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
            qf.clear();
            qf.sync();
        }
        {
            cybozu::Uuid uuid;
            cybozu::util::BlockDevice bd = device::getWldev(getWdevName());
            device::SuperBlock super(bd);
            uuid.set(super.getUuid());
            setUuid(uuid);
        }
    }
    cybozu::Uuid getUuid() const {
        cybozu::Uuid uuid;
        util::loadFile(volDir_, "uuid", uuid);
        return uuid;
    }
    void setUuid(const cybozu::Uuid &uuid) {
        util::saveFile(volDir_, "uuid", uuid);
    }
    std::string getWdevPath() const { return wdevPath_.str(); }
    std::string getWdevName() const {
        return device::getWdevNameFromWdevPath(wdevPath_.str());
    }
    /**
     * Take a snapshot by pushing a record to the queue file.
     *
     * @isMergeable
     *   true, the snapshot will be removed by merging diffs.
     * @maxWlogSendMb
     *   maximum wlog size to send at once [MiB]
     *
     * RETURN:
     *   gid of the snapshot.
     */
    uint64_t takeSnapshot(bool isMergeable, uint64_t maxWlogSendMb) {
        const char *const FUNC = __func__;
        const uint32_t pbs = device::getWldev(getWdevName()).getPhysicalBlockSize();
        const uint64_t maxWlogSendPb = maxWlogSendMb * (MEBI / pbs);
        if (maxWlogSendPb == 0) {
            throw cybozu::Exception(FUNC) << "maxWlogSendPb must be positive";
        }
        cybozu::util::QueueFile qf(queuePath().str(), O_RDWR);
        MetaLsidGid pre;
        if (qf.empty()) {
            pre = getDoneRecord();
        } else {
            qf.front(pre);
            pre.verify();
        }
        const std::string wdevPath = wdevPath_.str();
        const uint64_t lsid = device::getPermanentLsid(wdevPath);
        if (device::isOverflow(wdevPath)) {
            throw cybozu::Exception(FUNC) << "wlog overflow" << wdevPath;
        }
        if (pre.lsid > lsid) {
            throw cybozu::Exception(FUNC) << "invalid lsid" << pre.lsid << lsid;
        }
        const uint64_t gid = pre.gid + (lsid - pre.lsid + maxWlogSendPb) / maxWlogSendPb;
        assert(pre.gid < gid);
        qf.pushFront(MetaLsidGid(lsid, gid, isMergeable, ::time(0)));
        qf.sync();
        return gid;
    }
private:
    void loadWdevPath() {
        std::string s;
        util::loadFile(volDir_, "path", s);
        wdevPath_ = cybozu::FilePath(s);
    }
    void verifyWdevPathExistance() {
        if (!wdevPath_.stat().exists()) {
            throw cybozu::Exception("StorageVolInfo:not found") << wdevPath_.str();
        }
    }
    void verifyBaseDirExistance(const std::string &baseDirStr) {
        cybozu::FilePath baseDir(baseDirStr);
        cybozu::FileStat stat = baseDir.stat();
        if (!stat.exists()) {
            throw cybozu::Exception("StorageVolInfo:not exists") << baseDir.str();
        }
        if (!stat.isDirectory()) {
            throw cybozu::Exception("StorageVolInfo:not directory") << baseDir.str();
        }
    }
    void setDoneRecord(const MetaLsidGid &rec) {
        util::saveFile(volDir_, "done", rec);
    }
    MetaLsidGid getDoneRecord() const {
        MetaLsidGid rec;
        util::loadFile(volDir_, "done", rec);
        return rec;
    }
    cybozu::FilePath queuePath() const {
        return volDir_ + "queue";
    }
#if 0 // XXX
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
#endif // XXX
};

} //namespace walb
