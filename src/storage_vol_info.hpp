#pragma once
#include <cassert>
#include <cstring>
#include <time.h>
#include "cybozu/serializer.hpp"
#include "walb_queue_file.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "meta.hpp"
#include "uuid.hpp"
#include "walb_util.hpp"
#include "wdev_util.hpp"
#include "wdev_log.hpp"
#include "storage_constant.hpp"


namespace walb {


/**
 * Persistent data for a volume managed by a storage daemon.
 *
 * queue file:
 *   must have at least one record.
 */
class StorageVolInfo
{
private:
    cybozu::FilePath volDir_; /* volume directory. */
    std::string volId_; /* volume identifier. */
    cybozu::FilePath wdevPath_; /* wdev path. */

    using QFile = QueueFile<MetaLsidGid>;

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
    void init();
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
    const cybozu::FilePath& getVolDir() const {
        return volDir_;
    }
    /**
     * get status as a string vector.
     */
    StrVec getStatusAsStrVec(bool isVerbose) const;
    std::string getState() const {
        std::string ret;
        util::loadFile(volDir_, "state", ret);
        return ret;
    }
    void setState(const std::string& newState);
    void resetWlog(uint64_t gid);
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
     * @maxWlogSendMb
     *   maximum wlog size to send at once [MiB]
     *
     * RETURN:
     *   gid of the snapshot.
     */
    uint64_t takeSnapshot(uint64_t maxWlogSendMb) {
        const char *const FUNC = __func__;
        const uint64_t maxWlogSendPb = getMaxWlogSendPb(maxWlogSendMb, FUNC);
        QFile qf(queuePath().str(), O_RDWR);
        return takeSnapshotDetail(maxWlogSendPb, false, qf, device::getLatestLsid(getWdevPath()));
    }
    /**
     * Delete garbage wlogs if necessary.
     *
     * RETURN:
     *   true if there are wlogs to be deleted later.
     */
    bool deleteGarbageWlogs();
    /**
     * Delete all the wlogs.
     */
    void deleteAllWlogs();

    /**
     * Calling order:
     *   (1) deleteGarbageWlogs()
     *   (2) mayWlogTransferBeRequiredNow()
     *   (3) prepareWlogTransfer()
     *   (4) getTransferDiff()
     *   (5) finishWlogTransfer()
     *   (6) deleteGarbageWlogs()
     */
    bool mayWlogTransferBeRequiredNow(uint64_t& remainingMb) {
        return isWlogTransferRequiredDetail(false, remainingMb);
    }
    bool isWlogTransferRequiredLater(uint64_t& remainingMb) {
        return isWlogTransferRequiredDetail(true, remainingMb);
    }
    /**
     * @maxWlogSendMb
     *   maximum transferring size per once [MiB].
     * @intervalSec
     *   implicit snapshot interval [sec].
     *
     * RETURN:
     *   target lsid/gid range by two MetaLsidGids: recB and recE,
     *   and lsidLimit as uint64_t value,
     *   and boolean value which is true if we must pospone the wlog-transfer,
     *   and remaining log size to send in MiB.
     *   Do not transfer logpacks which lsid >= lsidLimit.
     */
    std::tuple<MetaLsidGid, MetaLsidGid, uint64_t, bool, uint64_t> prepareWlogTransfer(uint64_t maxWlogSendMb, size_t intervalSec);
    /**
     * RETURN:
     *   generated diff will be transferred to a proxy daemon.
     */
    MetaDiff getTransferDiff(const MetaLsidGid &recB, const MetaLsidGid &recE, uint64_t lsidE) const;
    /**
     * recB and recE must not be changed between calling
     * prepareWlogTransfer() and finishWlogTransfer().
     *
     * RETURN:
     *   true if there is remaining wlogs (that may be empty).
     */
    bool finishWlogTransfer(const MetaLsidGid &recB, const MetaLsidGid &recE, uint64_t lsidE);
    /**
     * Get the oldest and the latest gid.
     */
    std::pair<uint64_t, uint64_t> getGidRange() const;
    /**
     * Get the latest snapshot info.
     */
    MetaLsidGid getLatestSnap() const;
    /**
     * @sizeLb 0 can be specified (auto-detect).
     */
    void growWdev(uint64_t sizeLb = 0);
    uint32_t getPbs() const {
        cybozu::util::File file = device::getWldevFile(getWdevName());
        return cybozu::util::getPhysicalBlockSize(file.fd());
    }

    // debug code.
    std::vector<MetaLsidGid> getAllInQueue() const {
        std::vector<MetaLsidGid> v;
        QFile qf(queuePath().str(), O_RDWR);
        QFile::ConstIterator itr = qf.cbegin();
        while (itr != qf.cend()) {
            const MetaLsidGid rec = *itr;
            v.push_back(rec);
            ++itr;
        }
        return v;
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
    void verifyBaseDirExistance(const std::string &baseDirStr);
    void setDoneRecord(const MetaLsidGid &rec) {
        util::saveFile(volDir_, "done", rec);
    }
    MetaLsidGid getDoneRecord() const {
        MetaLsidGid rec;
        util::loadFile(volDir_, "done", rec);
        return rec;
    }
    uint64_t getDoneLsid() const {
        const uint64_t doneLsid = getDoneRecord().lsid;
        if (doneLsid == INVALID_LSID) {
            throw cybozu::Exception("StorageVolInfo:doneLsid is invalid");
        }
        return doneLsid;
    }
    cybozu::FilePath queuePath() const {
        return volDir_ + "queue";
    }
    uint64_t convertMibToPb(uint64_t mib) const {
        return mib * (MEBI / getPbs());
    }
    uint64_t getMaxWlogSendPb(uint64_t maxWlogSendMb, const char *msg) const {
        const uint64_t maxWlogSendPb = convertMibToPb(maxWlogSendMb);
        if (maxWlogSendPb == 0) {
            throw cybozu::Exception(msg) << "maxWlogSendPb must be positive";
        }
        return maxWlogSendPb;
    }
    uint64_t takeSnapshotDetail(uint64_t maxWlogSendPb, bool isMergeable, QFile& qf, uint64_t lsid);
    static void verifyMetaLsidGidEquality(const MetaLsidGid &rec0, const MetaLsidGid &rec1, const char *msg) {
        if (rec0.lsid != rec1.lsid || rec0.gid != rec1.gid) {
            cybozu::Exception(msg) << "not equal lsid or gid" << rec0 << rec1;
        }
    }
    device::SuperBlock getSuperBlock() const {
        cybozu::util::File file = device::getWldevFile(getWdevName());
        device::SuperBlock super;
        super.read(file.fd());
        return super;
    }
    bool isWlogTransferRequiredDetail(bool isLater, uint64_t& remainingMb);
    std::pair<MetaLsidGid, uint64_t> getEndSnapshot(
        QFile &qf, const MetaLsidGid &recB, uint64_t maxWlogSendPb, uint64_t permanentLsid);
    void removeOldRecordsFromQueueFile(QFile &qf, const MetaLsidGid &recB);
};

} //namespace walb
