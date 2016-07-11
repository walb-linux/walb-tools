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
    void init() {
        LOGd("volDir %s volId %s", volDir_.cStr(), volId_.c_str());
        util::makeDir(volDir_.str(), "StorageVolInfo", true);
        {
            QFile qf(queuePath().str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
            qf.sync();
        }
        util::saveFile(volDir_, "path", wdevPath_.str());
        setState(sSyncReady);
        const uint64_t lsid = -1;
        const uint64_t gid = -1;
        MetaLsidGid doneRec(lsid, gid, false, ::time(0));
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
    const cybozu::FilePath& getVolDir() const {
        return volDir_;
    }
    /**
     * get status as a string vector.
     */
    StrVec getStatusAsStrVec(bool isVerbose) const {
        StrVec v;
        if (!existsVolDir()) return v;

        const std::string wdevPathStr = wdevPath_.str();
        auto &fmt = cybozu::util::formatString;
        v.push_back(fmt("wdevPath %s", wdevPathStr.c_str()));
        const uint64_t sizeLb = device::getSizeLb(wdevPathStr);
        v.push_back(fmt("sizeLb %" PRIu64, sizeLb));
        const std::string sizeS = cybozu::util::toUnitIntString(sizeLb * LOGICAL_BLOCK_SIZE);
        v.push_back(fmt("size %s", sizeS.c_str()));
        v.push_back(fmt("isOverflow %d", device::isOverflow(wdevPathStr)));
        uint64_t logUsagePb = device::getLogUsagePb(wdevPathStr);
        v.push_back(fmt("logUsagePb %" PRIu64, logUsagePb));
        uint64_t logCapacityPb = device::getLogCapacityPb(wdevPathStr);
        v.push_back(fmt("logCapacityPb %" PRIu64, logCapacityPb));
        const cybozu::Uuid uuid = getUuid();
        v.push_back(fmt("uuid %s", uuid.str().c_str()));
        const uint32_t pbs = getPbs();
        v.push_back(fmt("pbs %" PRIu32, pbs));
        device::SuperBlock super = getSuperBlock();
        const uint32_t salt = super.getLogChecksumSalt();
        v.push_back(fmt("salt %" PRIx32, salt));

        if (!isVerbose) return v;

        v.push_back("-----DoneFile-----");
        const MetaLsidGid doneRec = getDoneRecord();
        v.push_back(doneRec.str());

        v.push_back("-----QueueFile-----");
        QFile qf(queuePath().str(), O_RDWR);
        QFile::ConstIterator itr = qf.cbegin();
        while (itr != qf.cend()) {
            const MetaLsidGid rec = *itr;
            v.push_back(rec.str());
            ++itr;
        }
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
            sTarget,
            sStandby,
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
            QFile qf(queuePath().str(), O_RDWR);
            qf.clear();
            qf.sync();
        }
        {
            device::SuperBlock super = getSuperBlock();
            setUuid(super.getUuid());
        }
        setState(sSyncReady);
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
        return takeSnapshotDetail(maxWlogSendPb, false, qf);
    }
    /**
     * Delete garbage wlogs if necessary.
     *
     * RETURN:
     *   true if there are wlogs to be deleted later.
     */
    bool deleteGarbageWlogs() {
        const uint64_t doneLsid = getDoneLsid();
        const std::string wdevName = getWdevName();
        device::LsidSet lsids;
        device::getLsidSet(wdevName, lsids);
        const uint64_t targetLsid = std::min(doneLsid, lsids.prevWritten);
        if (lsids.oldest < targetLsid) {
            device::eraseWal(wdevName, targetLsid);
            lsids.oldest = targetLsid;
        }
        return lsids.oldest < doneLsid;
    }
    /**
     * Delete all the wlogs.
     */
    void deleteAllWlogs() {
        const std::string wdevName = getWdevName();
        device::LsidSet lsids;
        device::getLsidSet(wdevName, lsids);
        if (lsids.oldest < lsids.prevWritten) {
            device::eraseWal(wdevName, lsids.prevWritten);
        }
    }

    /**
     * Calling order:
     *   (1) deleteGarbageWlogs()
     *   (2) mayWlogTransferBeRequiredNow()
     *   (3) prepareWlogTransfer()
     *   (4) getTransferDiff()
     *   (5) finishWlogTransfer()
     *   (6) deleteGarbageWlogs()
     *
     */
    bool mayWlogTransferBeRequiredNow() {
        return isWlogTransferRequiredDetail(false);
    }
    bool isWlogTransferRequiredLater() {
        return isWlogTransferRequiredDetail(true);
    }
    /**
     *
     * RETURN:
     *   target lsid/gid range by two MetaLsidGids: recB and recE,
     *   and lsidLimit as uint64_t value,
     *   and boolean value which is true if we must pospone the wlog-transfer.
     *   Do not transfer logpacks which lsid >= lsidLimit.
     */
    std::tuple<MetaLsidGid, MetaLsidGid, uint64_t, bool> prepareWlogTransfer(uint64_t maxWlogSendMb) {
        const char *const FUNC = __func__;
        QFile qf(queuePath().str(), O_RDWR);
        MetaLsidGid recB = getDoneRecord();
        MetaLsidGid recE;
        for (;;) {
            if (qf.empty()) break;
            qf.back(recE);
            if ((recE.lsid < recB.lsid) || (recB.lsid == recE.lsid && recE.gid <= recB.gid)) {
                qf.popBack();
                continue;
            }
            break;
        }
        const uint64_t maxWlogSendPb = getMaxWlogSendPb(maxWlogSendMb, FUNC);
        if (qf.empty()) {
            takeSnapshotDetail(maxWlogSendPb, true, qf);
            qf.back(recE);
        }
        if (!(recB.lsid <= recE.lsid)) {
            throw cybozu::Exception(FUNC)
                << "invalid MetaLsidGidRecord" << recB << recE;
        }
        assert(recB.gid < recE.gid);

        uint64_t lsidLimit;
        if (recB.gid + 1 == recE.gid) {
            lsidLimit = recE.lsid;
        } else {
            lsidLimit = std::min(recB.lsid + maxWlogSendPb, recE.lsid);
        }
        const bool doLater = (device::getPermanentLsid(getWdevPath()) < lsidLimit);
        return std::make_tuple(recB, recE, lsidLimit, doLater);
    }
    /**
     * RETURN:
     *   generated diff will be transferred to a proxy daemon.
     */
    MetaDiff getTransferDiff(const MetaLsidGid &recB, const MetaLsidGid &recE, uint64_t lsidE) const {
        MetaDiff diff;
        diff.snapB.set(recB.gid);
        if (lsidE == recE.lsid) {
            diff.snapE.set(recE.gid);
        } else {
            assert(recB.gid + 1 < recE.gid);
            diff.snapE.set(recB.gid + 1);
        }
        diff.timestamp = recE.timestamp;
        diff.isMergeable = recB.isMergeable;
        return diff;
    }
    /**
     * recB and recE must not be changed between calling
     * prepareWlogTransfer() and finishWlogTransfer().
     *
     * RETURN:
     *   true if there is remaining wlogs (that may be empty).
     */
    bool finishWlogTransfer(const MetaLsidGid &recB, const MetaLsidGid &recE, uint64_t lsidE) {
        const char *const FUNC = __func__;
        const MetaLsidGid recBx = getDoneRecord();
        verifyMetaLsidGidEquality(recB, recBx, FUNC);
        QFile qf(queuePath().str(), O_RDWR);
        if (qf.empty()) {
            throw cybozu::Exception(FUNC)
                << "Maybe BUG: queue must have at lease one record.";
        }
        MetaLsidGid recEx;
        qf.back(recEx);
        verifyMetaLsidGidEquality(recE, recEx, FUNC);
        assert(recB.lsid <= lsidE && lsidE <= recE.lsid);

        MetaLsidGid recS;
        recS.lsid = lsidE;
        if (lsidE == recE.lsid) {
            recS.gid = recE.gid;
            recS.isMergeable = recE.isMergeable;
            recS.timestamp = recE.timestamp;
        } else {
            assert(recB.gid + 1 < recE.gid);
            recS.gid = recB.gid + 1;
            recS.isMergeable = true;
            // gid is progressed while timestamp is not progressed.
            recS.timestamp = recB.timestamp;
        }
        setDoneRecord(recS);
        if (recS.gid == recE.gid) qf.popBack();
        return !qf.empty();
    }
    /**
     * Get the oldest and the latest gid.
     */
    std::pair<uint64_t, uint64_t> getGidRange() const {
        const MetaLsidGid rec0 = getDoneRecord();
        QFile qf(queuePath().str(), O_RDWR);
        if (qf.empty()) return {rec0.gid, rec0.gid};
        MetaLsidGid rec1;
        qf.front(rec1);
        return {rec0.gid, rec1.gid};
    }
    /**
     * Get the latest snapshot info.
     */
    MetaLsidGid getLatestSnap() const {
        QFile qf(queuePath().str(), O_RDWR);
        if (qf.empty()) return getDoneRecord();
        MetaLsidGid rec;
        qf.front(rec);
        return rec;
    }
    /**
     * @sizeLb 0 can be specified (auto-detect).
     */
    void growWdev(uint64_t sizeLb = 0) {
        const std::string path = getWdevPath();
        const uint64_t curSizeLb = device::getSizeLb(path);
        if (sizeLb > 0 && curSizeLb > sizeLb) {
            throw cybozu::Exception(__func__) << "shrink is not supported" << curSizeLb << sizeLb;
        }
        device::resize(path, sizeLb);
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
    uint64_t takeSnapshotDetail(uint64_t maxWlogSendPb, bool isMergeable, QFile& qf) {
        const char *const FUNC = __func__;
        MetaLsidGid pre;
        if (qf.empty()) {
            pre = getDoneRecord();
        } else {
            qf.front(pre);
        }
        const std::string wdevPath = wdevPath_.str();
        const uint64_t lsid = device::getLatestLsid(wdevPath);
        if (device::isOverflow(wdevPath)) {
            throw cybozu::Exception(FUNC) << "wlog overflow" << wdevPath;
        }
        if (pre.lsid > lsid) {
            throw cybozu::Exception(FUNC) << "invalid lsid" << pre.lsid << lsid;
        }
        const uint64_t gid = pre.gid + 1 + (lsid - pre.lsid) / maxWlogSendPb;
        MetaLsidGid cur(lsid, gid, isMergeable, ::time(0));
        qf.pushFront(cur);
        qf.sync();
        LOGs.debug() << FUNC << cur;
        return gid;
    }
    static void verifyMetaLsidGidEquality(const MetaLsidGid &rec0, const MetaLsidGid &rec1, const char *msg) {
        if (rec0.lsid != rec1.lsid || rec0.gid != rec1.gid) {
            cybozu::Exception(msg) << "not equal lsid or gid" << rec0 << rec1;
        }
    }
    uint32_t getPbs() const {
        cybozu::util::File file = device::getWldevFile(getWdevName());
        return cybozu::util::getPhysicalBlockSize(file.fd());
    }
    device::SuperBlock getSuperBlock() const {
        cybozu::util::File file = device::getWldevFile(getWdevName());
        device::SuperBlock super;
        super.read(file.fd());
        return super;
    }
    bool isWlogTransferRequiredDetail(bool isLater) {
        const uint64_t doneLsid = getDoneLsid();
        const std::string wdevName = getWdevName();
        device::LsidSet lsids;
        device::getLsidSet(wdevName, lsids);
        const uint64_t targetLsid = isLater ? lsids.latest : lsids.permanent;
        bool isQueueEmpty;
        {
            QFile qf(queuePath().str(), O_RDWR);
            isQueueEmpty = qf.empty();
        }
        return doneLsid < targetLsid || !isQueueEmpty;
    }
};

} //namespace walb
