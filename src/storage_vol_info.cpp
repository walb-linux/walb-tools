#include "storage_vol_info.hpp"

namespace walb {


void StorageVolInfo::init()
{
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


StrVec StorageVolInfo::getStatusAsStrVec(bool isVerbose) const
{
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


void StorageVolInfo::setState(const std::string& newState)
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


void StorageVolInfo::resetWlog(uint64_t gid)
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


bool StorageVolInfo::deleteGarbageWlogs()
{
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


void StorageVolInfo::deleteAllWlogs()
{
    const std::string wdevName = getWdevName();
    device::LsidSet lsids;
    device::getLsidSet(wdevName, lsids);
    if (lsids.oldest < lsids.prevWritten) {
        device::eraseWal(wdevName, lsids.prevWritten);
    }
}


std::tuple<MetaLsidGid, MetaLsidGid, uint64_t, bool> StorageVolInfo::prepareWlogTransfer(
    uint64_t maxWlogSendMb, size_t intervalSec) {
    const char *const FUNC = __func__;
    QFile qf(queuePath().str(), O_RDWR);
    const MetaLsidGid recB = getDoneRecord(); // begin
    removeOldRecordsFromQueueFile(qf, recB);
    /* Take a implicit snapshot if necessary. */
    device::LsidSet lsids;
    device::getLsidSet(getWdevName(), lsids);
    const uint64_t maxWlogSendPb = getMaxWlogSendPb(maxWlogSendMb, FUNC);
    MetaLsidGid recE;
    if (!qf.empty()) qf.front(recE);
    if (qf.empty() || (recE.lsid < lsids.latest && recE.timestamp + intervalSec <= uint64_t(::time(0)))) {
        takeSnapshotDetail(maxWlogSendPb, true, qf, lsids.latest);
    }
    /* Get the end of the target range. */
    uint64_t lsidLimit;
    std::tie(recE, lsidLimit) = getEndSnapshot(qf, recB, maxWlogSendPb, lsids.permanent);
    const bool doLater = lsids.permanent < lsidLimit;
    return std::make_tuple(recB, recE, lsidLimit, doLater);
}


MetaDiff StorageVolInfo::getTransferDiff(const MetaLsidGid &recB, const MetaLsidGid &recE, uint64_t lsidE) const
{
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


bool StorageVolInfo::finishWlogTransfer(const MetaLsidGid &recB, const MetaLsidGid &recE, uint64_t lsidE)
{
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
    removeOldRecordsFromQueueFile(qf, recS);
    return !qf.empty();
}


std::pair<uint64_t, uint64_t> StorageVolInfo::getGidRange() const
{
    const MetaLsidGid rec0 = getDoneRecord();
    QFile qf(queuePath().str(), O_RDWR);
    if (qf.empty()) return {rec0.gid, rec0.gid};
    MetaLsidGid rec1;
    qf.front(rec1);
    return {rec0.gid, rec1.gid};
}


MetaLsidGid StorageVolInfo::getLatestSnap() const
{
    QFile qf(queuePath().str(), O_RDWR);
    if (qf.empty()) return getDoneRecord();
    MetaLsidGid rec;
    qf.front(rec);
    return rec;
}


void StorageVolInfo::growWdev(uint64_t sizeLb)
{
    const std::string path = getWdevPath();
    const uint64_t curSizeLb = device::getSizeLb(path);
    if (sizeLb > 0 && curSizeLb > sizeLb) {
        throw cybozu::Exception(__func__) << "shrink is not supported" << curSizeLb << sizeLb;
    }
    device::resize(path, sizeLb);
}


void StorageVolInfo::verifyBaseDirExistance(const std::string &baseDirStr)
{
    cybozu::FilePath baseDir(baseDirStr);
    cybozu::FileStat stat = baseDir.stat();
    if (!stat.exists()) {
        throw cybozu::Exception("StorageVolInfo:not exists") << baseDir.str();
    }
    if (!stat.isDirectory()) {
        throw cybozu::Exception("StorageVolInfo:not directory") << baseDir.str();
    }
}


uint64_t StorageVolInfo::takeSnapshotDetail(uint64_t maxWlogSendPb, bool isMergeable, QFile& qf, uint64_t lsid)
{
    const char *const FUNC = __func__;
    MetaLsidGid pre;
    if (qf.empty()) {
        pre = getDoneRecord();
    } else {
        qf.front(pre);
    }
    const std::string wdevPath = wdevPath_.str();
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


bool StorageVolInfo::isWlogTransferRequiredDetail(bool isLater)
{
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


std::pair<MetaLsidGid, uint64_t> StorageVolInfo::getEndSnapshot(
    QFile &qf, const MetaLsidGid &recB, uint64_t maxWlogSendPb, uint64_t permanentLsid)
{
    assert(!qf.empty());
    QFile::ConstIterator it = qf.cend();
    --it;
    MetaLsidGid recE;
    it.get(recE);
    size_t nr = 1;
    while (it != qf.cbegin() && recE.isMergeable) {
        --it;
        MetaLsidGid rec;
        it.get(rec);
        if (rec.lsid > recB.lsid + maxWlogSendPb) break;
        if (rec.lsid > permanentLsid) break;
        recE = rec;
        nr++;
    }

    if (!(recB.lsid <= recE.lsid)) {
        throw cybozu::Exception(__func__) << "invalid MetaLsidGid record" << recB << recE;
    }
    assert(recB.gid < recE.gid);

    uint64_t lsidE;
    if (nr == 1 && recE.gid == recB.gid + 1) {
        lsidE = recE.lsid;
        if (lsidE - recB.lsid > maxWlogSendPb) {
            LOGs.warn() << "MaxWlogSendPb is too small" << maxWlogSendPb << lsidE - recB.lsid;
        }
    } else {
        lsidE = std::min(recB.lsid + maxWlogSendPb, recE.lsid);
    }
    return std::make_pair(recE, lsidE);
}


void StorageVolInfo::removeOldRecordsFromQueueFile(QFile &qf, const MetaLsidGid &recB)
{
    MetaLsidGid rec;
    while (!qf.empty()) {
        qf.back(rec);
        const bool isOld0 = rec.lsid < recB.lsid;
        const bool isOld1 = rec.lsid == recB.lsid && rec.gid <= recB.gid;
        if (isOld0 || isOld1) {
            qf.popBack();
        } else {
            break;
        }
    }
}

} // namespace walb
