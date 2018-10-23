#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"
#include <algorithm>
#include <atomic>
#include <type_traits>
#include <snappy.h>
#include "linux/walb/block_size.h"
#include "walb_diff_virt.hpp"
#include "murmurhash3.hpp"
#include "state_machine.hpp"
#include "action_counter.hpp"
#include "atomic_map.hpp"
#include "constant.hpp"
#include "host_info.hpp"
#include "dirty_full_sync.hpp"
#include "dirty_hash_sync.hpp"
#include "wdiff_transfer.hpp"
#include "command_param_parser.hpp"
#include "discard_type.hpp"
#include "walb_diff_io.hpp"
#include "snap_info.hpp"
#include "ts_delta.hpp"

namespace walb {

/**
 * Manage one instance for each volume.
 */
struct ArchiveVolState
{
private:
    std::recursive_mutex mu_;
public:
    static std::atomic<double>& lockTimeThreshold() {
        static std::atomic<double> value(DEFAULT_LOCK_TIME_THRESHOLD);
        return value;
    }
    /**
     * This is a wrapper of UniqueLock
     * to track lock wait time and log if it exceeds a threshold.
     */
    class Lock {
        UniqueLock ul_;
        std::string context_;
        cybozu::Stopwatch stopwatch_;
        size_t count_;
    public:
        Lock() = default;
        /**
         * args will be concatinated into context_.
         */
        template <typename... Args>
        Lock(std::recursive_mutex& mu, Args&&... args)
            : ul_(mu, std::defer_lock), context_(), stopwatch_(false), count_(0) {
            lock(args...);
        }
        ~Lock() {
            unlock();
        }
        Lock(const Lock&) = delete;
        Lock(Lock&& rhs) noexcept : Lock() {
            swap(rhs);
        }
        Lock& operator=(const Lock&) = delete;
        Lock& operator=(Lock&& rhs) noexcept {
            swap(rhs);
            return *this;
        }
        void swap(Lock& rhs) noexcept {
            std::swap(ul_, rhs.ul_);
            std::swap(context_, rhs.context_);
            std::swap(stopwatch_, rhs.stopwatch_);
            std::swap(count_, rhs.count_);
        }

        template <typename T>
        void addContext(T&& t) {
            std::stringstream ss;
            if (!context_.empty()) ss << ",";
            ss << std::forward<T>(t);
            context_ += ss.str();
        }
        template <typename Head, typename... Tail>
        void addContext(Head&& head, Tail&&... tail) {
            addContext<Head>(std::forward<Head>(head));
            addContext<Tail...>(std::forward<Tail>(tail)...);
        }
        void addContext() {}
        void resetContext() { context_.clear(); }

        template <typename... Args>
        void lock(Args&&... args) {
            resetContext();
            addContext(std::forward<Args>(args)...);
            lockDetail();
        }
        void lock() {
            lockDetail();
        }
        void unlock() {
            unlockDetail();
        }
    private:
        void lockDetail() {
            if (ul_.owns_lock()) return;
            count_++;
            stopwatch_.reset();
            ul_.lock();
            putLogIfNecessary("Lock waiting time exceeds threshold", stopwatch_.get());
        }
        void unlockDetail() {
            if (!ul_.owns_lock()) return;
            ul_.unlock();
            putLogIfNecessary("Lock holding time exceeds threshold", stopwatch_.get());
        }
        void putLogIfNecessary(const char* description, double elapsedSec) {
            if (elapsedSec > lockTimeThreshold().load()) {
                LOGs.warn() << description
                            << util::getElapsedTimeStr(elapsedSec)
                            << context_ << count_;
            }
        }
    };

    template <typename... Args>
    Lock lock(Args&&... args) {
        return Lock(mu_, args...);
    }
    Lock lock() {
        return Lock(mu_, volId);
    }

    std::string volId;

    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac;

    MetaDiffManager diffMgr;
    VolLvCache lvCache;

    /*
     * This is meaningful during full/hash backup/replication.
     */
    std::atomic<uint64_t> progressLb;

    /**
     * Timestamp of the latest sync (full, hash).
     * Lock of mu is required to access these variables.
     * 0 means none did not do sync after the daemon started.
     */
    uint64_t lastSyncTime;
    /**
     * Timestamp of the latest wdiff received from a proxy server.
     * Lock of mu is required to access these variables.
     * 0 means no diff was received after the daemon started.
     */
    uint64_t lastWdiffReceivedTime;

private:
    /**
     * Latest meta state. This is cache data. mu lock is required to access this variable.
     */
    MetaState latestMetaSt;
public:

    explicit ArchiveVolState(const std::string& volId0)
        : volId(volId0)
        , stopState(NotStopping)
        , sm(mu_)
        , ac(mu_)
        , diffMgr()
        , lvCache()
        , progressLb(0)
        , lastSyncTime(0)
        , lastWdiffReceivedTime(0) {
        sm.init(statePairTbl);
        initInner(volId0);
    }
    void updateLastSyncTime() {
        Lock lk = lock(__func__, volId);
        lastSyncTime = ::time(0);
    }
    void updateLastWdiffReceivedTime() {
        Lock lk = lock(__func__, volId);
        lastWdiffReceivedTime = ::time(0);
    }
    void setLatestMetaState(const MetaState& metaSt) {
        if (metaSt.isApplying) {
            throw cybozu::Exception(__func__) << "bad metaSt" << metaSt;
        }
        Lock lk = lock(__func__, volId, metaSt);
        latestMetaSt = metaSt;
    }
    MetaState getLatestMetaState() {
        Lock lk = lock(__func__, volId);
        return latestMetaSt;
    }
private:
    void initInner(const std::string& volId);
};


namespace archive_local {

class RemoteSnapshotManager
{
public:
    struct Info {
        std::string volId;
        std::string dstId;
        MetaState metaSt;
    };
    using InternalMap = std::map<std::string, Info>; // key: dstId;
    using Map = std::map<std::string, InternalMap>; // key: volId
private:
    using AutoLock = std::lock_guard<std::mutex>;
    mutable std::mutex mu_;
    Map map_;
public:
    void update(const std::string& volId, const std::string& dstId, const MetaState& metaSt) {
        AutoLock lk(mu_);
        map_[volId][dstId] = Info{volId, dstId, metaSt};
    }
    void remove(const std::string& volId) {
        AutoLock lk(mu_);
        auto it = map_.find(volId);
        if (it != map_.end()) map_.erase(it);
    }
    Map copyMap() const {
        AutoLock lk(mu_);
        return map_;
    }
};

} // namespace archive_local


struct ArchiveSingleton
{
    static ArchiveSingleton& getInstance() {
        static ArchiveSingleton instance;
        return instance;
    }

    /**
     * Read-only except for daemon initialization.
     */
    std::string nodeId;
    std::string baseDirStr;
    std::string volumeGroup;
    std::string thinpool;
    size_t maxConnections;
    size_t maxForegroundTasks;
    size_t socketTimeout;
    size_t maxWdiffSendNr;
    DiscardType discardType;
    uint64_t fsyncIntervalSize;
    KeepAliveParams keepAliveParams;
    bool doAutoResize;
    bool keepOneColdSnapshot;
    size_t maxOpenDiffs; // 0 means unlimited.
    size_t pctApplySleep; // 0 to 100. 0 means no sleep.
    bool allowExec;
    CompressOpt cmprOptForSync;

    /**
     * Writable and must be thread-safe.
     */
    ProcessStatus ps;
    AtomicMap<ArchiveVolState> stMap;
    archive_local::RemoteSnapshotManager remoteSnapshotManager;
    protocol::HandlerStatMgr handlerStatMgr;

    void setSocketParams(cybozu::Socket& sock) const {
        util::setSocketParams(sock, keepAliveParams, socketTimeout);
    }
};

inline ArchiveSingleton& getArchiveGlobal()
{
    return ArchiveSingleton::getInstance();
}

static const ArchiveSingleton& ga = getArchiveGlobal();

inline ArchiveVolState &getArchiveVolState(const std::string &volId)
{
    return getArchiveGlobal().stMap.get(volId);
}

inline ArchiveVolInfo getArchiveVolInfo(const std::string &volId)
{
    ArchiveVolState &volSt = getArchiveVolState(volId);
    return ArchiveVolInfo(ga.baseDirStr, volId, ga.volumeGroup, ga.thinpool, volSt.diffMgr, volSt.lvCache);
}

inline bool isThinpool()
{
    return !ga.thinpool.empty();
}

namespace archive_local {

inline StrVec getVolIdList()
{
    return util::getDirNameList(ga.baseDirStr);
}

template <typename F>
inline MetaDiffVec tryOpenDiffs(
    std::vector<cybozu::util::File>& fileV, ArchiveVolInfo& volInfo,
    bool allowEmpty, const MetaState& baseSt, F getDiffList)
{
    const char *const FUNC = __func__;

    const int maxRetryNum = 10;
    int retryNum = 0;
  retry:
    const MetaDiffVec diffV = getDiffList(baseSt);
    if (!allowEmpty && diffV.empty()) {
        throw cybozu::Exception(FUNC) << "diffV empty" << volInfo.volId;
    }
    // Try to open all wdiff files.
    for (const MetaDiff& diff : diffV) {
        cybozu::util::File op;
        const std::string filePathStr = volInfo.getDiffPath(diff).str();
        if (!op.open(filePathStr, O_RDONLY)) {
            LOGs.warn() << FUNC << "open failed" << filePathStr;
            retryNum++;
            if (retryNum == maxRetryNum) {
                throw cybozu::Exception(FUNC) << "exceed max retry";
            }
            fileV.clear();
            goto retry;
        }
        fileV.push_back(std::move(op));
    }
    return diffV;
}

const bool allowEmpty = true;

void prepareRawFullScanner(
    FileReader &file, ArchiveVolState &volSt, uint64_t sizeLb, uint64_t gid = UINT64_MAX);
void prepareVirtualFullScanner(
    VirtualFullScanner &virt, ArchiveVolState &volSt,
    ArchiveVolInfo &volInfo, uint64_t sizeLb, const MetaSnap &snap);
void verifyApplicable(const std::string& volId, uint64_t gid);
bool applyOpenedDiffs(std::vector<cybozu::util::File>&& fileV, cybozu::lvm::Lv& lv,
                      const std::atomic<int>& stopState,
                      DiffStatistics& statIn, DiffStatistics& statOut, std::string& memUsageStr);
bool applyDiffsToVolume(const std::string& volId, uint64_t gid);
void verifyNotApplying(const std::string &volId);
void verifyMergeable(const std::string &volId, uint64_t gid);
bool mergeDiffs(const std::string &volId, uint64_t gidB, bool isSize, uint64_t param3);


inline void removeLv(const std::string& vgName, const std::string& name)
{
    if (!cybozu::lvm::existsFile(vgName, name)) {
        // Do nothing.
        return;
    }
    cybozu::lvm::remove(cybozu::lvm::getLvStr(vgName, name));
}

bool restore(const std::string &volId, uint64_t gid);
void delSnapshot(const std::string &volId, uint64_t gid, bool isCold);


/**
 * Get list of all restored/cold volumes.
 */
inline StrVec listSnapshot(const std::string &volId, bool isCold)
{
    VolLvCache &lvC = getArchiveVolState(volId).lvCache;
    const std::vector<uint64_t> gidV = isCold ? lvC.getColdGidList() : lvC.getRestoredGidList();
    StrVec ret;
    for (uint64_t gid : gidV) ret.push_back(cybozu::itoa(gid));
    return ret;
}

StrVec listRestorable(const std::string &volId, bool isAll = false);
void doAutoResizeIfNecessary(ArchiveVolState& volSt, ArchiveVolInfo& volInfo, uint64_t sizeLb);
void dbgVerifyLatestMetaState(const std::string &volId);


template <typename T>
struct ZeroResetterT
{
    T &t;
    ZeroResetterT(T &t) : t(t) {}
    ~ZeroResetterT() { t = 0; }
};

using ZeroResetter = ZeroResetterT<std::atomic<uint64_t>>;


void backupServer(protocol::ServerParams &p, bool isFull);
void delSnapshotServer(protocol::ServerParams &p, bool isCold);


inline void runReplSync1stNegotiation(const std::string &volId, const AddrPort &addrPort, cybozu::Socket &sock, std::string &dstId)
{
    const cybozu::SocketAddr server = addrPort.getSocketAddr();
    util::connectWithTimeout(sock, server, ga.socketTimeout);
    ga.setSocketParams(sock);
    dstId = protocol::run1stNegotiateAsClient(sock, ga.nodeId, replSyncPN);
    protocol::sendStrVec(sock, {volId}, 1, __func__, msgAccept);
}


void verifyVolumeSize(ArchiveVolState &volSt, ArchiveVolInfo &volInfo, uint64_t sizeLb, Logger &logger);


inline TsDelta generateTsDelta(const std::string &volId, const std::string &dstId, const MetaState &srcSt, const MetaState &dstSt)
{
    return TsDelta{volId, dstId, {"archive", "archive"}
        , {srcSt.snapB.gidB, dstSt.snapB.gidB}, {srcSt.timestamp, dstSt.timestamp}};
}


bool runFullReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, uint64_t bulkLb, Logger &logger);
bool runFullReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, const cybozu::Uuid &archiveUuid, ArchiveVolState::Lock &lk, Logger &logger);
bool runHashReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, uint64_t bulkLb, const MetaDiff &diff, Logger &logger);
bool runHashReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, ArchiveVolState &lk, const MetaState &metaSt, Logger &logger);
bool runNoMergeDiffReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, const MetaSnap &srvLatestSnap, Logger &logger);
bool runDiffReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, const MetaSnap &srvLatestSnap, const CompressOpt &cmpr, uint64_t wdiffMergeSize, Logger &logger);
bool runDiffReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, ArchiveVolState &lk, const MetaState &metaSt, Logger &logger);
bool runResyncReplClient(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo, const std::string &dstId,
    packet::Packet &pkt, uint64_t bulkLb, Logger &logger);
bool runResyncReplServer(
    const std::string &volId, ArchiveVolState &volSt, ArchiveVolInfo &volInfo,
    packet::Packet &pkt, ArchiveVolState &lk, Logger &logger);

enum {
    DO_FULL_SYNC = 0,
    DO_RESYNC = 1,
    DO_HASH_OR_DIFF_SYNC = 2,
};

bool runReplSyncClient(const std::string &volId, cybozu::Socket &sock, const HostInfoForRepl &hostInfo,
                       bool isSize, uint64_t param, const std::string &dstId, Logger &logger);
bool runReplSyncServer(const std::string &volId, cybozu::Socket &sock, ArchiveVolState &lk, Logger &logger);

StrVec getAllStatusAsStrVec();
StrVec getVolStatusAsStrVec(const std::string &volId);


inline void getState(protocol::GetCommandParams &p)
{
    protocol::runGetStateServer(p, getArchiveVolState);
}


inline void getHostType(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, archiveHT);
}


inline void getVolList(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, getVolIdList());
}


inline void getPid(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, static_cast<size_t>(::getpid()));
}


inline MetaDiffVec getAllWdiffDetail(protocol::GetCommandParams &p)
{
    const VolIdAndGidRangeParam param = parseVolIdAndGidRangeParamForGet(p.params);

    ArchiveVolState &volSt = getArchiveVolState(param.volId);
    ArchiveVolState::Lock lk = volSt.lock(__func__, volSt.volId);
    return volSt.diffMgr.getAll(param.gid[0], param.gid[1]);
}


inline void getDiffList(protocol::GetCommandParams &p)
{
    const MetaDiffVec diffV = getAllWdiffDetail(p);
    StrVec v;
    for (const MetaDiff &diff : diffV) {
        v.push_back(formatMetaDiff("", diff));
    }
    protocol::sendValueAndFin(p, v);
}


inline void getTotalDiffSize(protocol::GetCommandParams &p)
{
    const MetaDiffVec diffV = getAllWdiffDetail(p);
    size_t totalSize = 0;
    for (const MetaDiff &d : diffV) totalSize += d.dataSize;
    protocol::sendValueAndFin(p, totalSize);
}


inline void getNumDiff(protocol::GetCommandParams &p)
{
    const MetaDiffVec diffV = getAllWdiffDetail(p);
    protocol::sendValueAndFin(p, diffV.size());
}


void getApplicableDiffList(protocol::GetCommandParams &p);
void existsDiff(protocol::GetCommandParams &p);
void existsBaseImage(protocol::GetCommandParams &p);
void getNumAction(protocol::GetCommandParams &p);
void getAllActions(protocol::GetCommandParams &p);
void getSnapshot(protocol::GetCommandParams &p, bool isCold);


inline void getRestored(protocol::GetCommandParams &p)
{
    bool isCold = false;
    getSnapshot(p, isCold);
}

inline void getCold(protocol::GetCommandParams &p)
{
    bool isCold = true;
    getSnapshot(p, isCold);
}

void getRestorable(protocol::GetCommandParams &p);
void getUuidDetail(protocol::GetCommandParams &p, bool isArchive);


inline void getUuid(protocol::GetCommandParams &p)
{
    getUuidDetail(p, false);
}


inline void getArchiveUuid(protocol::GetCommandParams &p)
{
    getUuidDetail(p, true);
}


void getBase(protocol::GetCommandParams &p);
void getBaseAll(protocol::GetCommandParams &p);
bool getBlockHash(
    const std::string &volId, uint64_t gid, uint64_t bulkLb, uint64_t sizeLb,
    packet::Packet &pkt, Logger &, cybozu::murmurhash3::Hash &hash);
bool virtualFullScanServer(
    const std::string &volId, uint64_t gid, uint64_t bulkLb, uint64_t sizeLb,
    packet::Packet &pkt, Logger &logger);
void getVolSize(protocol::GetCommandParams &p);


inline void getProgress(protocol::GetCommandParams &p)
{
    const std::string volId = parseVolIdParam(p.params, 1);
    ArchiveVolState &volSt = getArchiveVolState(volId);
    const uint64_t progressLb = volSt.progressLb.load();
    protocol::sendValueAndFin(p, progressLb);
    p.logger.debug() << "get progress succeeded" << volId << progressLb;
}


inline void getVolumeGroup(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, ga.volumeGroup);
    p.logger.debug() << "get volume-group succeeded";
}


inline void getThinpool(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, ga.thinpool);
    p.logger.debug() << "get thinpool succeeded";
}


MetaState getMetaStateDetail(const std::string &volId, bool isApplying, uint64_t gid);


inline void getMetaSnap(protocol::GetCommandParams &p)
{
    const VolIdAndGidParam param = parseVolIdAndGidParam(p.params, 1, false, UINT64_MAX);
    MetaState metaSt = getMetaStateDetail(param.volId, false, param.gid);
    protocol::sendValueAndFin(p, metaSt.snapB.str());
}


inline void getMetaState(protocol::GetCommandParams &p)
{
    const GetMetaStateParam param = parseGetMetaStateParam(p.params);
    MetaState metaSt = getMetaStateDetail(param.volId, param.isApplying, param.gid);
    protocol::sendValueAndFin(p, metaSt.strTs());
}


bool getLatestMetaState(const std::string &volId, MetaState &metaSt);
SnapshotInfo getLatestSnapshotInfo(const std::string &volId);
std::string getLatestSnapForVolume(const std::string& volId);
void getLatestSnap(protocol::GetCommandParams &p);
void getTsDelta(protocol::GetCommandParams &p);
void getHandlerStat(protocol::GetCommandParams &p);

} // namespace archive_local


void verifyAndRecoverArchiveVol(const std::string& volId);
void gcArchiveVol(const std::string& volId);
void c2aStatusServer(protocol::ServerParams &p);
void c2aInitVolServer(protocol::ServerParams &p);
void c2aClearVolServer(protocol::ServerParams &p);
void c2aStartServer(protocol::ServerParams &p);
void c2aStopServer(protocol::ServerParams &p);

/**
 * Execute dirty full sync protocol as server.
 */
inline void s2aDirtyFullSyncServer(protocol::ServerParams &p)
{
    const bool isFull = true;
    archive_local::backupServer(p, isFull);
}

/**
 * Execute dirty hash sync protocol as server.
 */
inline void s2aDirtyHashSyncServer(protocol::ServerParams &p)
{
    const bool isFull = false;
    archive_local::backupServer(p, isFull);
}

void c2aRestoreServer(protocol::ServerParams &p);

inline void c2aDelRestoredServer(protocol::ServerParams &p)
{
    bool isCold = false;
    archive_local::delSnapshotServer(p, isCold);
}

inline void c2aDelColdServer(protocol::ServerParams &p)
{
    bool isCold = true;
    archive_local::delSnapshotServer(p, isCold);
}

void c2aReloadMetadataServer(protocol::ServerParams &p);
void p2aWdiffTransferServer(protocol::ServerParams &p);
void c2aReplicateServer(protocol::ServerParams &p);
void a2aReplSyncServer(protocol::ServerParams &p);
void c2aApplyServer(protocol::ServerParams &p);
void c2aMergeServer(protocol::ServerParams &p);
void c2aResizeServer(protocol::ServerParams &p);
void c2aResetVolServer(protocol::ServerParams &p);
void changeSnapshot(protocol::ServerParams &p, bool enable);

inline void c2aDisableSnapshot(protocol::ServerParams &p)
{
    changeSnapshot(p, false);
}

inline void c2aEnableSnapshot(protocol::ServerParams &p)
{
    changeSnapshot(p, true);
}

void c2aVirtualFullScan(protocol::ServerParams &p);
void c2aBlockHashServer(protocol::ServerParams &p);
void c2aSetUuidServer(protocol::ServerParams &p);
void c2aSetStateServer(protocol::ServerParams &p);
void c2aSetBaseServer(protocol::ServerParams &p);
void c2aGarbageCollectDiffServer(protocol::ServerParams &p);
void s2aGatherLatestSnapServer(protocol::ServerParams &p);
#ifndef NDEBUG
void c2aDebugServer(protocol::ServerParams &p);
#endif

const protocol::GetCommandHandlerMap archiveGetHandlerMap = {
    { stateTN, archive_local::getState },
    { hostTypeTN, archive_local::getHostType },
    { volTN, archive_local::getVolList },
    { pidTN, archive_local::getPid },
    { diffTN, archive_local::getDiffList },
    { applicableDiffTN, archive_local::getApplicableDiffList },
    { totalDiffSizeTN, archive_local::getTotalDiffSize },
    { numDiffTN, archive_local::getNumDiff },
    { existsDiffTN, archive_local::existsDiff },
    { existsBaseImageTN, archive_local::existsBaseImage },
    { numActionTN, archive_local::getNumAction },
    { restoredTN, archive_local::getRestored },
    { coldTN, archive_local::getCold },
    { restorableTN, archive_local::getRestorable },
    { uuidTN, archive_local::getUuid },
    { archiveUuidTN, archive_local::getArchiveUuid },
    { baseTN, archive_local::getBase },
    { baseAllTN, archive_local::getBaseAll },
    { volSizeTN, archive_local::getVolSize },
    { progressTN, archive_local::getProgress },
    { volumeGroupTN, archive_local::getVolumeGroup },
    { thinpoolTN, archive_local::getThinpool },
    { allActionsTN, archive_local::getAllActions },
    { getMetaSnapTN, archive_local::getMetaSnap },
    { getMetaStateTN, archive_local::getMetaState },
    { getLatestSnapTN, archive_local::getLatestSnap },
    { getTsDeltaTN, archive_local::getTsDelta },
    { getHandlerStatTN, archive_local::getHandlerStat },
};

inline void c2aGetServer(protocol::ServerParams &p)
{
    protocol::runGetCommandServer(p, ga.nodeId, archiveGetHandlerMap,
                                  getArchiveGlobal().handlerStatMgr);
}

inline void c2aExecServer(protocol::ServerParams &p)
{
    protocol::runExecServer(p, ga.nodeId, ga.allowExec);
}

const protocol::Str2ServerHandler archiveHandlerMap = {
    // commands
    { statusCN, c2aStatusServer },
    { initVolCN, c2aInitVolServer },
    { clearVolCN, c2aClearVolServer },
    { resetVolCN, c2aResetVolServer },
    { startCN, c2aStartServer },
    { stopCN, c2aStopServer },
    { restoreCN, c2aRestoreServer },
    { delRestoredCN, c2aDelRestoredServer },
    { delColdCN, c2aDelColdServer },
    { dbgReloadMetadataCN, c2aReloadMetadataServer },
    { replicateCN, c2aReplicateServer },
    { applyCN, c2aApplyServer },
    { mergeCN, c2aMergeServer },
    { resizeCN, c2aResizeServer },
    { blockHashCN, c2aBlockHashServer },
    { dbgSetUuidCN, c2aSetUuidServer },
    { dbgSetStateCN, c2aSetStateServer },
    { dbgSetBaseCN, c2aSetBaseServer },
    { getCN, c2aGetServer },
    { execCN, c2aExecServer },
    { disableSnapshotCN, c2aDisableSnapshot },
    { enableSnapshotCN, c2aEnableSnapshot },
    { virtualFullScanCN, c2aVirtualFullScan },
    { gcDiffCN, c2aGarbageCollectDiffServer },
#ifndef NDEBUG
    { debugCN, c2aDebugServer },
#endif
    // protocols.
    { dirtyFullSyncPN, s2aDirtyFullSyncServer },
    { dirtyHashSyncPN, s2aDirtyHashSyncServer },
    { wdiffTransferPN, p2aWdiffTransferServer },
    { replSyncPN, a2aReplSyncServer },
    { gatherLatestSnapPN, s2aGatherLatestSnapServer },
};

} // namespace walb
