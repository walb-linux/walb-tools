#pragma once
#include "protocol.hpp"
#include "storage_vol_info.hpp"
#include "atomic_map.hpp"
#include "state_machine.hpp"
#include "constant.hpp"
#include <snappy.h>
#include "log_dev_monitor.hpp"
#include "wdev_util.hpp"
#include "walb_log_net.hpp"
#include "action_counter.hpp"
#include "walb_diff_pack.hpp"
#include "walb_diff_compressor.hpp"
#include "murmurhash3.hpp"
#include "dirty_full_sync.hpp"
#include "dirty_hash_sync.hpp"
#include "walb_util.hpp"
#include "bdev_reader.hpp"
#include "command_param_parser.hpp"
#include "snap_info.hpp"
#include "ts_delta.hpp"

namespace walb {

struct StorageVolState {
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // key is action identifier.

    explicit StorageVolState(const std::string& volId)
        : stopState(NotStopping), sm(mu), ac(mu) {
        sm.init(statePairTbl);
        initInner(volId);
    }
private:
    void initInner(const std::string& volId);
};

class StorageWorker
{
public:
    const std::string volId;
    explicit StorageWorker(const std::string &volId) : volId(volId) {
    }
    void operator()();
};

namespace storage_local {

class ProxyManager
{
private:
    using Clock = std::chrono::steady_clock;
    using TimePoint = typename Clock::time_point;
    using MilliSeconds = std::chrono::milliseconds;
    using Seconds = std::chrono::seconds;
    using AutoLock = std::lock_guard<std::mutex>;

    struct Info
    {
        cybozu::SocketAddr proxy;
        bool isAvailable;
        TimePoint checkedTime;
        explicit Info(const cybozu::SocketAddr &proxy)
            : proxy(proxy), isAvailable(true)
            , checkedTime(Clock::now() - Seconds(PROXY_HEARTBEAT_INTERVAL_SEC)) {
        }
        Info() : proxy(), isAvailable(false), checkedTime() {
        }
        std::string str() const {
            const int64_t timeToNextCheck
                = PROXY_HEARTBEAT_INTERVAL_SEC
                - std::chrono::duration_cast<Seconds>(Clock::now() - checkedTime).count();
            std::stringstream ss;
            ss << "host " << proxy.toStr() << ":" << proxy.getPort()
               << " isAvailable " << (isAvailable ? "1" : "0")
               << " timeToNextCheck " << timeToNextCheck;
            return ss.str();
        }
        friend inline std::ostream& operator<<(std::ostream& os, const Info &info) {
            os << info.str();
            return os;
        }
    };
    std::vector<Info> v_;
    mutable std::mutex mu_;
public:
    std::vector<cybozu::SocketAddr> getAvailableList() const {
        std::vector<cybozu::SocketAddr> ret;
        AutoLock lk(mu_);
        for (const Info &info : v_) {
            if (info.isAvailable) ret.push_back(info.proxy);
        }
        return ret;
    }
    StrVec getAsStrVec() const {
        AutoLock lk(mu_);
        StrVec ret;
        for (const Info &info : v_) {
            ret.push_back(info.str());
        }
        return ret;
    }
    void add(const std::vector<cybozu::SocketAddr> &proxyV) {
        AutoLock lk(mu_);
        for (const cybozu::SocketAddr &proxy : proxyV) {
            v_.emplace_back(proxy);
        }
    }
    void tryCheckAvailability();
    void kick() {
        TimePoint now = Clock::now();
        bool isAllUnavailable = true;
        {
            AutoLock lk(mu_);
            for (Info &info : v_) {
                if (info.isAvailable) isAllUnavailable = false;
                info.checkedTime = now - Seconds(PROXY_HEARTBEAT_INTERVAL_SEC);
            }
        }
        if (isAllUnavailable) tryCheckAvailability();
    }
private:
    void removeFromList(const cybozu::SocketAddr &proxy) {
        v_.erase(std::remove_if(v_.begin(), v_.end(), [&](const Info &info) {
                    return info.proxy.hasSameAddr(proxy) && info.proxy.getPort() == proxy.getPort();
                }));
    }
    Info checkAvailability(const cybozu::SocketAddr &);
};


class TsDeltaManager
{
public:
    using Map = std::map<std::string, TsDelta>; // key: volId
private:
    using AutoLock = std::lock_guard<std::mutex>;
    mutable std::mutex mu_;
    uint64_t lastSucceededTs_;
    Map map_;
public:
    void connectAndGet();
    Map copyMap(uint64_t *ts = nullptr) const {
        AutoLock lk(mu_);
        if (ts) *ts = lastSucceededTs_;
        return map_;
    }
private:
    void setMap(const Map& map) {
        AutoLock lk(mu_);
        map_ = map;
        lastSucceededTs_ = ::time(0);
    }
};

} // namespace storage_local

struct StorageSingleton
{
    static StorageSingleton& getInstance() {
        static StorageSingleton instance;
        return instance;
    }

    /**
     * Read-only except for daemon initialization.
     */
    cybozu::SocketAddr archive;
    std::vector<cybozu::SocketAddr> proxyV;
    std::string nodeId;
    std::string baseDirStr;
    uint64_t maxWlogSendMb;
    size_t implicitSnapshotIntervalSec;
    size_t delaySecForRetry;
    size_t maxConnections;
    size_t maxForegroundTasks;
    size_t maxBackgroundTasks;
    size_t socketTimeout;
    KeepAliveParams keepAliveParams;
    size_t tsDeltaGetterIntervalSec;
    bool allowExec;

    /**
     * Writable and must be thread-safe.
     */
    ProcessStatus ps;
    AtomicMap<StorageVolState> stMap;
    TaskQueue<std::string> taskQueue;
    std::unique_ptr<DispatchTask<std::string, StorageWorker>> dispatcher;
    std::unique_ptr<std::thread> wdevMonitor;
    std::atomic<bool> quitWdevMonitor;
    LogDevMonitor logDevMonitor;
    std::unique_ptr<std::thread> proxyMonitor;
    std::atomic<bool> quitProxyMonitor;
    storage_local::ProxyManager proxyManager;
    std::unique_ptr<std::thread> tsDeltaGetter;
    std::atomic<bool> quitTsDeltaGetter;
    storage_local::TsDeltaManager tsDeltaManager;
    std::atomic<uint64_t> fullScanLbPerSec; // 0 means unlimited.
    protocol::HandlerStatMgr handlerStatMgr;

    using Str2Str = std::map<std::string, std::string>;
    using AutoLock = std::lock_guard<std::mutex>;
    void addWdevName(const std::string& wdevName, const std::string& volId)
        {
            AutoLock al(wdevName2VolIdMutex);
            Str2Str::iterator i;
            bool maked;
            std::tie(i, maked) = wdevName2volId.insert(std::make_pair(wdevName, volId));
            if (!maked) throw cybozu::Exception("StorageSingleton:addWdevName:already exists") << wdevName << volId;
        }
    void delWdevName(const std::string& wdevName)
        {
            AutoLock al(wdevName2VolIdMutex);
            Str2Str::iterator i = wdevName2volId.find(wdevName);
            if (i == wdevName2volId.end()) throw cybozu::Exception("StorageSingleton:delWdevName:not found") << wdevName;
            wdevName2volId.erase(i);
        }
    std::string getVolIdFromWdevName(const std::string& wdevName) const
        {
            AutoLock al(wdevName2VolIdMutex);
            Str2Str::const_iterator i = wdevName2volId.find(wdevName);
            if (i == wdevName2volId.cend()) throw cybozu::Exception("StorageSingleton:getWvolIdFromWdevName:not found") << wdevName;
            return i->second;
        }
    bool existsWdevName(const std::string& wdevName) const
        {
            AutoLock al(wdevName2VolIdMutex);
            Str2Str::const_iterator i = wdevName2volId.find(wdevName);
            return i != wdevName2volId.end();
        }
    void setSocketParams(cybozu::Socket& sock) const {
        util::setSocketParams(sock, keepAliveParams, socketTimeout);
    }
private:
    mutable std::mutex wdevName2VolIdMutex;
    Str2Str wdevName2volId;
};

inline StorageSingleton& getStorageGlobal()
{
    return StorageSingleton::getInstance();
}

static const StorageSingleton& gs = getStorageGlobal();


inline void pushTask(const std::string &volId, size_t delayMs = 0)
{
    LOGs.debug() << __func__ << volId << delayMs;
    getStorageGlobal().taskQueue.push(volId, delayMs);
}

inline void pushTaskForce(const std::string &volId, size_t delayMs)
{
    LOGs.debug() << __func__ << volId << delayMs;
    getStorageGlobal().taskQueue.pushForce(volId, delayMs);
}

namespace storage_local {

void startMonitoring(const std::string& wdevPath, const std::string& volId);
void stopMonitoring(const std::string& wdevPath, const std::string& volId);


inline bool isUnderMonitoring(const std::string& wdevPath)
{
    return gs.logDevMonitor.exists(device::getWdevNameFromWdevPath(wdevPath));;
}


class MonitorManager
{
    const std::string wdevPath;
    const std::string volId;
    bool started_;
    bool dontStop_;
public:
    MonitorManager(const std::string& wdevPath, const std::string& volId)
        : wdevPath(wdevPath), volId(volId), started_(false), dontStop_(false) {
    }
    void start() {
        startMonitoring(wdevPath, volId);
        started_ = true;
    }
    void dontStop() { dontStop_ = true; }
    ~MonitorManager() {
        try {
            if (started_ && !dontStop_) {
                stopMonitoring(wdevPath, volId);
            }
        } catch (...) {
            LOGs.error() << __func__ << "stop monitoring failed" << volId;
        }
    }
};

} // namespace storage_local


inline StorageVolState &getStorageVolState(const std::string &volId)
{
    return getStorageGlobal().stMap.get(volId);
}

namespace storage_local {

StrVec getAllStatusAsStrVec();
StrVec getVolStatusAsStrVec(const std::string &volId, bool isVerbose);

} // namespace storage_local


void c2sStatusServer(protocol::ServerParams &p);
void c2sInitVolServer(protocol::ServerParams &p);
void c2sClearVolServer(protocol::ServerParams &p);
void c2sStartServer(protocol::ServerParams &p);
void c2sStopServer(protocol::ServerParams &p);
void c2sSnapshotServer(protocol::ServerParams &p);


namespace storage_local {

void backupClient(protocol::ServerParams &p, bool isFull);

} // storage_local

inline void c2sFullBkpServer(protocol::ServerParams &p)
{
    const bool isFull = true;
    storage_local::backupClient(p, isFull);
}

inline void c2sHashBkpServer(protocol::ServerParams &p)
{
    const bool isFull = false;
    storage_local::backupClient(p, isFull);
}


void wdevMonitorWorker() noexcept;
void proxyMonitorWorker() noexcept;
void tsDeltaGetterWorker() noexcept;
void startIfNecessary(const std::string &volId);
void c2sResetVolServer(protocol::ServerParams &p);
void c2sResizeServer(protocol::ServerParams &p);
void c2sKickServer(protocol::ServerParams &p);
void c2sSetFullScanBpsServer(protocol::ServerParams &p);
void c2sDumpLogpackHeaderServer(protocol::ServerParams &p);


namespace storage_local {


void verifyMaxWlogSendPbIsNotTooSmall(uint64_t maxWlogSendPb, uint64_t logpackPb, const char *msg);
LogPackHeader readLogPackHeaderOnce(const std::string &volId, uint64_t lsid);
void dumpLogPackHeader(const std::string &volId, uint64_t lsid, const LogPackHeader &packH) noexcept;
bool extractAndSendAndDeleteWlog(const std::string &volId);

SnapshotInfo getLatestSnapshotInfo(const std::string &volId);
TsDelta generateTsDelta(const SnapshotInfo &src, const SnapshotInfo &dst, const std::string& archiveId);


inline void getState(protocol::GetCommandParams &p)
{
    protocol::runGetStateServer(p, getStorageVolState);
}


inline void getHostType(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, storageHT);
}


inline void getVolList(protocol::GetCommandParams &p)
{
    StrVec v = util::getDirNameList(gs.baseDirStr);
    protocol::sendValueAndFin(p, v);
}


inline void getPid(protocol::GetCommandParams &p)
{
    protocol::sendValueAndFin(p, static_cast<size_t>(::getpid()));
}


void isOverflow(protocol::GetCommandParams &p);
std::string getLogUsageForVolume(const std::string& volId, bool throwError);
void getLogUsage(protocol::GetCommandParams &p);
std::string getLatestSnapForVolume(const std::string& volId);

void getLatestSnap(protocol::GetCommandParams &p);
void getUuid(protocol::GetCommandParams &p);
void getTsDelta(protocol::GetCommandParams &p);
void getHandlerStat(protocol::GetCommandParams &p);

} // namespace storage_local

const protocol::GetCommandHandlerMap storageGetHandlerMap = {
    { stateTN, storage_local::getState },
    { hostTypeTN, storage_local::getHostType },
    { volTN, storage_local::getVolList },
    { pidTN, storage_local::getPid },
    { isOverflowTN, storage_local::isOverflow },
    { logUsageTN, storage_local::getLogUsage },
    { getLatestSnapTN, storage_local::getLatestSnap },
    { uuidTN, storage_local::getUuid },
    { getTsDeltaTN, storage_local::getTsDelta },
    { getHandlerStatTN, storage_local::getHandlerStat },
};

inline void c2sGetServer(protocol::ServerParams &p)
{
    protocol::runGetCommandServer(p, gs.nodeId, storageGetHandlerMap,
                                  getStorageGlobal().handlerStatMgr);
}

inline void c2sExecServer(protocol::ServerParams &p)
{
    protocol::runExecServer(p, gs.nodeId, gs.allowExec);
}

const protocol::Str2ServerHandler storageHandlerMap = {
    { statusCN, c2sStatusServer },
    { initVolCN, c2sInitVolServer },
    { clearVolCN, c2sClearVolServer },
    { resetVolCN, c2sResetVolServer },
    { startCN, c2sStartServer },
    { stopCN, c2sStopServer },
    { fullBkpCN, c2sFullBkpServer },
    { hashBkpCN, c2sHashBkpServer },
    { resizeCN, c2sResizeServer },
    { snapshotCN, c2sSnapshotServer },
    { kickCN, c2sKickServer },
    { setFullScanBpsCN, c2sSetFullScanBpsServer },
    { dbgDumpLogpackHeaderCN, c2sDumpLogpackHeaderServer },
    { getCN, c2sGetServer },
    { execCN, c2sExecServer },
};

} // namespace walb
