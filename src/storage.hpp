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
    size_t delaySecForRetry;
    size_t maxForegroundTasks;
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

const StorageSingleton& gs = getStorageGlobal();

inline void pushTask(const std::string &volId, size_t delayMs=0)
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

inline void startMonitoring(const std::string& wdevPath, const std::string& volId)
{
    const char *const FUNC = __func__;
    StorageSingleton &g = getStorageGlobal();
    const std::string wdevName = device::getWdevNameFromWdevPath(wdevPath);
    g.addWdevName(wdevName, volId);
    if (!g.logDevMonitor.add(wdevName)) {
        throw cybozu::Exception(FUNC) << "failed to add" << volId << wdevName;
    }
    pushTask(volId);
}

inline void stopMonitoring(const std::string& wdevPath, const std::string& volId)
{
    StorageSingleton &g = getStorageGlobal();
    const std::string wdevName = device::getWdevNameFromWdevPath(wdevPath);
    g.logDevMonitor.del(wdevName);
    g.delWdevName(wdevName);
    g.taskQueue.remove([&](const std::string &volId2) {
            return volId == volId2;
        });
}

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

inline void StorageVolState::initInner(const std::string& volId)
{
    StorageVolInfo volInfo(gs.baseDirStr, volId);
    if (volInfo.existsVolDir()) {
        sm.set(volInfo.getState());
    } else {
        sm.set(sClear);
    }
    LOGs.debug() << "StorageVolState::initInner" << sm.get();
}

inline StorageVolState &getStorageVolState(const std::string &volId)
{
    return getStorageGlobal().stMap.get(volId);
}

namespace storage_local {

inline StrVec getAllStatusAsStrVec()
{
    StrVec v;
    auto fmt = cybozu::util::formatString;

    v.push_back("-----StorageGlobal-----");
    v.push_back(fmt("nodeId %s", gs.nodeId.c_str()));
    v.push_back(fmt("baseDir %s", gs.baseDirStr.c_str()));
    v.push_back(fmt("maxWlogSendMb %" PRIu64, gs.maxWlogSendMb));
    v.push_back(fmt("delaySecForRetry %zu", gs.delaySecForRetry));
    v.push_back(fmt("maxForegroundTasks %zu", gs.maxForegroundTasks));
    v.push_back(fmt("socketTimeout %zu", gs.socketTimeout));
    v.push_back(fmt("keepAlive %s", gs.keepAliveParams.toStr().c_str()));

    v.push_back("-----Archive-----");
    v.push_back(fmt("host %s:%u", gs.archive.toStr().c_str(), gs.archive.getPort()));

    v.push_back("-----Proxy-----");
    for (std::string &s : gs.proxyManager.getAsStrVec()) {
        v.push_back(std::move(s));
    }

    v.push_back("-----TaskQueue-----");
    for (const auto &pair : gs.taskQueue.getAll()) {
        const std::string &volId = pair.first;
        const int64_t &timeDiffMs = pair.second;
        v.push_back(fmt("volume %s timeDiffMs %" PRIi64 "", volId.c_str(), timeDiffMs));
    }

    v.push_back("-----Volume-----");
    for (const std::string &volId : gs.stMap.getKeyList()) {
        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);

        const std::string state = volSt.sm.get();
        if (state == sClear) continue;

        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const std::string wdevPath = volInfo.getWdevPath();
        const uint64_t logUsagePb = device::getLogUsagePb(wdevPath);
        const uint64_t logCapacityPb = device::getLogCapacityPb(wdevPath);
        uint64_t oldestGid, latestGid;
        std::tie(oldestGid, latestGid) = volInfo.getGidRange();
        const uint64_t oldestLsid = device::getOldestLsid(wdevPath);
        const uint64_t permanentLsid = device::getPermanentLsid(wdevPath);

        const std::string volStStr = fmt(
            "volume %s state %s logUsagePb %" PRIu64 " logCapacityPb %" PRIu64 ""
            " oldestGid %" PRIu64 " latestGid %" PRIu64 ""
            " oldestLsid %" PRIu64 " permanentLsid %" PRIu64 ""
            , volId.c_str(), state.c_str()
            , logUsagePb, logCapacityPb
            , oldestGid, latestGid, oldestLsid, permanentLsid);
        v.push_back(volStStr);
    }
    return v;
}

inline StrVec getVolStatusAsStrVec(const std::string &volId, bool isVerbose)
{
    auto fmt = cybozu::util::formatString;
    StrVec v;
    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);

    const std::string state = volSt.sm.get();
    v.push_back(fmt("hostType storage"));
    v.push_back(fmt("volId %s", volId.c_str()));
    v.push_back(fmt("state %s", state.c_str()));
    if (state == sClear) return v;

    v.push_back(formatActions("action", volSt.ac, allActionVec));
    v.push_back(fmt("stopState %s", stopStateToStr(StopState(volSt.stopState.load()))));
    StorageVolInfo volInfo(gs.baseDirStr, volId);
    v.push_back(fmt("isUnderMonitoring %d", isUnderMonitoring(volInfo.getWdevPath())));
    for (std::string& s : volInfo.getStatusAsStrVec(isVerbose)) {
        v.push_back(std::move(s));
    }
    return v;
}

} // namespace storage_local

inline void c2sStatusServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        StrVec v;
        const VolIdOrAllParam param = parseVolIdOrAllParam(protocol::recvStrVec(p.sock, 0, FUNC), 0);
        if (param.isAll) {
            v = storage_local::getAllStatusAsStrVec();
        } else {
            const bool isVerbose = true;
            v = storage_local::getVolStatusAsStrVec(param.volId, isVerbose);
        }
        protocol::sendValueAndFin(pkt, sendErr, v);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2sInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const InitVolParam param = parseInitVolParam(protocol::recvStrVec(p.sock, 2, FUNC), true);
        const std::string &volId = param.volId;
        const std::string &wdevPath = param.wdevPath;
        StorageVolState &volSt = getStorageVolState(volId);
        StateMachineTransaction tran(volSt.sm, sClear, stInitVol, FUNC);

        if (gs.existsWdevName(device::getWdevNameFromWdevPath(wdevPath))) {
            throw cybozu::Exception(FUNC) << "wdevPath is already used" << volId << wdevPath;
        }
        StorageVolInfo volInfo(gs.baseDirStr, volId, wdevPath);
        volInfo.init();
        tran.commit(sSyncReady);
        pkt.writeFin(msgOk);
        logger.info() << "initVol succeeded" << volId << wdevPath;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

inline void c2sClearVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);
        StorageVolState &volSt = getStorageVolState(volId);
        StateMachineTransaction tran(volSt.sm, sSyncReady, stClearVol, FUNC);

        StorageVolInfo volInfo(gs.baseDirStr, volId);
        volInfo.clear();
        tran.commit(sClear);
        pkt.writeFin(msgOk);
        logger.info() << "clearVol succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

inline void c2sStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const StartParam param = parseStartParam(protocol::recvStrVec(p.sock, 2, FUNC), true);
        const std::string &volId = param.volId;
        const bool isTarget = param.isTarget;

        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const std::string wdevPath = volInfo.getWdevPath();
        const bool isOverflow = device::isOverflow(wdevPath);
        const std::string st = volInfo.getState();
        if (isTarget) {
            if (isOverflow) {
                throw cybozu::Exception(FUNC) << "overflow" << volId << wdevPath;
            }
            StateMachineTransaction tran(volSt.sm, sStopped, stStartTarget, FUNC);
            if (st != sStopped) throw cybozu::Exception(FUNC) << "bad state" << st;
            storage_local::startMonitoring(volInfo.getWdevPath(), volId);
            volInfo.setState(sTarget);
            tran.commit(sTarget);
        } else {
            StateMachineTransaction tran(volSt.sm, sSyncReady, stStartStandby, FUNC);
            if (st != sSyncReady) throw cybozu::Exception(FUNC) << "bad state" << st;
            if (isOverflow) {
                volInfo.resetWlog(0);
            }
            storage_local::startMonitoring(volInfo.getWdevPath(), volId);
            volInfo.setState(sStandby);
            tran.commit(sStandby);
        }
        pkt.writeFin(msgOk);
        logger.info() << "start succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * Target --> Stopped, or Standby --> SyncReady.
 */
inline void c2sStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StopParam param = parseStopParam(protocol::recvStrVec(p.sock, 0, FUNC), false);
        const std::string &volId = param.volId;

        StorageVolState &volSt = getStorageVolState(volId);
        Stopper stopper(volSt.stopState);
        if (!stopper.changeFromNotStopping(param.stopOpt.isForce() ? ForceStopping : Stopping)) {
            throw cybozu::Exception(FUNC) << "already under stopping" << volId;
        }
        pkt.writeFin(msgAccept);
        sendErr = false;
        UniqueLock ul(volSt.mu);
        StateMachine &sm = volSt.sm;

        waitUntil(ul, [&]() {
                return isStateIn(volSt.sm.get(), sSteadyStates)
                    && volSt.ac.isAllZero(allActionVec);
            }, FUNC);

        const std::string st = sm.get();
        verifyStateIn(st, sAcceptForStop, FUNC);

        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const std::string fst = volInfo.getState();
        {
            static const struct State {
                const char *from;
                const char *pass;
                const char *to;
            } stateTbl[] = {
                { sTarget, stStopTarget, sStopped },
                { sStandby, stStopStandby, sSyncReady },
            };
            const State& s = stateTbl[st == sTarget ? 0 : 1];
            StateMachineTransaction tran(sm, s.from, s.pass, FUNC);
            ul.unlock();
            if (fst != s.from) throw cybozu::Exception(FUNC) << "bad state" << fst;
            storage_local::stopMonitoring(volInfo.getWdevPath(), volId);
            volInfo.setState(s.to);
            tran.commit(s.to);
        }
        logger.info() << "stop succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

namespace storage_local {

inline void backupClient(protocol::ServerParams &p, bool isFull)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);

    const BackupParam param = parseBackupParam(protocol::recvStrVec(p.sock, 0, FUNC));
    const std::string& volId = param.volId;
    const uint64_t bulkLb = param.bulkLb;
    const uint64_t curTime = ::time(0);
    logger.debug() << FUNC << volId << bulkLb << curTime;
    std::string archiveId;

    ForegroundCounterTransaction foregroundTasksTran;
    verifyMaxForegroundTasks(gs.maxForegroundTasks, FUNC);

    StorageVolInfo volInfo(gs.baseDirStr, volId);

    packet::Packet cPkt(p.sock);

    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);

    StateMachine &sm = volSt.sm;

    const std::string &st = isFull ? stFullSync : stHashSync;
    StateMachineTransaction tran0(sm, sSyncReady, st, FUNC);
    ul.unlock();

    const uint64_t sizeLb = device::getSizeLb(volInfo.getWdevPath());
    storage_local::MonitorManager monitorMgr(volInfo.getWdevPath(), volId);

    const cybozu::SocketAddr& archive = gs.archive;
    {
        cybozu::Socket aSock;
        util::connectWithTimeout(aSock, archive, gs.socketTimeout);
        gs.setSocketParams(aSock);
        const std::string &protocolName = isFull ? dirtyFullSyncPN : dirtyHashSyncPN;
        archiveId = protocol::run1stNegotiateAsClient(aSock, gs.nodeId, protocolName);
        packet::Packet aPkt(aSock);
        aPkt.write(storageHT);
        aPkt.write(volId);
        aPkt.write(sizeLb);
        aPkt.write(curTime);
        aPkt.write(bulkLb);
        aPkt.flush();
        logger.debug() << "send" << storageHT << volId << sizeLb << curTime << bulkLb;
        {
            std::string res;
            aPkt.read(res);
            if (res == msgAccept) {
                cPkt.writeFin(msgAccept);
            } else {
                cybozu::Exception e(FUNC);
                e << "bad response" << archiveId << res;
                cPkt.write(e.what());
                throw e;
            }
        }
        MetaSnap snap;
        if (!isFull) aPkt.read(snap);
        const uint64_t gidB = isFull ? 0 : snap.gidE + 1;
        volInfo.resetWlog(gidB);
        const cybozu::Uuid uuid = volInfo.getUuid();
        aPkt.write(uuid);
        aPkt.flush();
        packet::Ack(aSock).recv();
        monitorMgr.start();

        // (7) in storage-daemon.txt
        logger.info() << (isFull ? dirtyFullSyncPN : dirtyHashSyncPN)
                      << "started" << volId << archiveId;
        if (isFull) {
            const std::string bdevPath = volInfo.getWdevPath();
            if (!dirtyFullSyncClient(aPkt, bdevPath, 0, sizeLb, bulkLb, volSt.stopState, gs.ps, gs.fullScanLbPerSec)) {
                logger.warn() << FUNC << "force stopped" << volId;
                return;
            }
        } else {
            const uint32_t hashSeed = curTime;
            AsyncBdevReader reader(volInfo.getWdevPath());
            if (!dirtyHashSyncClient(aPkt, reader, sizeLb, bulkLb, hashSeed, volSt.stopState, gs.ps, gs.fullScanLbPerSec)) {
                logger.warn() << FUNC << "force stopped" << volId;
                return;
            }
        }

        // (8), (9) in storage-daemon.txt
        {
            const uint64_t gidE = volInfo.takeSnapshot(gs.maxWlogSendMb);
            pushTask(volId);
            aPkt.write(MetaSnap(gidB, gidE));
            aPkt.flush();
        }
        packet::Ack(aSock).recv();
    }
    ul.lock();
    tran0.commit(sStopped);
    StateMachineTransaction tran1(sm, sStopped, stStartTarget, FUNC);
    volInfo.setState(sTarget);
    tran1.commit(sTarget);
    monitorMgr.dontStop();
    logger.info() << (isFull ? dirtyFullSyncPN : dirtyHashSyncPN)
                  << "succeeded" << volId << archiveId;
}

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

inline void c2sSnapshotServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const std::string volId = parseVolIdParam(protocol::recvStrVec(p.sock, 1, FUNC), 0);

        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);
        const std::string st = volSt.sm.get();
        verifyStateIn(st, sAcceptForSnapshot, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);

        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const uint64_t gid = volInfo.takeSnapshot(gs.maxWlogSendMb);
        pkt.write(msgOk);
        sendErr = false;
        pkt.writeFin(gid);
        pushTaskForce(volId, 0);
        logger.info() << "snapshot succeeded" << volId << gid;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

namespace storage_local {

inline void verifyMaxWlogSendPbIsNotTooSmall(uint64_t maxWlogSendPb, uint64_t logpackPb, const char *msg)
{
    if (maxWlogSendPb < logpackPb) {
        throw cybozu::Exception(msg)
            << "maxWlogSendPb is too small" << maxWlogSendPb << logpackPb;
    }
}

/**
 * Nothing will be checked. Just read.
 */
inline LogPackHeader readLogPackHeaderOnce(const std::string &volId, uint64_t lsid)
{
    StorageVolInfo volInfo(gs.baseDirStr, volId);
    const std::string wdevPath = volInfo.getWdevPath();
    const std::string wdevName = device::getWdevNameFromWdevPath(wdevPath);
    const std::string wldevPath = device::getWldevPathFromWdevName(wdevName);
    device::SimpleWldevReader reader(wldevPath);
    const uint32_t pbs = reader.super().getPhysicalBlockSize();
    const uint32_t salt = reader.super().getLogChecksumSalt();
    reader.reset(lsid);
    LogPackHeader packH(pbs, salt);
    packH.rawReadFrom(reader);
    return packH;
}

inline void dumpLogPackHeader(const std::string &volId, uint64_t lsid, const LogPackHeader &packH) noexcept
{
    try {
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const cybozu::FilePath& volDir = volInfo.getVolDir();
        cybozu::TmpFile tmpFile(volDir.str());
        cybozu::util::File file(tmpFile.fd());
        file.write(packH.rawData(), packH.pbs());
        cybozu::FilePath outPath(volDir);
        outPath += cybozu::util::formatString("logpackheader-%" PRIu64 "", lsid);
        tmpFile.save(outPath.str());
    } catch (std::exception &e) {
        LOGs.error() << __func__ << volId << lsid << e.what();
    }
}

/**
 * RETURN:
 *   true if there is remaining to send or delete.
 */
inline bool extractAndSendAndDeleteWlog(const std::string &volId)
{
    const char *const FUNC = __func__;
    StorageVolState &volSt = getStorageVolState(volId);
    StorageVolInfo volInfo(gs.baseDirStr, volId);

    bool isRemainingGarbage = volInfo.deleteGarbageWlogs();
    if (!volInfo.mayWlogTransferBeRequiredNow()) {
        LOGs.debug() << FUNC << "no need to run wlog-transfer now" << volId;
        return isRemainingGarbage || volInfo.isWlogTransferRequiredLater();
    }

    MetaLsidGid rec0, rec1;
    uint64_t lsidLimit;
    bool doLater;
    std::tie(rec0, rec1, lsidLimit, doLater) = volInfo.prepareWlogTransfer(gs.maxWlogSendMb);
    if (doLater) {
        LOGs.debug() << FUNC << "wait a bit for wlogs to be permanent" << volId;
        return true;
    }
    const std::string wdevPath = volInfo.getWdevPath();
    const std::string wdevName = device::getWdevNameFromWdevPath(wdevPath);
    const std::string wldevPath = device::getWldevPathFromWdevName(wdevName);
    device::AsyncWldevReader reader(wldevPath);
    const uint32_t pbs = reader.super().getPhysicalBlockSize();
    const uint32_t salt = reader.super().getLogChecksumSalt();
    const uint64_t maxWlogSendPb = gs.maxWlogSendMb * MEBI / pbs;
    const uint64_t lsidB = rec0.lsid;
    const cybozu::Uuid uuid = volInfo.getUuid();
    const uint64_t volSizeLb = device::getSizeLb(wdevPath);
    const uint64_t maxLogSizePb = lsidLimit - lsidB;

    cybozu::Socket sock;
    packet::Packet pkt(sock);
    std::string serverId;
    bool isAvailable = false;
    for (const cybozu::SocketAddr &proxy : gs.proxyManager.getAvailableList()) {
        try {
            util::connectWithTimeout(sock, proxy, gs.socketTimeout);
            gs.setSocketParams(sock);
            serverId = protocol::run1stNegotiateAsClient(sock, gs.nodeId, wlogTransferPN);
            pkt.write(volId);
            pkt.write(uuid);
            pkt.write(pbs);
            pkt.write(salt);
            pkt.write(volSizeLb);
            pkt.write(maxLogSizePb);
            pkt.flush();
            LOGs.debug() << "send" << volId << uuid << pbs << salt << volSizeLb << maxLogSizePb;
            std::string res;
            pkt.read(res);
            if (res == msgAccept) {
                isAvailable = true;
                break;
            }
            LOGs.warn() << FUNC << res;
        } catch (std::exception &e) {
            LOGs.warn() << FUNC << e.what();
        }
    }
    if (!isAvailable) {
        throw cybozu::Exception(FUNC) << "There is no available proxy";
    }

    ProtocolLogger logger(gs.nodeId, serverId);
    WlogSender sender(sock, logger, pbs, salt);

    LogPackHeader packH(pbs, salt);
    reader.reset(lsidB);

    LOGs.debug() << FUNC << "start" << volId << lsidB << lsidLimit;
    LogBlockShared blockS;
    uint64_t lsid = lsidB;
    try {
        for (;;) {
            if (volSt.stopState == ForceStopping || gs.ps.isForceShutdown()) {
                throw cybozu::Exception(FUNC) << "force stopped" << volId;
            }
            if (lsid == lsidLimit) break;
            if (!readLogPackHeader(reader, packH, lsid)) {
                dumpLogPackHeader(volId, lsid, packH); // for analysis.
                throw cybozu::Exception(FUNC) << "invalid logpack header" << volId << lsid;
            }
            verifyMaxWlogSendPbIsNotTooSmall(maxWlogSendPb, packH.header().total_io_size + 1, FUNC);
            const uint64_t nextLsid =  packH.nextLogpackLsid();
            if (lsidLimit < nextLsid) break;
            sender.pushHeader(packH);
            for (size_t i = 0; i < packH.header().n_records; i++) {
                if (!readLogIo(reader, packH, i, blockS)) {
                    throw cybozu::Exception(FUNC) << "invalid logpack IO" << volId << lsid << i;
                }
                sender.pushIo(packH, i, blockS);
            }
            lsid = nextLsid;
        }
    } catch (...) {
        LOGs.info() << FUNC << volId << lsidB << lsid << lsidLimit;
        throw;
    }
    sender.sync();
    const uint64_t lsidE = lsid;
    const MetaDiff diff = volInfo.getTransferDiff(rec0, rec1, lsidE);
    pkt.write(diff);
    pkt.flush();
    packet::Ack(sock).recv();
    const bool isRemainingData = volInfo.finishWlogTransfer(rec0, rec1, lsidE);
    isRemainingGarbage = volInfo.deleteGarbageWlogs();
    LOGs.debug() << FUNC << "end  " << volId << lsidB << lsidE;
    return isRemainingData || isRemainingGarbage || volInfo.isWlogTransferRequiredLater();
}

} // storage_local

/**
 * Run wlog-trnasfer or wlog-remove for a specified volume.
 */
inline void StorageWorker::operator()()
{
    const char *const FUNC = "StorageWorker::operator()";
    LOGs.debug() << FUNC << "start";
    StorageVolState& volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    const std::string st = volSt.sm.get();
    LOGs.debug() << FUNC << volId << st;
    if (st == stStartStandby || st == stStartTarget) {
        // This is rare case, but possible.
        pushTask(volId, 1000);
        return;
    }
    verifyStateIn(st, sAcceptForWlogAction, FUNC);
    verifyActionNotRunning(volSt.ac, allActionVec, FUNC);

    StorageVolInfo volInfo(gs.baseDirStr, volId);
    const std::string wdevPath = volInfo.getWdevPath();
    if (device::isOverflow(wdevPath)) {
        LOGs.error() << FUNC << "overflow" << volId << wdevPath;
        // stop to push the task and change state to stop if target
        if (st != sTarget) return;
        StateMachineTransaction tran(volSt.sm, sTarget, stStopTarget, FUNC);
        ul.unlock();
        storage_local::stopMonitoring(wdevPath, volId);
        volInfo.setState(sStopped);
        tran.commit(sStopped);
        return;
    }

    if (st == sStandby) {
        ActionCounterTransaction tran(volSt.ac, saWlogRemove);
        ul.unlock();
        volInfo.deleteAllWlogs();
        return;
    }

    ActionCounterTransaction tran(volSt.ac, saWlogSend);
    ul.unlock();
    try {
        const bool isRemaining = storage_local::extractAndSendAndDeleteWlog(volId);
        if (isRemaining) pushTask(volId);
    } catch (...) {
        pushTaskForce(volId, gs.delaySecForRetry * 1000);
        throw;
    }
}

namespace storage_local {

inline ProxyManager::Info ProxyManager::checkAvailability(const cybozu::SocketAddr &proxy)
{
    const char *const FUNC = __func__;
    Info info(proxy);
    info.isAvailable = false;
    try {
        cybozu::Socket sock;
        util::connectWithTimeout(sock, proxy, PROXY_HEARTBEAT_SOCKET_TIMEOUT_SEC);
        const std::string type = protocol::runGetHostTypeClient(sock, gs.nodeId);
        if (type == proxyHT) info.isAvailable = true;
    } catch (std::exception &e) {
        LOGs.warn() << FUNC << e.what();
    } catch (...) {
        LOGs.warn() << FUNC << "unknown error";
    }
    info.checkedTime = Clock::now();
    return info;
}

inline void ProxyManager::tryCheckAvailability()
{
    Info *target = nullptr;
    {
        TimePoint now = Clock::now();
        AutoLock lk(mu_);
        TimePoint minCheckedTime = now - Seconds(PROXY_HEARTBEAT_INTERVAL_SEC);
        for (Info &info : v_) {
            if (info.checkedTime < minCheckedTime) {
                minCheckedTime = info.checkedTime;
                target = &info;
            }
        }
    }
    if (!target) return;
    Info info = checkAvailability(target->proxy);
    {
        AutoLock lk(mu_);
        *target = info;
    }
}

inline SnapshotInfo getLatestSnapshotInfo(const std::string &volId)
{
    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string state = volSt.sm.get();
    SnapshotInfo snapInfo;
    snapInfo.init();
    snapInfo.volId = volId;
    if (state != sTarget) return snapInfo;

    StorageVolInfo volInfo(gs.baseDirStr, volId);
    MetaLsidGid lsidGid = volInfo.getLatestSnap();

    snapInfo.gid = lsidGid.gid;
    snapInfo.lsid = lsidGid.lsid;
    snapInfo.timestamp = lsidGid.timestamp;
    return snapInfo;
}


inline TsDelta generateTsDelta(const SnapshotInfo &src, const SnapshotInfo &dst, const std::string& archiveId)
{
    if (src.volId != dst.volId) {
        throw cybozu::Exception(__func__)
            << "bad result" << src.volId << dst.volId;
    }
    return TsDelta{src.volId, archiveId, {"storage", "archive"}
        , {src.gid, dst.gid}, {src.timestamp, dst.timestamp}};
}


inline void TsDeltaManager::connectAndGet()
{
    LOGs.debug() << "connectAndGet called";

    const char *const FUNC = __func__;
    cybozu::Socket sock;
    std::string archiveId;
    try {
        util::connectWithTimeout(sock, gs.archive, gs.socketTimeout);
        gs.setSocketParams(sock);
        archiveId = protocol::run1stNegotiateAsClient(sock, gs.nodeId, gatherLatestSnapPN);
    } catch (std::exception &e) {
        LOGs.warn() << FUNC << e.what();
        return;
    }
    ProtocolLogger logger(gs.nodeId, archiveId);

    // get volumes with Target state.
    packet::Packet pkt(sock);
    std::vector<SnapshotInfo> snapInfoV0;
    StrVec volIdV;
    try {
        for (const std::string &volId : gs.stMap.getKeyList()) {
            const SnapshotInfo snapInfo = getLatestSnapshotInfo(volId);
            if (snapInfo.isUnknown()) continue;
            snapInfoV0.push_back(snapInfo);
            volIdV.push_back(volId);
        }
    } catch (std::exception &e) {
        logger.warn() << FUNC << e.what();
        pkt.write(e.what());
        return;
    }

    // communicate with the archive process.
    pkt.write(msgOk);
    protocol::sendStrVec(sock, volIdV, 0, FUNC, msgAccept);
    std::vector<SnapshotInfo> snapInfoV1;
    pkt.read(snapInfoV1);
    std::string msg;
    pkt.read(msg);
    if (msg != msgOk) {
        throw cybozu::Exception(FUNC) << "something wrong";
    }
    sock.close();

    // store internal map.
    if (snapInfoV0.size() != snapInfoV1.size()) {
        throw cybozu::Exception(FUNC)
            << "bad result" << snapInfoV0.size() << snapInfoV0.size();
    }
    Map map;
    for (size_t i = 0; i < snapInfoV0.size(); i++) {
        const TsDelta tsd = generateTsDelta(snapInfoV0[i], snapInfoV1[i], archiveId);
        map.emplace(tsd.volId, tsd);
    }
    setMap(map);
    logger.debug() << "gather-latest-snap succeeded";
}

} // namespace storage_local

inline void wdevMonitorWorker() noexcept
{
    const char *const FUNC = __func__;
    StorageSingleton& g = getStorageGlobal();
    const int timeoutMs = 1000;
    const int delayMs = 1000;
    while (!g.quitWdevMonitor) {
        try {
            const StrVec v = g.logDevMonitor.poll(timeoutMs);
            if (v.empty()) continue;
            for (const std::string& wdevName : v) {
                LOGs.debug() << FUNC << wdevName;
                const std::string volId = g.getVolIdFromWdevName(wdevName);
                // There is an delay to transfer wlogs in bulk.
                pushTask(volId, delayMs);
            }
        } catch (std::exception& e) {
            LOGs.error() << FUNC << e.what();
        } catch (...) {
            LOGs.error() << FUNC << "unknown error";
        }
    }
}

inline void proxyMonitorWorker() noexcept
{
    const char *const FUNC = __func__;
    StorageSingleton& g = getStorageGlobal();
    const int intervalMs = 1000;
    while (!g.quitProxyMonitor) {
        try {
            g.proxyManager.tryCheckAvailability();
            util::sleepMs(intervalMs);
        } catch (std::exception& e) {
            LOGs.error() << FUNC << e.what();
        } catch (...) {
            LOGs.error() << FUNC << "unknown error";
        }
    }
}

inline void tsDeltaGetterWorker() noexcept
{
    const char *const FUNC = __func__;
    StorageSingleton& g = getStorageGlobal();
    assert(g.tsDeltaGetterIntervalSec > 0);
    size_t remaining = 5; // The first run will be 5 seconds later.
    while (!g.quitTsDeltaGetter) {
        try {
            util::sleepMs(1000);
            if (--remaining == 0) {
                g.tsDeltaManager.connectAndGet();
            }
        } catch (std::exception& e) {
            LOGs.error() << FUNC << e.what();
        } catch (...) {
            LOGs.error() << FUNC << "unknown error";
        }
        if (remaining == 0) {
            remaining = g.tsDeltaGetterIntervalSec;
        }
    }
}

inline void startIfNecessary(const std::string &volId)
{
    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    StorageVolInfo volInfo(gs.baseDirStr, volId);

    const std::string st = volSt.sm.get();
    if (st == sTarget || st == sStandby) {
        storage_local::startMonitoring(volInfo.getWdevPath(), volId);
    }
    LOGs.info() << "start monitoring" << volId;
}

inline void c2sResetVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const VolIdAndGidParam param = parseVolIdAndGidParam(protocol::recvStrVec(p.sock, 0, FUNC), 0, false, 0);
        const std::string &volId = param.volId;
        const uint64_t gid = param.gid;

        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNotStopping(volSt.stopState, volId, FUNC);
        StateMachineTransaction tran(volSt.sm, sStopped, stReset);
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        volInfo.resetWlog(gid);
        tran.commit(sSyncReady);
        pkt.writeFin(msgOk);
        sendErr = false;
        logger.info() << "reset succeeded" << volId << gid;
    } catch (std::exception &e) {
        logger.error() << FUNC << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

/**
 * This will resize just walb device.
 * You must resize underlying devices before calling it.
 */
inline void c2sResizeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const ResizeParam param = parseResizeParam(protocol::recvStrVec(p.sock, 0, FUNC), false, false);
        const std::string &volId = param.volId;
        const uint64_t newSizeLb = param.newSizeLb;

        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        verifyStateIn(volSt.sm.get(), sAcceptForResize, FUNC);

        StorageVolInfo volInfo(gs.baseDirStr, volId);
        volInfo.growWdev(newSizeLb);

        pkt.writeFin(msgOk);
        logger.info() << "resize succeeded" << volId << newSizeLb;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * Kick heartbeat protocol to proxy servers and WlogTransfer retry.
 * No parameter is required.
 */
inline void c2sKickServer(protocol::ServerParams &p)
{
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        protocol::recvStrVec(p.sock, 0, __func__); // ignore the received string vec.
        getStorageGlobal().proxyManager.kick();

        StorageSingleton& g = getStorageGlobal();
        size_t num = 0;
        std::stringstream ss;
        for (const auto &pair : g.taskQueue.getAll()) {
            const std::string &volId = pair.first;
            const int64_t delay = pair.second;
            if (delay > 0) {
                pushTaskForce(volId, 0); // run immediately
                ss << volId << ",";
                num++;
            }
        }

        pkt.writeFin(msgOk);
        logger.info() << "kick" << num << ss.str();
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

inline void c2sSetFullScanBpsServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const uint64_t size = parseSetFullScanBps(protocol::recvStrVec(p.sock, 0, FUNC));
        StorageSingleton& g = getStorageGlobal();
        g.fullScanLbPerSec = size / LOGICAL_BLOCK_SIZE;
        pkt.writeFin(msgOk);
        logger.info() << "set-full-scan-bps" << size;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

inline void c2sDumpLogpackHeaderServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        const VolIdAndLsidParam param = parseVolIdAndLsidParam(protocol::recvStrVec(p.sock, 0, FUNC));
        const std::string &volId = param.volId;
        const uint64_t lsid = param.lsid;

        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);
        const std::string st = volSt.sm.get();
        if (st == sClear) throw cybozu::Exception(FUNC) << "not found" << volId;

        LogPackHeader packH = storage_local::readLogPackHeaderOnce(volId, lsid);
        storage_local::dumpLogPackHeader(volId, lsid, packH);

        ul.unlock();
        pkt.writeFin(msgOk);
        logger.info() << "dump-logpack-header" << volId << lsid;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

namespace storage_local {

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

inline void isOverflow(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    const std::string volId = parseVolIdParam(p.params, 1);

    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (st == sClear) {
        throw cybozu::Exception(FUNC) << "bad state" << st;
    }
    const StorageVolInfo volInfo(gs.baseDirStr, volId);
    const std::string wdevPath = volInfo.getWdevPath();
    const bool isOverflow = walb::device::isOverflow(wdevPath);
    ul.unlock();
    protocol::sendValueAndFin(p, size_t(isOverflow));
    p.logger.debug() << "get overflow succeeded" << volId << isOverflow;
}

inline std::string getLogUsageForVolume(const std::string& volId, bool throwError)
{
    const char *const FUNC = __func__;
    auto fmt = cybozu::util::formatString;

    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (st == sClear) {
        if (throwError) throw cybozu::Exception(FUNC) << "bad state" << volId << st;
        return std::string();
    }
    const StorageVolInfo volInfo(gs.baseDirStr, volId);
    const std::string wdevPath = volInfo.getWdevPath();
    const uint64_t logUsagePb = device::getLogUsagePb(wdevPath);
    const uint64_t logCapacityPb = device::getLogCapacityPb(wdevPath);
    const uint32_t pbs = volInfo.getPbs();
    ul.unlock();

    return fmt("name:%s\t"
               "usage_pb:%" PRIu64 "\t"
               "capacity_pb:%" PRIu64 "\t"
               "pbs:%u"
               , volId.c_str(), logUsagePb, logCapacityPb, pbs);
}

inline void getLogUsage(protocol::GetCommandParams &p)
{
    VolIdOrAllParam param = parseVolIdOrAllParam(p.params, 1);
    StrVec ret;
    if (param.isAll) {
        for (const std::string &volId : gs.stMap.getKeyList()) {
            std::string line = getLogUsageForVolume(volId, false);
            if (line.empty()) continue;
            ret.push_back(std::move(line));
        }
    } else {
        ret.push_back(getLogUsageForVolume(param.volId, true));
    }
    protocol::sendValueAndFin(p, ret);
    p.logger.debug() << "get log-usage succeeded";
}

inline std::string getLatestSnapForVolume(const std::string& volId)
{
    auto fmt = cybozu::util::formatString;

    const SnapshotInfo snapInfo = getLatestSnapshotInfo(volId);
    std::string line;
    if (snapInfo.isUnknown()) return line; // empty.

    line += fmt("name:%s\t", snapInfo.volId.c_str());
    line += "kind:storage\t";
    line += fmt("gid:%" PRIu64 "\t", snapInfo.gid);
    line += fmt("lsid:%" PRIu64 "\t", snapInfo.lsid);
    line += fmt("timestamp:%s", cybozu::unixTimeToPrettyStr(snapInfo.timestamp).c_str());
    return line;
}

inline void getLatestSnap(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    VolIdOrAllParam param = parseVolIdOrAllParam(p.params, 1);
    StrVec ret;
    if (param.isAll) {
        for (const std::string &volId : gs.stMap.getKeyList()) {
            std::string line = getLatestSnapForVolume(volId);
            if (line.empty()) continue;
            ret.push_back(std::move(line));
        }
    } else {
        std::string line = getLatestSnapForVolume(param.volId);
        if (line.empty()) {
            throw cybozu::Exception(FUNC)
                << "could not get latest snapshot for volume" << param.volId;
        }
        ret.push_back(std::move(line));
    }
    protocol::sendValueAndFin(p, ret);
}

inline void getUuid(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    const std::string volId = parseVolIdParam(p.params, 1);

    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    const std::string st = volSt.sm.get();
    if (st == sClear) {
        throw cybozu::Exception(FUNC) << "bad state" << st;
    }
    const StorageVolInfo volInfo(gs.baseDirStr, volId);
    const cybozu::Uuid uuid = volInfo.getUuid();
    ul.unlock();
    const std::string uuidStr = uuid.str();
    protocol::sendValueAndFin(p, uuidStr);
    p.logger.debug() << "get uuid succeeded" << volId << uuidStr;
}

inline void getTsDelta(protocol::GetCommandParams &p)
{
    StrVec ret;
    uint64_t ts = 0;
    const TsDeltaManager::Map map = gs.tsDeltaManager.copyMap(&ts);
    for (const TsDeltaManager::Map::value_type& pair : map) {
        const TsDelta& tsd = pair.second;
        ret.push_back(tsd.toStr());
    }
    protocol::sendValueAndFin(p, ret);
    p.logger.debug() << "get ts-delta succeeded" << cybozu::unixTimeToPrettyStr(ts);
}

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
};

inline void c2sGetServer(protocol::ServerParams &p)
{
    protocol::runGetCommandServer(p, gs.nodeId, storageGetHandlerMap);
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
