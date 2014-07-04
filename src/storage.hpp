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

    /**
     * Writable and must be thread-safe.
     */
    std::atomic<bool> forceQuit;
    AtomicMap<StorageVolState> stMap;
    TaskQueue<std::string> taskQueue;
    std::unique_ptr<DispatchTask<std::string, StorageWorker>> dispatcher;
    std::unique_ptr<std::thread> wdevMonitor;
    std::atomic<bool> quitWdevMonitor;
    LogDevMonitor logDevMonitor;
    std::unique_ptr<std::thread> proxyMonitor;
    std::atomic<bool> quitProxyMonitor;
    storage_local::ProxyManager proxyManager;

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
private:
    mutable std::mutex wdevName2VolIdMutex;
    Str2Str wdevName2volId;
};

inline StorageSingleton& getStorageGlobal()
{
    return StorageSingleton::getInstance();
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
    g.taskQueue.push(volId);
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

const StorageSingleton& gs = getStorageGlobal();

inline void StorageVolState::initInner(const std::string& volId)
{
    StorageVolInfo volInfo(gs.baseDirStr, volId);
    if (volInfo.existsVolDir()) {
        sm.set(volInfo.getState());
    } else {
        sm.set(sClear);
    }
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
    v.push_back(fmt("maxWlogSendMb %" PRIu64 "", gs.maxWlogSendMb));
    v.push_back(fmt("delaySecForRetry %zu", gs.delaySecForRetry));
    v.push_back(fmt("maxForegroundTasks %zu", gs.maxForegroundTasks));
    v.push_back(fmt("socketTimeout %zu", gs.socketTimeout));

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
    const char *const FUNC = __func__;
    auto fmt = cybozu::util::formatString;
    StrVec v0, v1;
    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);

    const std::string state = volSt.sm.get();
    if (state == sClear) {
        throw cybozu::Exception(FUNC) << "not found" << volId;
    }

    v0.push_back(fmt("volId %s", volId.c_str()));
    v0.push_back(fmt("state %s", state.c_str()));
    v0.push_back(formatActions("action", volSt.ac, allActionVec));
    v0.push_back(fmt("stopState %s", stopStateToStr(StopState(volSt.stopState.load()))));

    StorageVolInfo volInfo(gs.baseDirStr, volId);
    v1 = volInfo.getStatusAsStrVec(isVerbose);

    std::move(v1.begin(), v1.end(), std::back_inserter(v0));
    return v0;
}

} // namespace storage_local

inline void c2sStatusServer(protocol::ServerParams &p)
{
    packet::Packet pkt(p.sock);
    ProtocolLogger logger(gs.nodeId, p.clientId);
    StrVec params;
    pkt.read(params);

    StrVec v;
    bool sendErr = true;
    try {
        if (params.empty()) {
            v = storage_local::getAllStatusAsStrVec();
        } else {
            const std::string &volId = params[0];
            bool isVerbose = false;
            if (params.size() >= 2) {
                isVerbose = static_cast<int>(cybozu::atoi(params[1])) != 0;
            }
            v = storage_local::getVolStatusAsStrVec(volId, isVerbose);
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
        const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
        const std::string &volId = v[0];
        const std::string &wdevPath = v[1];
        StorageVolState &volSt = getStorageVolState(volId);
        StateMachineTransaction tran(volSt.sm, sClear, stInitVol, FUNC);

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
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    try {
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

/**
 * params[0]: volId
 * params[1]: "master" or "slave".
 */
inline void c2sStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
	    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
	    const std::string &volId = v[0];
		const std::string& role = v[1];
		if (role != "master" && role != "slave") {
            throw cybozu::Exception(FUNC) << "neighter master nor slave" << role;
		}
        const bool isMaster = role == "master";
        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);
        verifyNotStopping(volSt.stopState, volId, FUNC);
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const std::string st = volInfo.getState();
        if (isMaster) {
            StateMachineTransaction tran(volSt.sm, sStopped, stStartMaster, FUNC);
            if (st != sStopped) throw cybozu::Exception(FUNC) << "bad state" << st;
            volInfo.setState(sMaster);
            storage_local::startMonitoring(volInfo.getWdevPath(), volId);
            tran.commit(sMaster);
        } else {
            StateMachineTransaction tran(volSt.sm, sSyncReady, stStartSlave, FUNC);
            if (st != sSyncReady) throw cybozu::Exception(FUNC) << "bad state" << st;
            volInfo.setState(sSlave);
            storage_local::startMonitoring(volInfo.getWdevPath(), volId);
            tran.commit(sSlave);
        }
        pkt.writeFin(msgOk);
        logger.info() << "start succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * params[0]: volId
 * params[1]: StopOpt as string (optional)
 *
 * Master --> Stopped, or Slave --> SyncReady.
 */
inline void c2sStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        std::string volId;
        StopOpt stopOpt;
        std::tie(volId, stopOpt) = parseStopParams(v, FUNC);

        StorageVolState &volSt = getStorageVolState(volId);
        Stopper stopper(volSt.stopState);
        if (!stopper.changeFromNotStopping(stopOpt.isForce() ? ForceStopping : Stopping)) {
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
        if (st == sMaster) {
            StateMachineTransaction tran(sm, sMaster, stStopMaster, FUNC);
            ul.unlock();
            storage_local::stopMonitoring(volInfo.getWdevPath(), volId);
            if (fst != sMaster) throw cybozu::Exception(FUNC) << "bad state" << fst;
            volInfo.setState(sStopped);
            tran.commit(sStopped);
        } else {
            assert(st == sSlave);
            StateMachineTransaction tran(sm, sSlave, stStopSlave, FUNC);
            ul.unlock();
            if (fst != sSlave) throw cybozu::Exception(FUNC) << "bad state" << fst;
            storage_local::stopMonitoring(volInfo.getWdevPath(), volId);
            volInfo.setState(sSyncReady);
            tran.commit(sSyncReady);
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

    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
    const std::string& volId = v[0];
    const uint64_t bulkLb = util::parseBulkLb(v[1], FUNC);
    const uint64_t curTime = ::time(0);
    logger.debug() << volId << bulkLb << curTime;
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
        const std::string &protocolName = isFull ? dirtyFullSyncPN : dirtyHashSyncPN;
        archiveId = protocol::run1stNegotiateAsClient(aSock, gs.nodeId, protocolName);
        packet::Packet aPkt(aSock);
        aPkt.write(storageHT);
        aPkt.write(volId);
        aPkt.write(sizeLb);
        aPkt.write(curTime);
        aPkt.write(bulkLb);
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
        packet::Ack(aSock).recv();
        monitorMgr.start();

        // (7) in storage-daemon.txt
        if (isFull) {
            const std::string bdevPath = volInfo.getWdevPath();
            if (!dirtyFullSyncClient(aPkt, bdevPath, sizeLb, bulkLb, volSt.stopState, gs.forceQuit)) {
                logger.warn() << FUNC << "force stopped" << volId;
                return;
            }
        } else {
            const uint32_t hashSeed = curTime;
            cybozu::util::BlockDevice bd(volInfo.getWdevPath(), O_RDONLY);
            if (!dirtyHashSyncClient(aPkt, bd, sizeLb, bulkLb, hashSeed, volSt.stopState, gs.forceQuit)) {
                logger.warn() << FUNC << "force stopped" << volId;
                return;
            }
        }

        // (8), (9) in storage-daemon.txt
        {
            const uint64_t gidE = volInfo.takeSnapshot(gs.maxWlogSendMb);
            getStorageGlobal().taskQueue.push(volId);
            aPkt.write(MetaSnap(gidB, gidE));
        }
        packet::Ack(aSock).recv();
    }
    ul.lock();
    tran0.commit(sStopped);
    StateMachineTransaction tran1(sm, sStopped, stStartMaster, FUNC);
    volInfo.setState(sMaster);
    tran1.commit(sMaster);
    monitorMgr.dontStop();

    logger.info() << "full-bkp succeeded" << volId << archiveId;
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

/**
 * Take a snapshot to restore in archive hosts.
 */
inline void c2sSnapshotServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    const StrVec v = protocol::recvStrVec(p.sock, 1, FUNC);
    const std::string &volId = v[0];
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
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
        getStorageGlobal().taskQueue.push(volId);
        logger.info() << "snapshot succeeded" << volId << gid;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

namespace storage_local {

inline void readLogPackHeader(device::AsyncWldevReader &reader, LogPackHeader &packH, uint64_t lsid, const char *msg)
{
    reader.read((char *)packH.rawData(), 1);
    if (packH.header().logpack_lsid != lsid) {
        throw cybozu::Exception(msg)
            << "logpack lsid invalid" << packH.header().logpack_lsid << lsid;
    }
    reader.readAhead();
}

inline void readLogIo(device::AsyncWldevReader &reader, LogPackHeader &packH, size_t recIdx, LogBlockShared &blockS)
{
    const LogRecord &lrec = packH.record(recIdx);
    if (!lrec.hasData()) return;

    const size_t ioSizePb = lrec.ioSizePb(packH.pbs());
    blockS.resize(ioSizePb);
    for (size_t i = 0; i < ioSizePb; i++) {
        reader.read(blockS.get(i), 1);
    }
    reader.readAhead();
}

inline void verifyMaxWlogSendPbIsNotTooSmall(uint64_t maxWlogSendPb, uint64_t logpackPb, const char *msg)
{
    if (maxWlogSendPb < logpackPb) {
        throw cybozu::Exception(msg)
            << "maxWlogSendPb is too small" << maxWlogSendPb << logpackPb;
    }
}

/**
 * Delete all wlogs which lsid is less than a specifeid lsid.
 * Given INVALID_LSID, all existing wlogs will be deleted.
 *
 * RETURN:
 *   true if all the wlogs have been deleted.
 */
inline bool deleteWlogs(const std::string &volId, uint64_t lsid = INVALID_LSID)
{
    StorageVolInfo volInfo(gs.baseDirStr, volId);
    const std::string wdevPath = volInfo.getWdevPath();
    const uint64_t remainingPb = device::eraseWal(wdevPath, lsid);
    return remainingPb == 0;
}

/**
 * RETURN:
 *   true if there is remaining to send.
 */
inline bool extractAndSendAndDeleteWlog(const std::string &volId)
{
    const char *const FUNC = __func__;
    StorageVolState &volSt = getStorageVolState(volId);
    StorageVolInfo volInfo(gs.baseDirStr, volId);

    if (!volInfo.isRequiredWlogTransfer()) {
        LOGs.debug() << FUNC << "not required to wlog-transfer";
        return false;
    }

    MetaLsidGid rec0, rec1;
    uint64_t lsidLimit;
    std::tie(rec0, rec1, lsidLimit) = volInfo.prepareWlogTransfer(gs.maxWlogSendMb);
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
            serverId = protocol::run1stNegotiateAsClient(sock, gs.nodeId, wlogTransferPN);
            pkt.write(volId);
            pkt.write(uuid);
            pkt.write(pbs);
            pkt.write(salt);
            pkt.write(volSizeLb);
            pkt.write(maxLogSizePb);
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
    log::Sender sender(sock, logger);
    sender.setParams(pbs, salt);
    sender.start();

    LogPackHeader packH(pbs, salt);
    reader.reset(lsidB);

    LogBlockShared blockS(pbs);
    uint64_t lsid = lsidB;
    for (;;) {
        if (volSt.stopState == ForceStopping || gs.forceQuit) {
            throw cybozu::Exception(FUNC) << "force stopped" << volId;
        }
        if (lsid == lsidLimit) break;
        readLogPackHeader(reader, packH, lsid, FUNC);
        verifyMaxWlogSendPbIsNotTooSmall(maxWlogSendPb, packH.header().total_io_size + 1, FUNC);
        const uint64_t nextLsid =  packH.nextLogpackLsid();
        if (lsidLimit < nextLsid) break;
        sender.pushHeader(packH);
        for (size_t i = 0; i < packH.header().n_records; i++) {
            readLogIo(reader, packH, i, blockS);
            sender.pushIo(packH, i, blockS);
        }
        lsid = nextLsid;
    }
    sender.sync();
    const uint64_t lsidE = lsid;
    const MetaDiff diff = volInfo.getTransferDiff(rec0, rec1, lsidE);
    pkt.write(diff);
    packet::Ack(sock).recv();
    const bool isRemaining = volInfo.finishWlogTransfer(rec0, rec1, lsidE);

    bool isEmpty = true;
    if (lsidB < lsidE) isEmpty = storage_local::deleteWlogs(volId, lsidE);

    return !isEmpty || isRemaining;
}

} // storage_local

/**
 * Run wlog-trnasfer or wlog-remove for a specified volume.
 */
inline void StorageWorker::operator()()
{
    const char *const FUNC = __func__;
    StorageVolState& volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    const std::string st = volSt.sm.get();
    StorageSingleton &g = getStorageGlobal();
    if (st == stStartSlave || st == stStartMaster) {
        // This is rare case, but possible.
        g.taskQueue.push(volId, 1000);
        return;
    }
    verifyStateIn(st, sAcceptForWlogAction, FUNC);
    verifyActionNotRunning(volSt.ac, allActionVec, FUNC);

    if (st == sSlave) {
        ActionCounterTransaction tran(volSt.ac, saWlogRemove);
        ul.unlock();
        storage_local::deleteWlogs(volId);
        return;
    }

    ActionCounterTransaction tran(volSt.ac, saWlogSend);
    ul.unlock();
    try {
        const bool isRemaining = storage_local::extractAndSendAndDeleteWlog(volId);
        if (isRemaining) g.taskQueue.push(volId);
    } catch (...) {
        g.taskQueue.pushForce(volId, gs.delaySecForRetry * 1000);
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
                g.taskQueue.push(volId, delayMs);
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

inline void startIfNecessary(const std::string &volId)
{
    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    StorageVolInfo volInfo(gs.baseDirStr, volId);

    const std::string st = volSt.sm.get();
    if (st == sMaster || st == sSlave) {
        storage_local::startMonitoring(volInfo.getWdevPath(), volId);
    }
    LOGs.info() << "start monitoring" << volId;
}

/**
 * This is for test and debug.
 *
 * params[0]: volId.
 * params[1]: gid as string (optional)
 */
inline void c2sResetVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    bool sendErr = true;
    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        if (v.empty()) throw cybozu::Exception(FUNC) << "empty param";
        const std::string &volId = v[0];
        const uint64_t gid = (v.size() >= 2 ? static_cast<uint64_t>(cybozu::atoi(v[1])) : 0);

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
 *
 * params[0]: volId
 * params[1]: size [byte] suffix k/m/g/t can be used.
 */
inline void c2sResizeServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    packet::Packet pkt(p.sock);

    try {
        StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        if (v.size() != 2) throw cybozu::Exception(FUNC) << "bad params";
        const std::string &volId = v[0];
        const uint64_t newSizeLb = util::parseSizeLb(v[1], FUNC);

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
        protocol::recvStrVec(p.sock, 0, __func__);
        getStorageGlobal().proxyManager.kick();

        StorageSingleton& g = getStorageGlobal();
        size_t num = 0;
        std::stringstream ss;
        for (const auto &pair : g.taskQueue.getAll()) {
            const std::string &volId = pair.first;
            const int64_t delay = pair.second;
            if (delay > 0) {
                g.taskQueue.pushForce(volId, 0); // run immediately
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

inline void isOverflow(protocol::GetCommandParams &p)
{
    const char *const FUNC = __func__;
    std::string volId;
    cybozu::util::parseStrVec(p.params, 1, 1, {&volId});
    StorageVolState &volSt = getStorageVolState(volId);
    const std::string st = volSt.sm.get();
    if (st == sClear) {
        throw cybozu::Exception(FUNC) << "bad state" << st;
    }
    const StorageVolInfo volInfo(gs.baseDirStr, volId);
    const std::string wdevPath = volInfo.getWdevPath();
    const bool isOverflow = walb::device::isOverflow(wdevPath);
    protocol::sendValueAndFin(p, size_t(isOverflow));
}

} // namespace storage_local

const protocol::GetCommandHandlerMap storageGetHandlerMap = {
    { stateTN, storage_local::getState },
    { hostTypeTN, storage_local::getHostType },
    { volTN, storage_local::getVolList },
    { isOverflowTN, storage_local::isOverflow },
};

inline void c2sGetServer(protocol::ServerParams &p)
{
    protocol::runGetCommandServer(p, gs.nodeId, storageGetHandlerMap);
}

inline void c2sExecServer(protocol::ServerParams &p)
{
    protocol::runExecServer(p, gs.nodeId);
}

const std::map<std::string, protocol::ServerHandler> storageHandlerMap = {
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
    { getCN, c2sGetServer },
    { execCN, c2sExecServer },
};

} // namespace walb
