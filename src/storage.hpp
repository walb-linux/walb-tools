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

namespace walb {

// action
const char *const sWlogSend = "WlogSend";

struct StorageVolState {
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // key is action identifier.

    explicit StorageVolState(const std::string& volId)
        : stopState(NotStopping), sm(mu), ac(mu) {
        const struct StateMachine::Pair tbl[] = {
            { sClear, stInitVol },
            { stInitVol, sSyncReady },
            { sSyncReady, stClearVol },
            { stClearVol, sClear },

            { sSyncReady, stStartSlave },
            { stStartSlave, sSlave },
            { sSlave, stStopSlave },
            { stStopSlave, sSyncReady },

            { sSlave, stWlogRemove },
            { stWlogRemove, sSlave },

            { sSyncReady, stFullSync },
            { stFullSync, sStopped },
            { sSyncReady, stHashSync },
            { stHashSync, sStopped },
            { sStopped, stReset },
            { stReset, sSyncReady },

            { sStopped, stStartMaster },
            { stStartMaster, sMaster },
            { sMaster, stStopMaster },
            { stStopMaster, sStopped },
        };
        sm.init(tbl);

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
            const int64_t timeDiffSec
                = std::chrono::duration_cast<Seconds>(checkedTime - Clock::now()).count();
            std::stringstream ss;
            ss << "sockaddr " << proxy.toStr()
               << " isAvailable " << (isAvailable ? "1" : "0")
               << " timeDiffSec " << timeDiffSec;
            return ss.str();
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
    void tryCheckAvailability() {
        Info *target = nullptr;
        {
            TimePoint now = Clock::now();
            AutoLock lk(mu_);
            TimePoint minCheckedTime = now - Seconds(PROXY_HEARTBEAT_INTERVAL_SEC);
            for (Info &info : v_) {
                info.checkedTime < minCheckedTime;
                minCheckedTime = info.checkedTime;
                target = &info;
            }
        }
        if (!target) return;
        Info info = checkAvailability(target->proxy);
        AutoLock lk(mu_);
        *target = info;
    }
private:
    void removeFromList(const cybozu::SocketAddr &proxy) {
        v_.erase(std::remove_if(v_.begin(), v_.end(), [&](const Info &info) {
                    return info.proxy == proxy;
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
    bool dontStop_;
public:
    MonitorManager(const std::string& wdevPath, const std::string& volId)
        : wdevPath(wdevPath), volId(volId), dontStop_(false) {
    }
    void start() { startMonitoring(wdevPath, volId); }
    void dontStop() { dontStop_ = true; }
    ~MonitorManager() {
        if (!dontStop_) stopMonitoring(wdevPath, volId);
    }
};

} // storage_local

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

inline StrVec getAllStateStrVec()
{
    StrVec v;
    auto fmt = cybozu::util::formatString;

    v.push_back("-----Archive-----");
    v.push_back(gs.archive.toStr());

    v.push_back("-----Proxy-----");
    for (std::string &s : gs.proxyManager.getAsStrVec()) {
        v.push_back(std::move(s));
    }

    v.push_back("-----TaskQueue-----");
    for (const auto &pair : gs.taskQueue.getAll()) {
        const std::string &volId = pair.first;
        const int64_t &timeDiffMs = pair.second;
        std::stringstream ss;
        ss << "volume " << volId << " timeDiffMs " << timeDiffMs;
        v.push_back(ss.str());
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

        std::string volStStr = fmt(
            "%s %s %" PRIu64 "/%" PRIu64 " %" PRIu64 " %" PRIu64 ""
            , volId.c_str(), state.c_str()
            , logUsagePb, logCapacityPb
            , oldestGid, latestGid);

        v.push_back(volStStr);
    }
    return v;
}

inline StrVec getVolStateStrVec(const std::string &volId, bool isVerbose)
{
    StrVec v;

    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);

    if (volSt.sm.get() == sClear) {
        throw cybozu::Exception(__func__) << "not found" << volId;
    }

    StorageVolInfo volInfo(gs.baseDirStr, volId);
    // TODO: show the memory state of the volume.
    v = volInfo.getStatusAsStrVec(isVerbose);
    return v;
}

} // namespace storage_local

inline void c2sStatusServer(protocol::ServerParams &p)
{
    packet::Packet pkt(p.sock);
    ProtocolLogger logger(gs.nodeId, p.clientId);
    std::vector<std::string> params;
    pkt.read(params);

    StrVec v;
    bool sendErr = true;
    try {
        if (params.empty()) {
            v = storage_local::getAllStateStrVec();
        } else {
            const std::string &volId = params[0];
            bool isVerbose = false;
            if (params.size() >= 2) {
                isVerbose = static_cast<int>(cybozu::atoi(params[1])) != 0;
            }
            v = storage_local::getVolStateStrVec(volId, isVerbose);
        }
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(v);
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2sListVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = util::getDirNameList(gs.baseDirStr);
    protocol::sendStrVec(p.sock, v, 0, FUNC);
    packet::Ack(p.sock).send();
    ProtocolLogger logger(gs.nodeId, p.clientId);
    logger.debug() << "listVol succeeded";
}

inline void c2sInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
    const std::string &volId = v[0];
    const std::string &wdevPath = v[1];
    packet::Packet pkt(p.sock);

    try {
        StorageVolState &volSt = getStorageVolState(volId);
        StateMachineTransaction tran(volSt.sm, sClear, stInitVol, FUNC);

        StorageVolInfo volInfo(gs.baseDirStr, volId, wdevPath);
        volInfo.init();
        tran.commit(sSyncReady);
        pkt.write(msgOk);
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
        pkt.write(msgOk);
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
    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
    ProtocolLogger logger(gs.nodeId, p.clientId);
    const std::string &volId = v[0];
    const bool isMaster = (v[1] == "master");
    packet::Packet pkt(p.sock);

    try {
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
        pkt.write(msgOk);
        logger.info() << "start succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

/**
 * params[0]: volId
 * params[1]: isForce: "0" or "1".
 *
 * Master --> Stopped, or Slave --> SyncReady.
 */
inline void c2sStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);
    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
    const std::string &volId = v[0];
    const int isForceInt = cybozu::atoi(v[1]);
    const bool isForce = (isForceInt != 0);
    packet::Packet pkt(p.sock);

    try {
        StorageVolState &volSt = getStorageVolState(volId);
        Stopper stopper(volSt.stopState, isForce);
        if (!stopper.isSuccess()) {
            throw cybozu::Exception(FUNC) << "already under stopping" << volId;
        }
        UniqueLock ul(volSt.mu);
        StateMachine &sm = volSt.sm;

        waitUntil(ul, [&]() {
                if (!volSt.ac.isAllZero(StrVec{sWlogSend})) return false;
                return isStateIn(sm.get(), {sClear, sSyncReady, sStopped, sMaster, sSlave});
            }, FUNC);

        const std::string st = sm.get();
        verifyStateIn(st, {sMaster, sSlave}, FUNC);
        verifyNoActionRunning(volSt.ac, StrVec{sWlogSend}, FUNC);

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
        pkt.write(msgOk);
        logger.info() << "stop succeeded" << volId;
    } catch (std::exception &e) {
        logger.error() << e.what();
        pkt.write(e.what());
    }
}

namespace storage_local {

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool sendDirtyFullImage(
    packet::Packet &pkt, StorageVolInfo &volInfo,
    uint64_t sizeLb, uint64_t bulkLb, const std::atomic<int> &stopState)
{
    std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
    cybozu::util::BlockDevice bd(volInfo.getWdevPath(), O_RDONLY);
    std::string encBuf;

    uint64_t remainingLb = sizeLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || gs.forceQuit) {
            return false;
        }
        uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        size_t size = lb * LOGICAL_BLOCK_SIZE;
        bd.read(&buf[0], size);
        const size_t encSize = snappy::Compress(&buf[0], size, &encBuf);
        pkt.write(encSize);
        pkt.write(&encBuf[0], encSize);
        remainingLb -= lb;
    }
    return true;
}

inline bool executeDirtyHashSync(
    packet::Packet &pkt, StorageVolInfo &volInfo,
    uint64_t sizeLb, uint64_t bulkLb, const std::atomic<int> &stopState)
{
    // QQQ
    cybozu::disable_warning_unused_variable(pkt);
    cybozu::disable_warning_unused_variable(volInfo);
    cybozu::disable_warning_unused_variable(sizeLb);
    cybozu::disable_warning_unused_variable(bulkLb);
    cybozu::disable_warning_unused_variable(stopState);
    return false;
}

inline void backup(protocol::ServerParams &p, bool isFull)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);

    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC);
    const std::string& volId = v[0];
    const uint64_t bulkLb = cybozu::atoi(v[1]);
    const uint64_t curTime = ::time(0);
    logger.debug() << volId << bulkLb << curTime;
    std::string archiveId;

    ForegroundCounterTransaction foregroundTasksTran;
    verifyMaxForegroundTasks(gs.maxForegroundTasks, FUNC);

    StorageVolInfo volInfo(gs.baseDirStr, volId);

    packet::Packet cPack(p.sock);

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
                cPack.write(msgAccept);
                p.sock.close();
            } else {
                cybozu::Exception e(FUNC);
                e << "bad response" << archiveId << res;
                cPack.write(e.what());
                throw e;
            }
        }
        // (7) in storage-daemon.txt
        MetaSnap snap;
        if (isFull) {
            if (!storage_local::sendDirtyFullImage(aPkt, volInfo, sizeLb, bulkLb, volSt.stopState)) {
                logger.warn() << FUNC << "force stopped" << volId;
                return;
            }
        } else {
            aPkt.read(snap);
            if (!storage_local::executeDirtyHashSync(aPkt, volInfo, sizeLb, bulkLb, volSt.stopState)) {
                logger.warn() << FUNC << "force stopped" << volId;
                return;
            }
        }
        const uint64_t gidB = isFull ? 0 : snap.gidE + 1;
        volInfo.resetWlog(gidB);
        const cybozu::Uuid uuid = volInfo.getUuid();
        aPkt.write(uuid);
        packet::Ack(aSock).recv();
        monitorMgr.start();

        // (8), (9) in storage-daemon.txt
        {
            const uint64_t gidE = volInfo.takeSnapshot(gs.maxWlogSendMb);
            getStorageGlobal().taskQueue.push(volId);
            aPkt.write(gidB);
            aPkt.write(gidE);
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
    storage_local::backup(p, isFull);
}

inline void c2sHashBkpServer(protocol::ServerParams &p)
{
    const bool isFull = false;
    storage_local::backup(p, isFull);
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
        verifyStateIn(st, {sMaster, sStopped}, FUNC);
        verifyNotStopping(volSt.stopState, volId, FUNC);

        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const uint64_t gid = volInfo.takeSnapshot(gs.maxWlogSendMb);
        pkt.write(msgOk);
        sendErr = false;
        pkt.write(gid);
        getStorageGlobal().taskQueue.push(volId);
        logger.info() << "snapshot succeeded" << volId << gid;
    } catch (std::exception &e) {
        logger.error() << e.what();
        if (sendErr) pkt.write(e.what());
    }
}

inline void c2sHostTypeServer(protocol::ServerParams &p)
{
    protocol::runHostTypeServer(p, storageHT);
}

namespace storage_local {

inline void readLogPackHeader(device::AsyncWldevReader &reader, log::PackHeaderRaw &packH, uint64_t lsid, const char *msg)
{
    reader.read((char *)packH.rawData(), 1);
    if (packH.header().logpack_lsid != lsid) {
        throw cybozu::Exception(msg)
            << "logpack lsid invalid" << packH.header().logpack_lsid << lsid;
    }
    reader.readAhead();
}

inline void readLogIo(device::AsyncWldevReader &reader, log::PackHeaderRaw &packH, size_t recIdx, log::BlockDataShared &blockD)
{
    const log::RecordWrap lrec(&packH, recIdx);
    if (!lrec.hasData()) return;

    blockD.resize(lrec.ioSizePb());
    for (size_t i = 0; i < lrec.ioSizePb(); i++) {
        reader.read((char *)blockD.get(i), 1);
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

    std::shared_ptr<uint8_t> packHeaderBlock = cybozu::util::allocateBlocks<uint8_t>(pbs, pbs);
    log::PackHeaderRaw packH(packHeaderBlock, pbs, salt);
    reader.reset(lsidB);

    log::BlockDataShared blockD(pbs);
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
            readLogIo(reader, packH, i, blockD);
            sender.pushIo(packH, i, blockD);
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
    verifyStateIn(st, {sMaster, stFullSync, stHashSync, sSlave}, FUNC);

    if (st == sSlave) {
        StateMachineTransaction tran(volSt.sm, sSlave, stWlogRemove);
        ul.unlock();
        storage_local::deleteWlogs(volId);
        tran.commit(sSlave);
        return;
    }

    verifyNoActionRunning(volSt.ac, StrVec{sWlogSend}, FUNC);
    ActionCounterTransaction tran(volSt.ac, sWlogSend);
    ul.unlock();
    try {
        const bool isRemaining = storage_local::extractAndSendAndDeleteWlog(volId);
        if (isRemaining) getStorageGlobal().taskQueue.push(volId);
    } catch (...) {
        getStorageGlobal().taskQueue.pushForce(volId, gs.delaySecForRetry * 1000);
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
        protocol::run1stNegotiateAsClient(sock, gs.nodeId, hostTypePN);
        const std::string type = protocol::runHostTypeClient(sock);
        if (type == proxyHT) info.isAvailable = true;
    } catch (std::exception &e) {
        LOGs.warn() << FUNC << e.what();
    } catch (...) {
        LOGs.warn() << FUNC << "unknown error";
    }
    info.checkedTime = Clock::now();
    return info;
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
    StorageSingleton& g = getStorageGlobal();
    const int intervalMs = 1000;
    while (!g.quitProxyMonitor) {
        try {
            g.proxyManager.tryCheckAvailability();
            util::sleepMs(intervalMs);
        } catch (std::exception& e) {
            LOGs.error() << "proxyMonitorWorker" << e.what();
        } catch (...) {
            LOGs.error() << "proxyMonitorWorker:unknown error";
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

    try {
        const StrVec v = protocol::recvStrVec(p.sock, 0, FUNC);
        if (v.empty()) throw cybozu::Exception(FUNC) << "empty param";
        const std::string &volId = v[0];
        uint64_t gid = 0;
        if (v.size() >= 2) gid = cybozu::atoi(v[1]);

        StorageVolState &volSt = getStorageVolState(volId);
        UniqueLock ul(volSt.mu);

        verifyNotStopping(volSt.stopState, volId, FUNC);
        StateMachineTransaction tran(volSt.sm, sStopped, stReset);
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        volInfo.resetWlog(gid);
        tran.commit(sSyncReady);
        pkt.write(msgOk);
        logger.info() << "reset succeeded" << volId << gid;
    } catch (std::exception &e) {
        logger.error() << FUNC << e.what();
        pkt.write(e.what());
    }
}

} // walb
