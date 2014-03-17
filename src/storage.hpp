#pragma once
#include "protocol.hpp"
#include "storage_vol_info.hpp"
#include "atomic_map.hpp"
#include "state_machine.hpp"
#include "constant.hpp"
#include <snappy.h>
#include "log_dev_monitor.hpp"

namespace walb {

inline std::string getWdevNameFromWdevPath(const std::string& wdevPath)
{
    // wdevPath = "/dev/walb/" + wdevName
    const std::string prefix = "/dev/walb/";
    if (wdevPath.find(prefix) != 0) throw cybozu::Exception("getWdevNameFromWdevPath:bad name") << wdevPath;
    return wdevPath.substr(prefix.size());
}

struct StorageVolState {
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;

    explicit StorageVolState(const std::string& volId) : stopState(NotStopping), sm(mu) {
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

            { sMaster, stWlogSend },
            { stWlogSend, sMaster },
        };
        sm.init(tbl);

        initInner(volId);
    }
private:
    void initInner(const std::string& volId);
};

class StorageWorker : public cybozu::thread::Runnable
{
public:
    const std::string volId;
    explicit StorageWorker(const std::string &volId) : volId(volId) {
    }
    void operator()();
    // QQQ
};

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

    /**
     * Writable and must be thread-safe.
     */
    std::atomic<bool> forceQuit;
    walb::AtomicMap<StorageVolState> stMap;
    TaskQueue<std::string> taskQueue;
    std::unique_ptr<DispatchTask<std::string, StorageWorker>> dispatcher;
    std::unique_ptr<std::thread> wdevMonitor;
    std::atomic<bool> quitMonitor;
    LogDevMonitor logDevMonitor;

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
    getStorageGlobal().addWdevName(getWdevNameFromWdevPath(wdevPath), volId);
}
inline void stopMonitoring(const std::string& wdevPath)
{
    getStorageGlobal().delWdevName(getWdevNameFromWdevPath(wdevPath));
}
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

inline void c2sStatusServer(protocol::ServerParams &p)
{
    packet::Packet packet(p.sock);
    std::vector<std::string> params;
    packet.read(params);

    if (params.empty()) {
        // for all volumes
        packet.write("not implemented yet");
        // TODO
    } else {
        // for a volume
        const std::string &volId = params[0];
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        if (!volInfo.existsVolDir()) {
            cybozu::Exception e("c2sStatusServer:no such volume");
            packet.write(e.what());
            throw e;
        }
        packet.write("ok");

        StrVec statusStrV;
        // TODO: show the memory state of the volume.
        for (std::string &s : volInfo.getStatusAsStrVec()) {
            statusStrV.push_back(std::move(s));
        }
        packet.write(statusStrV);
    }
}

inline void c2sListVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = util::getDirNameList(gs.baseDirStr);
    protocol::sendStrVec(p.sock, v, 0, FUNC, false);
    ProtocolLogger logger(gs.nodeId, p.clientId);
    logger.debug() << FUNC << "succeeded";
}

inline void c2sInitVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &volId = v[0];
    const std::string &wdevPathName = v[1];

    StateMachine &sm = getStorageVolState(volId).sm;
    {
        StateMachineTransaction tran(sm, sClear, stInitVol, FUNC);
        StorageVolInfo volInfo(gs.baseDirStr, volId, wdevPathName);
        volInfo.init();
        tran.commit(sSyncReady);
    }
    packet::Ack(p.sock).send();

    ProtocolLogger logger(gs.nodeId, p.clientId);
    logger.info() << FUNC << "initialize"
                  << "volId" << volId
                  << "wdev" << wdevPathName;
}

inline void c2sClearVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];

    StateMachine &sm = getStorageVolState(volId).sm;
    {
        StateMachineTransaction tran(sm, sSyncReady, stClearVol, FUNC);
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        const std::string wdevPathName = volInfo.getWdevPath();
        volInfo.clear();
        tran.commit(sClear);
    }

    packet::Ack(p.sock).send();

    ProtocolLogger logger(gs.nodeId, p.clientId);
    logger.info() << FUNC << "cleared volId" << volId;
}

/**
 * params[0]: volId
 * params[1]: "master" or "slave".
 */
inline void c2sStartServer(protocol::ServerParams &p)
{
    const StrVec v = protocol::recvStrVec(p.sock, 2, "c2sStartServer", false);
    const std::string &volId = v[0];
    const bool isMaster = (v[1] == "master");

    StateMachine &sm = getStorageVolState(volId).sm;
    if (isMaster) {
        StateMachineTransaction tran(sm, sStopped, stStartMaster, "c2sStartServer");
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        volInfo.setState(sMaster);
        storage_local::startMonitoring(volInfo.getWdevPath(), volId);
        tran.commit(sMaster);
    } else {
        StateMachineTransaction tran(sm, sSyncReady, stStartSlave, "c2sStartServer");
        StorageVolInfo volInfo(gs.baseDirStr, volId);
        volInfo.setState(sSlave);
        storage_local::startMonitoring(volInfo.getWdevPath(), volId);
        tran.commit(sSlave);
    }
    packet::Ack(p.sock).send();
}

/**
 * params[0]: volId
 * params[1]: isForce: "0" or "1".
 */
inline void c2sStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &volId = v[0];
    const int isForceInt = cybozu::atoi(v[1]);
    const bool isForce = (isForceInt != 0);

    StorageVolState &volSt = getStorageVolState(volId);
    packet::Ack(p.sock).send();

    Stopper stopper(volSt.stopState, isForce);
    if (!stopper.isSuccess()) {
        return;
    }

    UniqueLock ul(volSt.mu);
    StateMachine &sm = volSt.sm;

    waitUntil(ul, [&]() {
            return isStateIn(sm.get(), {stFullSync, stHashSync, stWlogSend, stWlogRemove});
        }, FUNC);

    const std::string st = sm.get();
    if (!isStateIn(st, {sMaster, sSlave})) {
        /* For SyncReady state (after FullSync and HashSync canceled),
           there is nothing to do. */
        return;
    }

    StorageVolInfo volInfo(gs.baseDirStr, volId);
    if (st == sMaster) {
        StateMachineTransaction tran(sm, sMaster, stStopMaster, FUNC);
        ul.unlock();

        storage_local::stopMonitoring(volInfo.getWdevPath());

        volInfo.setState(sStopped);
        tran.commit(sStopped);
    } else {
        assert(st == sSlave);
        StateMachineTransaction tran(sm, sSlave, stStopSlave, FUNC);
        ul.unlock();

        storage_local::stopMonitoring(volInfo.getWdevPath());

        volInfo.setState(sSyncReady);
        tran.commit(sSyncReady);
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

/**
 * @isMergeable
 *   true, the snapshot will be removed.
 * RETURN:
 *   Gid of the taken snapshot.
 */
inline uint64_t takeSnapshot(const std::string &/*volId*/, bool /*isMergeable*/)
{
    // QQQ
    return -1;
}

} // storage_local

inline void c2sFullSyncServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    ProtocolLogger logger(gs.nodeId, p.clientId);

    const StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string& volId = v[0];
    const uint64_t bulkLb = cybozu::atoi(v[1]);
    const uint64_t curTime = ::time(0);
    logger.debug() << volId << bulkLb << curTime;
    std::string archiveId;

    StorageVolInfo volInfo(gs.baseDirStr, volId);
    packet::Packet cPack(p.sock);

    StorageVolState &volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    StateMachine &sm = volSt.sm;

    StateMachineTransaction tran0(sm, sSyncReady, stFullSync, FUNC);
    ul.unlock();

    volInfo.resetWlog(0);

    const uint64_t sizeLb = getSizeLb(volInfo.getWdevPath());
    const cybozu::Uuid uuid = volInfo.getUuid();
    logger.debug() << sizeLb << uuid;

    // ToDo : start master((3) at full-sync as client in storage-daemon.txt)

    const cybozu::SocketAddr& archive = gs.archive;
    {
        cybozu::Socket aSock;
        aSock.connect(archive);
        archiveId = walb::protocol::run1stNegotiateAsClient(aSock, gs.nodeId, "dirty-full-sync");
        walb::packet::Packet aPack(aSock);
        aPack.write(storageHT);
        aPack.write(volId);
        aPack.write(uuid);
        aPack.write(sizeLb);
        aPack.write(curTime);
        aPack.write(bulkLb);
        {
            std::string res;
            aPack.read(res);
            if (res == "ok") {
                cPack.write("ok");
                p.sock.close();
            } else {
                cybozu::Exception e(FUNC);
                e << "bad response" << archiveId << res;
                cPack.write(e.what());
                throw e;
            }
        }
        // (7) in storage-daemon.txt
        if (!storage_local::sendDirtyFullImage(aPack, volInfo, sizeLb, bulkLb, volSt.stopState)) {
            logger.warn() << FUNC << "force stopped" << volId;
            // TODO: stop monitoring.
            return;
        }
        // (8), (9) in storage-daemon.txt
        {
            // TODO take a snapshot
            uint64_t gidB = 0, gidE = 1;
            aPack.write(gidB);
            aPack.write(gidE);
        }
        walb::packet::Ack(aSock).recv();
    }
    tran0.commit(sStopped);

    StateMachineTransaction tran1(sm, sStopped, stStartMaster, FUNC);
    volInfo.setState(sMaster);
    tran1.commit(sMaster);

    // TODO: If thrown an error, someone must stop monitoring task.

    logger.info() << FUNC << "done" << "archive" << archiveId;
}

/**
 * Take a snapshot to restore in archive hosts.
 */
inline void c2sSnapshotServer(protocol::ServerParams &/*p*/)
{
    // QQQ
    // storage_local::takeSnapshot();
}

namespace storage_local {

inline uint64_t extractAndSendWlog(const std::string &/*volId*/)
{
    // QQQ
    return -1;
}

/**
 * Delete all wlogs which lsid is less than a specifeid lsid.
 * Given INVALID_LSID, all existing wlogs will be deleted.
 *
 * RETURN:
 *   true if all the wlogs have been deleted.
 */
inline bool deleteWlogs(const std::string &/*volId*/, uint64_t /*lsid*/ = INVALID_LSID)
{
    // QQQ
    return false;
}

} // storage_local

inline void StorageWorker::operator()()
{
    const char *const FUNC = "StorageWorker::operator()";
    StorageVolState& volSt = getStorageVolState(volId);
    UniqueLock ul(volSt.mu);
    verifyNotStopping(volSt.stopState, volId, FUNC);
    const std::string st = volSt.sm.get();
    verifyStateIn(st, {sMaster, sSlave}, FUNC);

    if (st == sMaster) {
        StateMachineTransaction tran(volSt.sm, sMaster, stWlogSend);
        ul.unlock();
        uint64_t lsid = storage_local::extractAndSendWlog(volId);
        const bool isEmpty = storage_local::deleteWlogs(volId, lsid);
        if (!isEmpty) getStorageGlobal().taskQueue.push(volId);
        tran.commit(sMaster);
    } else { // sSlave
        StateMachineTransaction tran(volSt.sm, sSlave, stWlogRemove);
        ul.unlock();
        storage_local::deleteWlogs(volId);
        tran.commit(sSlave);
    }
    // QQQ
}

inline void wdevMonitorWorker() noexcept
{
    StorageSingleton& g = getStorageGlobal();
    const int timeoutMs = 1000;
    while (!g.quitMonitor) {
        try {
            const StrVec v = g.logDevMonitor.poll(timeoutMs);
            if (v.empty()) continue;
            for (const std::string& wdevName : v) {
                const std::string volId = g.getVolIdFromWdevName(wdevName);
                g.taskQueue.push(volId);
            }
        } catch (std::exception& e) {
            LOGs.error() << "wdevMonitorWorker" << e.what();
        } catch (...) {
            LOGs.error() << "wdevMonitorWorker:unknown error";
        }
    }
}

} // walb
