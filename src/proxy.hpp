#pragma once
#include "walb_util.hpp"
#include "protocol.hpp"
#include "state_machine.hpp"
#include "atomic_map.hpp"
#include "action_counter.hpp"
#include "proxy_vol_info.hpp"
#include "wdiff_data.hpp"
#include "host_info.hpp"
#include "task_queue.hpp"
#include "walb_util.hpp"

namespace walb {

struct ProxyVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // archive name is action identifier here.

    MetaDiffManager diffMgr; // for the master.
    AtomicMap<MetaDiffManager> diffMgrMap; // for each archive.

    /**
     * This is protected by state machine.
     * In Started/WlogRecv state, this is read-only and accessed by multiple threads.
     * Otherwise, this is writable and accessed by only one thread.
     */
    std::set<std::string> archiveSet;

    explicit ProxyVolState(const std::string &volId)
        : stopState(NotStopping), sm(mu), ac(mu)
        , diffMgr(), diffMgrMap(), archiveSet() {
        const struct StateMachine::Pair tbl[] = {
            { pClear, ptAddArchiveInfo },
            { ptAddArchiveInfo, pStopped },

            { pStopped, ptClearVol },
            { ptClearVol, pClear },

            { pStopped, ptAddArchiveInfo },
            { ptAddArchiveInfo, pStopped },

            { pStopped, ptDeleteArchiveInfo },
            { ptDeleteArchiveInfo, pStopped },

            { pStopped, ptDeleteArchiveInfo },
            { ptDeleteArchiveInfo, pClear },

            { pStopped, ptStart },
            { ptStart, pStarted },

            { pStarted, ptStop },
            { ptStop, pStopped },

            { pStarted, ptWlogRecv },
            { ptWlogRecv, pStarted },
        };
        sm.init(tbl);
        initInner(volId);
    }
private:
    void initInner(const std::string &volId);
};

struct ProxyTask
{
    std::string volId;
    std::string archiveName;

    bool operator==(const ProxyTask &rhs) const {
        return volId == rhs.volId && archiveName == rhs.archiveName;
    }
    bool operator<(const ProxyTask &rhs) const {
        int c = volId.compare(rhs.volId);
        if (c < 0) return true;
        if (c > 0) return false;
        return archiveName < rhs.archiveName;
    }
};

class ProxyWorker : public cybozu::thread::Runnable
{
private:
    const ProxyTask task_;

public:
    ProxyWorker(const ProxyTask &task) : task_(task) {
    }
    /**
     * This will do wdiff send to an archive server.
     * You can throw an exception.
     */
    void operator()() override {
        // QQQ
    }
};

struct ProxySingleton
{
    static ProxySingleton& getInstance() {
        static ProxySingleton instance;
        return instance;
    }

    /**
     * Read-only except for daemon initialization.
     */
    std::string nodeId;
    std::string baseDirStr;

    /**
     * Writable and must be thread-safe.
     */
    std::atomic<bool> forceQuit;
    AtomicMap<ProxyVolState> stMap;
    TaskQueue<ProxyTask> taskQueue;
    std::unique_ptr<util::DispatchTask<ProxyTask, ProxyWorker> > dispatcher;
};

inline ProxySingleton& getProxyGlobal()
{
    return ProxySingleton::getInstance();
}

const ProxySingleton& gp = getProxyGlobal();

/**
 * This is called just one time and by one thread.
 * You need not take care about thread-safety inside this function.
 */
inline void ProxyVolState::initInner(const std::string &volId)
{
    cybozu::FilePath volDir(gp.baseDirStr);
    volDir += volId;

    ProxyVolInfo volInfo(gp.baseDirStr, volId, diffMgr, diffMgrMap, archiveSet);
    if (!volInfo.existsVolDir()) {
        sm.set(pClear);
        return;
    }

    sm.set(volInfo.getState());
    volInfo.loadAllArchiveInfo();

    // Retry to make hard links of wdiff files in the master directory.
    std::vector<MetaDiff> diffV = volInfo.getAllDiffsInMaster();
    for (const MetaDiff &d : diffV) {
        for (const std::string &name : archiveSet) {
            volInfo.tryToMakeHardlinkInSlave(d, name);
        }
    }
    volInfo.deleteDiffsFromMaster(diffV);
    // Here the master directory must contain no wdiff file.
    if (!diffMgr.getAll().empty()) {
        throw cybozu::Exception("ProxyVolState::initInner")
            << "there are wdiff files in the master directory";
    }
}

inline ProxyVolState &getProxyVolState(const std::string &volId)
{
    return getProxyGlobal().stMap.get(volId);
}

inline void c2pStatusServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

/**
 * params:
 *   [0]: volId
 *
 * State transition: Stopped --> Start --> Started.
 */
inline void c2pStartServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    packet::Packet pkt(p.sock);
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];
    packet::Ack(p.sock).send();

    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    util::verifyNotStopping(volSt.stopState, volId, FUNC);
    util::verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    {
        StateMachineTransaction tran(volSt.sm, pStopped, ptStart);
        // TODO: enqueue backgrond tasks for the volume.
        tran.commit(pStarted);
    }
}

/**
 * params:
 *   [0]: volId
 *   [1]: isForce
 *
 * State transition: Started --> Stop --> Stopped.
 * In addition, this will stop all background tasks before changing state.
 */
inline void c2pStopServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &volId = v[0];
    const bool isForce = static_cast<int>(cybozu::atoi(v[1])) != 0;

    ProxyVolState &volSt = getProxyVolState(volId);
    packet::Ack(p.sock).send();

    util::Stopper stopper(volSt.stopState, isForce);
    if (!stopper.isSuccess()) return;

    // TODO: clear all related tasks from the task queue.

    UniqueLock ul(volSt.mu);
    util::waitUntil(ul, [&]() {
            if (!volSt.ac.isAllZero(volSt.archiveSet)) return false;
            const std::string &st = volSt.sm.get();
            return st == pStopped || st == pStarted || st == pClear;
        }, FUNC);

    const std::string &st = volSt.sm.get();
    if (st != pStarted) return;

    StateMachineTransaction tran(volSt.sm, pStarted, ptStop, FUNC);
    ul.unlock();
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    const std::string fst = volInfo.getState();
    if (fst != pStarted) {
        throw cybozu::Exception(FUNC) << "not Started state" << fst;
    }
    volInfo.setState(pStopped);
    tran.commit(pStopped);
}

namespace proxy_local {

inline void getArchiveInfo(const std::string& volId, const std::string &archiveName, HostInfo &hi)
{
    const char *const FUNC = __func__;
    ProxyVolState& volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    util::verifyNotStopping(volSt.stopState, volId, FUNC);
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (!volInfo.existsArchiveInfo(archiveName)) {
        throw cybozu::Exception(FUNC) << "archive info not exists" << archiveName;
    }
    hi = volInfo.getArchiveInfo(archiveName);
}

inline void addArchiveInfo(const std::string &volId, const std::string &archiveName, const HostInfo &hi, bool ensureNotExistance)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    util::verifyNotStopping(volSt.stopState, volId, FUNC);
    util::verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    const std::string &curr = volSt.sm.get(); // pStopped or pClear

    StateMachineTransaction tran(volSt.sm, curr, ptAddArchiveInfo);
    ul.unlock();
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    if (curr == pClear) volInfo.init();
    volInfo.addArchiveInfo(archiveName, hi, ensureNotExistance);
    tran.commit(pStopped);
}

inline void deleteArchiveInfo(const std::string &volId, const std::string &archiveName)
{
    const char *const FUNC = __func__;
    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    util::verifyNotStopping(volSt.stopState, volId, FUNC);
    util::verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);

    StateMachineTransaction tran(volSt.sm, pStopped, ptDeleteArchiveInfo);
    ul.unlock();
    ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
    volInfo.deleteArchiveInfo(archiveName);
    ul.lock();
    bool shouldClear = volInfo.notExistsArchiveInfo();
    if (shouldClear) volInfo.clear();
    tran.commit(shouldClear ? pClear : pStopped);
}

} // namespace proxy_local

/**
 * params:
 *   [0]: volId
 *   [1]: add/delete/update as string
 *   [2]: archive name
 *   [3]: serialized HostInfo data. (add/update only)
 *
 * State transition.
 *   (1) Clear --> AddArchiveInfo --> Stopped
 *   (2) Stopped --> X --> Stopped
 *       X is AddArchiveInfo/DeleteArchiveInfo/UpdateArchiveInfo.
 *   (3) Stopped --> DeleteArchiveInfo --> Clear
 */
inline void c2pArchiveInfoServer(protocol::ServerParams &p)
{
    const char * const FUNC = "c2pArchiveInfoServer";
    StrVec v = protocol::recvStrVec(p.sock, 3, FUNC, false);
    const std::string &volId = v[0];
    const std::string &cmd = v[1];
    const std::string &archiveName = v[2];

    packet::Packet pkt(p.sock);
    try {
        HostInfo hi;
        if (cmd == "add" || cmd == "update") {
            pkt.read(hi);
            proxy_local::addArchiveInfo(volId, archiveName, hi, cmd == "add");
            pkt.write("ok");
            return;
        } else if (cmd == "get") {
            proxy_local::getArchiveInfo(volId, archiveName, hi);
            pkt.write("ok");
            pkt.write(hi);
            return;
        } else if (cmd == "delete") {
            proxy_local::deleteArchiveInfo(volId, archiveName);
            pkt.write("ok");
            return;
        }
    } catch (std::exception &e) {
        pkt.write(std::string(FUNC) + e.what());
        throw;
    }
    throw cybozu::Exception(FUNC) << "invalid command name" << cmd;
}

/**
 * params:
 *   [0]: volId
 *
 * State transition: stopped --> ClearVol --> clear.
 */
inline void c2pClearVolServer(protocol::ServerParams &p)
{
    const char *const FUNC = __func__;
    StrVec v = protocol::recvStrVec(p.sock, 1, FUNC, false);
    const std::string &volId = v[0];
    packet::Ack(p.sock).send();

    ProxyVolState &volSt = getProxyVolState(volId);
    UniqueLock ul(volSt.mu);
    util::verifyNotStopping(volSt.stopState, volId, FUNC);
    util::verifyNoActionRunning(volSt.ac, volSt.archiveSet, FUNC);
    {
        StateMachineTransaction tran(volSt.sm, pStopped, ptClearVol);
        volSt.archiveSet.clear();
        ul.unlock();
        ProxyVolInfo volInfo(gp.baseDirStr, volId, volSt.diffMgr, volSt.diffMgrMap, volSt.archiveSet);
        volInfo.clear();
        tran.commit(pClear);
    }
}

/**
 * params:
 *   [0]: volId
 *   [1]: uuid (cybozu::Uuid)
 *   [2]: diff (walb::MetaDiff)
 *   [3]: pbs (uint32_t)
 *   [4]: salt (uint32_t)
 *   [5]: sizeLb (uint64_t)
 *   [6]: lsidB (uint64_t)
 *   [7]: lsidE (uint64_t)
 *
 * State transition: Started --> WlogRecv --> Started
 */
inline void s2pWlogTransferServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

} // walb
