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
    std::vector<std::string> archiveList;

    explicit ProxyVolState(const std::string &volId)
        : stopState(NotStopping), sm(mu), ac(mu)
        , diffMgr(), diffMgrMap(), archiveList() {
        const struct StateMachine::Pair tbl[] = {
            { pClear, ptAddArchiveInfo },
            { ptAddArchiveInfo, pStopped },

            { pStopped, ptClearVol },
            { ptClearVol, pClear },

            { pStopped, ptAddArchiveInfo },
            { ptAddArchiveInfo, pStopped },

            { pStopped, ptUpdateArchiveInfo },
            { ptUpdateArchiveInfo, pStopped },

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
    std::string archiveId;

    bool operator==(const ProxyTask &rhs) const {
        return volId == rhs.volId && archiveId == rhs.archiveId;
    }
    bool operator<(const ProxyTask &rhs) const {
        int c = volId.compare(rhs.volId);
        if (c < 0) return true;
        if (c > 0) return false;
        return archiveId < rhs.archiveId;
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

    ProxyVolInfo volInfo(gp.baseDirStr, volId, diffMgr, diffMgrMap);
    if (!volInfo.existsVolDir()) {
        sm.set(pClear);
        return;
    }

    sm.set(volInfo.getState());
    volInfo.reloadMaster();
    // Load host info if exist.
    // Load wdiff meta data for master and each archive directory.
    for (const std::string &name : volInfo.getArchiveNameList()) {
        HostInfo hi = volInfo.getArchiveInfo(name);
        hi.verify();
        archiveList.push_back(name);
        volInfo.reloadSlave(name);
    }
    // Retry to make hard links of wdiff files in the master directory.
    std::vector<MetaDiff> diffV = volInfo.getAllDiffsInMaster();
    for (const MetaDiff &d : diffV) {
        for (const std::string &name : archiveList) {
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

inline void verifyNotStopping(const std::string &volId, const char *msg)
{
    util::verifyNotStopping(getProxyVolState(volId).stopState, volId, msg);
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
inline void c2pStartServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

/**
 * params:
 *   [0]: volId
 *
 * State transition: Started --> Stop --> Stopped.
 * In addition, this will stop all background tasks before changing state.
 */
inline void c2pStopServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

namespace proxy_local {

inline void getArchiveInfo(const std::string &/*archiveId*/, HostInfo &/*hi*/)
{
    // QQQ
}

inline void addArchiveInfo(const std::string &/*archiveId*/, const HostInfo &/*hi*/)
{
    // QQQ
}

inline void deleteArchiveInfo(const std::string &/*archiveId*/)
{
    // QQQ
}

inline void updateArchiveInfo(const std::string &/*archiveId*/, const HostInfo &/*hi*/)
{
    // QQQ
}

template <typename Func>
inline void runAndReplyOkOrErr(packet::Packet &pkt, const char *msg, Func func)
{
    bool failed = false;
    std::string errMsg;
    try {
        func();
    } catch (std::exception &e) {
        failed = true;
        errMsg = e.what();
    }
    if (failed) {
        pkt.write(errMsg);
        throw cybozu::Exception(msg) << errMsg;
    }
    pkt.write("ok");
}

} // namespace proxy_local

/**
 * params:
 *   [0]: volId
 *   [1]: add/delete/update as string
 *   [2]: serialized HostInfo data. (add/update only)
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
    StrVec v = protocol::recvStrVec(p.sock, 2, FUNC, false);
    const std::string &cmd = v[0];
    const std::string &archiveId = v[1];

    if (cmd == "add" || cmd == "update") {
        packet::Packet pkt(p.sock);
        HostInfo hi;
        pkt.read(hi);
        hi.verify();
        if (cmd == "add") {
            proxy_local::runAndReplyOkOrErr(
                pkt, FUNC,
                [&]() { proxy_local::addArchiveInfo(archiveId, hi); });
        } else {
            proxy_local::runAndReplyOkOrErr(
                pkt, FUNC,
                [&]() { proxy_local::updateArchiveInfo(archiveId, hi); });
        }
        return;
    }
    if (cmd == "get" || cmd == "delete") {
        packet::Packet pkt(p.sock);
        HostInfo hi;
        if (cmd == "get") {
            proxy_local::runAndReplyOkOrErr(
                pkt, FUNC,
                [&]() { proxy_local::getArchiveInfo(archiveId, hi); });
            pkt.write(hi);
        } else {
            proxy_local::runAndReplyOkOrErr(
                pkt, FUNC,
                [&]() { proxy_local::deleteArchiveInfo(archiveId); });
        }
        return;
    }
    throw cybozu::Exception(FUNC) << "invalid command name" << cmd;
}

/**
 * params:
 *   [0]: volId
 *
 * State transition: stopped --> ClearVol --> clear.
 */
inline void c2pClearVolServer(protocol::ServerParams &/*p*/)
{
    // QQQ
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
