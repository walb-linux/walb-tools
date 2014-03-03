#pragma once
#include "walb_util.hpp"
#include "protocol.hpp"
#include "state_machine.hpp"
#include "state_map.hpp"
#include "action_counter.hpp"
#include "proxy_vol_info.hpp"
#include "wdiff_data.hpp"
#include "host_info.hpp"

namespace walb {

struct ProxyVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // archive name is action identifier here.
    MetaDiffManager diffMgr; // for the master.
    StateMap<MetaDiffManager> diffMgrMap; // for each archive.

    /**
     * This is protected by state machine.
     * In Started/WlogRecv state, this is read-only and accessed by multiple threads.
     * Otherwise, this is writable and accessed by only one thread.
     */
    std::vector<HostInfo> hostInfoV;

    explicit ProxyVolState(const std::string &volId)
        : stopState(NotStopping), sm(mu), ac(mu)
        , diffMgr(), diffMgrMap(), hostInfoV() {
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

struct ProxySingleton
{
    static ProxySingleton& getInstance() {
        static ProxySingleton instance;
        return instance;
    }
    std::string nodeId;
    std::string baseDirStr;

    StateMap<ProxyVolState> stMap;
};

inline ProxySingleton& getProxyGlobal()
{
    return ProxySingleton::getInstance();
}

const ProxySingleton& gp = getProxyGlobal();

/**
 * This is called just one time.
 */
inline void ProxyVolState::initInner(const std::string &volId)
{
    cybozu::FilePath volDir(gp.baseDirStr);
    volDir += volId;

    // Load host info if exist.
    // Load wdiff meta data for master and each archive directory.

    // QQQ
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
inline void c2pArchiveInfoServer(protocol::ServerParams &/*p*/)
{
    // QQQ
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
