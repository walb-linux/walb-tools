#pragma once
#include "walb_util.hpp"
#include "protocol.hpp"
#include "state_machine.hpp"
#include "state_map.hpp"
#include "action_counter.hpp"
#include "proxy_vol_info.hpp"

namespace walb {

struct ProxyVolState
{
    std::recursive_mutex mu;
    std::atomic<int> stopState;
    StateMachine sm;
    ActionCounters ac; // archive name is action identifier here.

    explicit ProxyVolState(const std::string &volId)
        : stopState(NotStopping), sm(mu), ac(mu) {
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

inline void ProxyVolState::initInner(const std::string &/*volId*/)
{
    // QQQ
}

inline void c2pStatusServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline void c2pStartServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline void c2pStopServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline void c2pArchiveInfoServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

inline void c2pClearVolServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

} // walb
