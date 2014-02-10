#pragma once
#include "protocol.hpp"

namespace walb {

struct ProxySingleton
{
    static ProxySingleton& getInstance() {
        static ProxySingleton instance;
        return instance;
    }
    std::string nodeId;
    std::string baseDirStr;
};

static inline void c2pStatusServer(protocol::ServerParams &/*p*/)
{
    // now editing
}

} // walb
