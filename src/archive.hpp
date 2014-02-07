#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"

namespace walb {

namespace archive_local {
} // namespace archive_local

static inline void c2aStatusServer(protocol::ServerParams &/*p*/)
{
    // QQQ
}

static inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, "c2aInitVolServer", false);
    const std::string &volId = v[0];

    ArchiveVolInfo volInfo(p.baseDirStr, volId);
    volInfo.init();

    packet::Ack(p.sock).send();
}

/**
 * Execute dirty full sync protocol as server.
 * Client is storage server or another archive server.
 */
static inline void x2aDirtyFullSyncServer(protocol::ServerParams &p)
{
}

} // walb
