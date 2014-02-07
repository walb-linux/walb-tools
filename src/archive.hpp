#pragma once
#include "protocol.hpp"
#include "archive_vol_info.hpp"

namespace walb {

namespace archive_local {

} // namespace archive_local

static inline void c2aStatusServer(protocol::ServerParams &/*p*/)
{
    // now editing
}

/**
 *
 */
static inline void c2aInitVolServer(protocol::ServerParams &p)
{
    const std::vector<std::string> v =
        protocol::recvStrVec(p.sock, 1, "c2aInitVolServer", false);
    const std::string &volId = v[0];

    ArchiveVolInfo volInfo(p.baseDirStr, volId);
    volInfo.init();

    packet::Ack(p.sock).send();

    // QQQ


    // now editing
}

} // walb
