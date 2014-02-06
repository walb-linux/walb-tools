#pragma once

namespace walb {

static inline void clientArchiveStatus(
    cybozu::Socket &/*sock*/, ProtocolLogger &/*logger*/,
    const std::atomic<bool> &/*forceQuit*/, const std::vector<std::string> &/*params*/)
{
    // now editing
}

static inline void archiveStatus(
    cybozu::Socket &/*sock*/,
    ProtocolLogger &/*logger*/,
    const std::string &/*baseDirStr*/,
    const std::atomic<bool> &/*forceQuit*/,
    std::atomic<walb::server::ProcessStatus> &/*procStat*/)
{
    // now editing
}

} // walb
