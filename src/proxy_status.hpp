#pragma once

namespace walb {

static inline void clientProxyStatus(
    cybozu::Socket &/*sock*/, ProtocolLogger &/*logger*/,
    const std::atomic<bool> &/*forceQuit*/, const std::vector<std::string> &/*params*/)
{
    // now editing
}

static inline void proxyStatus(
    cybozu::Socket &/*sock*/,
    ProtocolLogger &/*logger*/,
    const std::string &/*baseDirStr*/,
    const std::atomic<bool> &/*forceQuit*/,
    std::atomic<walb::server::ControlFlag> &/*ctrlFlag*/)
{
    // now editing
}

} // walb
