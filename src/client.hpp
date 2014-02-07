#pragma once
#include "protocol.hpp"

namespace walb {

namespace client_local {

} // namespace client_local

/**
 * params.size() == 0 or 1.
 * params[0]: volId
 */
static inline void c2xGetStrVecClient(protocol::ClientParams &p)
{
    packet::Packet packet(p.sock);
    packet.write(p.params);

    std::string st;
    packet.read(st);
    if (st != "ok") {
        throw cybozu::Exception("c2xGetStrVecClient:not ok") << st;
    }

    std::vector<std::string> v;
    packet.read(v);
    for (const std::string &s : v) {
        ::printf("%s\n", s.c_str());
    }
}

/**
 * param[0]: volId
 * param[2]: wdevPath
 */
static inline void c2sInitVolClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, "c2sInitVolClient");
}

/**
 * param[0]: volId
 */
static inline void c2aInitVolClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 1, "c2aInitVolClient");
}

} // namespace walb
