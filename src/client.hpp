#pragma once
#include "protocol.hpp"

namespace walb {

namespace client_local {

static inline void sendStrVec(
    cybozu::Socket &sock,
    const std::vector<std::string> &v, size_t numToSend, const char *msg)
{
    if (v.size() != numToSend) {
        throw cybozu::Exception(msg) << "bad size" << numToSend << v.size();
    }
    packet::Packet packet(sock);
    for (size_t i = 0; i < numToSend; i++) {
        if (v[i].empty()) {
            throw cybozu::Exception(msg) << "empty string" << i;
        }
        packet.write(v[i]);
    }

    packet::Ack ack(sock);
    ack.recv();
}

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
    client_local::sendStrVec(p.sock, p.params, 2, "c2sInitVolClient");
}

/**
 * param[0]: volId
 */
static inline void c2aInitVolClient(protocol::ClientParams &p)
{
    client_local::sendStrVec(p.sock, p.params, 1, "c2aInitVolClient");
}

} // namespace walb
