#pragma once
#include "protocol.hpp"

namespace walb {

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

static inline void c2sInitVolClient(protocol::ClientParams &p)
{
    if (p.params.size() != 2 || p.params[0].empty() || p.params[1].empty()) {
        throw cybozu::Exception("c2sInitVolClient:bad params size");
    }
    packet::Packet packet(p.sock);
    packet.write(p.params[0]);
    packet.write(p.params[1]);

    packet::Ack ack(p.sock);
    ack.recv();
}

static inline void c2aInitVolClient(protocol::ClientParams &/*p*/)
{
    // parameters check
}

} // namespace walb
