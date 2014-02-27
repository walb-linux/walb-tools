#pragma once
#include "protocol.hpp"
#include "constant.hpp"

namespace walb {

/**
 * params.size() == 0 or 1.
 * params[0]: volId
 */
inline void c2xGetStrVecClient(protocol::ClientParams &p)
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
 * For storage:
 *   params[0]: volId
 *   params[2]: wdevPath
 * For archive:
 *   params[0]: volId
 */
inline void c2xInitVolClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, "c2xInitVolClient");
}

/**
 * Server is storage or archive.
 * params[0]: volId
 */
inline void c2xClearVolClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 1, "c2xClearVolClient");
}

/**
 * For storage:
 *   params[0]: volId
 *   params[1]: master
 * For archive:
 *   params[0]: volId
 */
inline void c2xStartClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 0, "c2xStartClient");
}

/**
 * params[0]: volId
 * params[1]: isForce: "0" or "1".
 */
inline void c2xStopClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, "c2xStopClient");
}

inline void c2sFullSyncClient(protocol::ClientParams &p)
{
    if (p.params.size() != 1 && p.params.size() != 2) {
        throw cybozu::Exception("c2sFullSyncClient:bad size param") << p.params.size();
    }
    std::vector<std::string> v;
    if (p.params[0].empty()) {
        throw cybozu::Exception("c2sFullSyncClient:empty volId");
    }
    v.push_back(p.params[0]);
    uint64_t bulkLb = walb::DEFAULT_BULK_LB;
    if (p.params.size() == 2) {
        bulkLb = cybozu::atoi(p.params[1]);
        if (bulkLb == 0) throw cybozu::Exception("c2sFullSyncClient:zero bulkLb");
    }
    v.push_back(cybozu::itoa(bulkLb));
    LOGd("sendStrVec");
    protocol::sendStrVec(p.sock, v, 2, "c2sFullSyncClient", false);

    std::string st;
    packet::Packet packet(p.sock);
    packet.read(st);
    if (st != "ok") {
        throw cybozu::Exception("c2sFullSyncClient:not ok") << st;
    }
}

/**
 * Restore command.
 * parameters: volId, gid
 */
inline void c2aRestoreClient(protocol::ClientParams &p)
{
    protocol::sendStrVec(p.sock, p.params, 2, "c2aRestoreClient", false);
    packet::Packet pkt(p.sock);

    std::string s;
    pkt.read(s);
    if (s != "ok") {
        throw cybozu::Exception("c2aRestoreClient:failed") << s;
    }
}

/**
 * pattern (1)
 *   get <archiveId>
 * pattern (2)
 *   add <archiveId> <addr>:<port> <cmprType>:<cmprLevel>
 * pattern (3)
 *   update <archiveId> <addr>:<port> <cmprType>:<cmprLevel>
 * pattern (4)
 *   delete <archiveId>
 *
 * <cmprType>: compression type. none, snappy, gzip, or lzma.
 * <cmprLevel>: compression level. integer from 0 to 9.
 */
inline void c2pArchiveInfoClient(protocol::ClientParams &/*p*/)
{
    // QQQ
}

/**
 *
 */
inline void c2sSnapshotClient(protocol::ClientParams &/*p*/)
{

    // QQQ
}

} // namespace walb
