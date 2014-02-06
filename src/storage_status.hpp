#pragma once

namespace walb {

static inline void clientStorageStatus(
    cybozu::Socket &sock, ProtocolLogger &/*logger*/,
    const std::atomic<bool> &/*forceQuit*/, const std::vector<std::string> &params)
{
    packet::Packet packet(sock);
    packet.write(params);

    std::string st;
    packet.read(st);
    if (st != "ok") {
        throw cybozu::Exception("clientStorageStatus:bad status") << st;
    }

    std::vector<std::string> v;
    packet.read(v);
    for (const std::string &s : v) {
        ::printf("%s\n", s.c_str());
    }
}

static inline void storageStatus(
    cybozu::Socket &sock,
    ProtocolLogger &logger,
    const std::string &baseDirStr,
    const std::atomic<bool> &/*forceQuit*/,
    std::atomic<walb::server::ProcessStatus> &/*procStat*/)
{
    packet::Packet packet(sock);
    std::vector<std::string> params;
    packet.read(params);

    if (params.empty()) {
        // for all volumes
        packet.write("not implemented yet");
        // TODO
    } else {
        // for a volume
        const std::string &volId = params[0];
        try {
            StorageVolInfo volInfo(baseDirStr, volId);
            packet.write("ok");
            packet.write(volInfo.getStatusAsStrVec());
        } catch (std::exception &e) {
            packet.write(e);
            logger.error("storageStatus:failed %s", e.what());
        }
    }
}

} // walb
