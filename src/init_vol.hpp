#pragma once

namespace walb {

static inline void clientInitVol(cybozu::Socket& sock, ProtocolLogger& /*logger*/, 
    const std::atomic<bool> &/*forceQuit*/, const std::vector<std::string> &params)
{
    if (params.size() != 2 || params[0].empty() || params[1].empty()) {
        throw cybozu::Exception("clientInitVol:bad params size");
    }
    packet::Packet packet(sock);
    packet.write(params[0]);
    packet.write(params[1]);

    packet::Ack ack(sock);
    ack.recv();
}

static inline void storageInitVol(cybozu::Socket &sock,
    ProtocolLogger& /*logger*/,
    const std::string &baseDirStr,
    const std::atomic<bool> &/*forceQuit*/,
    std::atomic<cybozu::server::ControlFlag> &/*ctrlFlag*/)
{
    packet::Packet packet(sock);
    std::string volId, wdevPathName;
    packet.read(volId);
    packet.read(wdevPathName);

    cybozu::FilePath basePath(baseDirStr);
    cybozu::FilePath wdevPath(wdevPathName);
    basePath += volId;
    if (basePath.stat().exists()) {
        throw cybozu::Exception("storageInitVol:already exists") << basePath.str();
    }
    if (!wdevPath.stat().exists()) {
        throw cybozu::Exception("storageInitVol:not found") << wdevPathName;
    }
    if (!basePath.mkdir()) {
        throw cybozu::Exception("storageInitVol:can't make directory") << basePath.str();
    }
    // QQQ : make path, queue, done, state files
}

} // walb


