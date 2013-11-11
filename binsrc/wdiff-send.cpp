/**
 * @file
 * @brief To send wdiff to a server.
 * @author MITSUNARI Shigeo
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <stdexcept>
#include <cstdio>
#include <time.h>
#include "cybozu/option.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/atoi.hpp"
#include "cybozu/log.hpp"
#include "protocol.hpp"
#include "meta.hpp"
#include "file_path.hpp"
#include "time.hpp"
#include "net_util.hpp"
#include "meta.hpp"
#include "walb_log_file.hpp"
#include "walb_log_net.hpp"
#include "walb_diff_merge.hpp"

struct Option : cybozu::Option
{
    std::string serverHostPort;
    std::string name;
    uint64_t gid;
    std::vector<std::string> wdiffPathV;
    std::string clientId;
    bool canNotMerge;
    std::string timeStampStr;

    Option() {
        appendMust(&serverHostPort, "server", "server host:port");
        appendMust(&name, "name", "volume identifier");
        appendOpt(&gid, 0, "gid", "begin gid.");
        appendParamVec(&wdiffPathV, "wdiff_path_list", "wdiff path list");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&clientId, hostName, "id", "client identifier");
        appendBoolOpt(&canNotMerge, "m", "clear canMerge flag.");
        appendOpt(&timeStampStr, "", "t", "timestamp in YYYYmmddHHMMSS format.");
        appendHelp("h");
    }
};

void sendWdiff(cybozu::Socket &sock, const std::string &clientId,
              const std::string &name, walb::diff::Merger& merger, walb::MetaDiff &diff)
{
    const std::string diffFileName = createDiffFileName(diff);
    LOGi("try to send %s...", diffFileName.c_str());

    std::string serverId = walb::protocol::run1stNegotiateAsClient(
        sock, clientId, "wdiff-send");
    walb::ProtocolLogger logger(clientId, serverId);

    const walb::diff::FileHeaderWrap& fileH = merger.header();

    /* wdiff-send negotiation */
    walb::packet::Packet packet(sock);
    packet.write(name);
    {
        cybozu::Uuid uuid;
        uuid.set(fileH.getUuid());
        packet.write(uuid);
    }
    packet.write(fileH.getMaxIoBlocks());
    packet.write(diff);
    {
        walb::packet::Answer ans(sock);
        int err; std::string msg;
        if (!ans.recv(&err, &msg)) {
            logger.error("negotiation failed: %d %s", err, msg.c_str());
            throw cybozu::Exception("sendWdiff:negotiation") << err << msg;
        }
    }

    /* Send diff packs. */

    walb::packet::StreamControl ctrl(sock);
    walb::diff::RecIo recIo;
    while (merger.pop(recIo)) {
        ctrl.next();
        const walb::diff::RecordRaw& recRaw = recIo.record();
        const walb_diff_record& rec = recRaw.record();
        sock.write(&rec, sizeof(rec));
        if (recRaw.dataSize() > 0) {
            const walb::diff::IoData& io = recIo.io();
            sock.write(io.rawData(), recRaw.dataSize());
        }
    }
    ctrl.end();
    walb::packet::Ack ack(sock);
    ack.recv();

    /* The wdiff-send protocol has finished.
       You can close the socket. */
}

int main(int argc, char *argv[])
try {
    cybozu::SetLogFILE(::stderr);

    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        throw std::runtime_error("option error.");
    }
    std::string host;
    uint16_t port;
    std::tie(host, port) = cybozu::net::parseHostPortStr(opt.serverHostPort);

    uint64_t ts = ::time(0);
    if (!opt.timeStampStr.empty()) {
        ts = cybozu::strToUnixTime(opt.timeStampStr);
    }

    const uint64_t gid = opt.gid;
    walb::diff::Merger merger;
    merger.addWdiffs(opt.wdiffPathV);
    merger.prepare();
    walb::MetaDiff diff;
    diff.init();
    diff.setSnap0(gid);
    diff.setSnap1(gid + 1);
    diff.setTimestamp(ts);
    diff.setCanMerge(!opt.canNotMerge);
    cybozu::Socket sock;
    sock.connect(host, port);
    sendWdiff(sock, opt.clientId, opt.name, merger, diff);
} catch (std::exception &e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "caught an other error.\n");
    return 1;
}
