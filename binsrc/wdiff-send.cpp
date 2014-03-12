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
#include "cybozu/thread.hpp"
#include "meta.hpp"
#include "file_path.hpp"
#include "time.hpp"
#include "net_util.hpp"
#include "meta.hpp"
#include "walb_log_file.hpp"
#include "walb_log_net.hpp"
#include "walb_diff_merge.hpp"
#include "walb_diff_compressor.hpp"
#include "protocol.hpp"
#include "uuid.hpp"

struct Option : cybozu::Option
{
    std::string serverHostPort;
    std::string name;
    uint64_t gid;
    std::vector<std::string> wdiffPathV;
    std::string clientId;
    bool canNotMerge;
    std::string timeStampStr;
    size_t threadNum;
    int compType;

    Option(int argc, const char *const argv[]) {
        std::string compStr;
        appendMust(&serverHostPort, "server", "server host:port");
        appendMust(&name, "name", "volume identifier");
        appendOpt(&gid, 0, "gid", "begin gid.");
        appendParamVec(&wdiffPathV, "wdiff_path_list", "wdiff path list");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&clientId, hostName, "id", "client identifier");
        appendBoolOpt(&canNotMerge, "m", "clear canMerge flag.");
        appendOpt(&timeStampStr, "", "ts", "timestamp in YYYYmmddHHMMSS format.");
        appendOpt(&threadNum, cybozu::GetProcessorNum(), "tn", "number of thread");
        appendOpt(&compStr, "snappy", "c", "compression type(none, gzip, snappy(default), lzma)");
        appendHelp("h");
        if (!parse(argc, argv)) {
            usage();
            exit(1);
        }
        if (threadNum == 0) {
            throw cybozu::Exception("bad number of thread");
        }
        const struct {
            const char *name;
            int type;
        } tbl[] = {
            { "none", WALB_DIFF_CMPR_NONE },
            { "gzip", WALB_DIFF_CMPR_GZIP },
            { "snappy", WALB_DIFF_CMPR_SNAPPY },
            { "lzma", WALB_DIFF_CMPR_LZMA },
        };
        for (const auto& e : tbl) {
            if (compStr == e.name) {
                compType = e.type;
                return;
            }
        }
        throw cybozu::Exception("Option:bad c option") << compStr;
    }
};

void sendWdiff(cybozu::Socket &sock,
              walb::diff::Merger& merger, walb::MetaDiff &diff, const Option& opt)
{
    const std::string diffFileName = createDiffFileName(diff);
    LOGi("try to send %s...", diffFileName.c_str());

    std::string serverId = walb::protocol::run1stNegotiateAsClient(
        sock, opt.clientId, "wdiff-send");
    walb::ProtocolLogger logger(opt.clientId, serverId);

    const walb::diff::FileHeaderWrap& fileH = merger.header();

    /* wdiff-send negotiation */
    walb::packet::Packet packet(sock);
    packet.write(opt.name);
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
    const size_t maxPushedNum = opt.threadNum * 2 - 1;
    walb::ConverterQueue conv(maxPushedNum, opt.threadNum, true, opt.compType);
    walb::diff::Packer packer;
    size_t pushedNum = 0;
    while (merger.pop(recIo)) {
        const walb_diff_record& rec = recIo.record();
        const walb::diff::IoData& io = recIo.io();
        if (packer.add(rec, io.data.data())) {
            continue;
        }
        conv.push(packer.getPackAsUniquePtr());
        pushedNum++;
        packer.reset();
        packer.add(rec, io.data.data());
        if (pushedNum < maxPushedNum) {
            continue;
        }
        std::unique_ptr<char[]> p = conv.pop();
        ctrl.next();
        sock.write(p.get(), walb::diff::PackHeader(p.get()).wholePackSize());
        pushedNum--;
    }
    if (!packer.empty()) {
        conv.push(packer.getPackAsUniquePtr());
    }
    conv.quit();
    while (std::unique_ptr<char[]> p = conv.pop()) {
        ctrl.next();
        sock.write(p.get(), walb::diff::PackHeader(p.get()).wholePackSize());
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

    Option opt(argc, argv);
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
    diff.snapB.gidB = gid;
    diff.snapB.gidE = gid;
    diff.snapE.gidB = gid + 1;
    diff.snapE.gidE = gid + 1;
    diff.timestamp = ts;
    diff.canMerge = !opt.canNotMerge;
    cybozu::Socket sock;
    sock.connect(host, port);
    sendWdiff(sock, merger, diff, opt);
} catch (std::exception &e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "caught an other error.\n");
    return 1;
}
