/**
 * @file
 * @brief WalB client tool.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <string>
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
#include "protocol.hpp"
#include "net_util.hpp"

struct Option : public cybozu::Option
{
    std::string addr;
    uint16_t port;
    std::string cmd;
    std::vector<std::string> params;
    std::string clientId;
    Option() {
        appendMust(&addr, "a", "host name or address");
        appendMust(&port, "p", "port number");
        appendParam(&cmd, "command", "command name");
        appendParamVec(&params, "parameters", "command parameters");

        std::string hostName = cybozu::net::getHostName();
        appendOpt(&clientId, hostName, "id", "client identfier");

        appendHelp("h");
    }
};

namespace walb {

void runClient(Option &opt)
{
    cybozu::Socket sock;
    sock.connect(opt.addr, opt.port);
    std::atomic<bool> forceQuit(false);
    std::string serverId = protocol::run1stNegotiateAsClient(
        sock, opt.clientId, opt.cmd);
    ProtocolLogger logger(opt.clientId, serverId);

    const std::map<std::string, protocol::ClientHandler> h = {
        { "echo", clientEcho },
        { "storage-status", clientStorageStatus },
        { "proxy-status", clientProxyStatus },
        { "archive-status", clientArchiveStatus },
        { "init-vol", clientInitVol },
    };
    protocol::clientDispatch(opt.cmd, sock, logger, forceQuit, opt.params, h);
}

} // namespace walb

int main(int argc, char *argv[])
try {
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    cybozu::SetLogFILE(::stderr);
    walb::runClient(opt);

} catch (std::exception &e) {
    LOGe("Client: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("Client: caught other error.");
    return 1;
}

/* end of file */
