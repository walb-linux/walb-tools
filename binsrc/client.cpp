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

int main(int argc, char *argv[])
try {
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        throw std::runtime_error("option error.");
    }
    cybozu::SetLogFILE(::stderr);
    cybozu::Socket sock;
    sock.connect(opt.addr, opt.port);
    std::atomic<bool> forceQuit(false);
    walb::protocol::runProtocolAsClient(
        sock, opt.clientId, forceQuit, opt.cmd, opt.params);
    return 0;
} catch (std::exception &e) {
    ::fprintf(::stderr, "exception: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "caught other error.\n");
    return 1;
}

/* end of file */
