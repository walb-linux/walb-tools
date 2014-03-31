/**
 * @file
 * @brief WalB controller tool.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <string>
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
#include "net_util.hpp"
#include "controller.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option : public cybozu::Option
{
    std::string addr;
    uint16_t port;
    std::string cmd;
    std::vector<std::string> params;
    std::string ctrlId;
    bool isDebug;
    size_t socketTimeout;
    Option() {
        appendMust(&addr, "a", "host name or address");
        appendMust(&port, "p", "port number");
        appendParam(&cmd, "command", "command name");
        appendParamVec(&params, "parameters", "command parameters");
        appendBoolOpt(&isDebug, "debug", "put debug message.");
        appendOpt(&socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "sockTimeout", "Socket timeout [sec].");

        std::string hostName = cybozu::net::getHostName();
        appendOpt(&ctrlId, hostName, "id", "controller identfier");

        appendHelp("h");
    }
};

void runClient(Option &opt)
{
    cybozu::Socket sock;
    util::connectWithTimeout(sock, cybozu::SocketAddr(opt.addr, opt.port), opt.socketTimeout);
    std::string serverId = protocol::run1stNegotiateAsClient(
        sock, opt.ctrlId, opt.cmd);
    ProtocolLogger logger(opt.ctrlId, serverId);

    const std::map<std::string, protocol::ClientHandler> h = {
        { statusPN, c2xGetStrVecClient },
        { listVolPN, c2xListVolClient },
        { initVolPN, c2xInitVolClient },
        { clearVolPN, c2xClearVolClient },
        { resetVolPN, c2xResetVolClient },
        { fullBkpPN, c2sFullSyncClient },
        { restorePN, c2aRestoreClient },
        { dropPN, c2aDropClient },
        { startPN, c2xStartClient },
        { stopPN, c2xStopClient },
        { archiveInfoPN, c2pArchiveInfoClient },
        { snapshotPN, c2sSnapshotClient },
        { applyPN, c2aApplyClient },
        { mergePN, c2aMergeClient },
        { resizePN, c2xResizeClient },
        { hostTypePN, c2xHostTypeClient },
        { dbgReloadMetadataPN, c2aReloadMetadataClient },
    };
    protocol::clientDispatch(opt.cmd, sock, logger, opt.params, h);
}

int main(int argc, char *argv[])
try {
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    util::setLogSetting("-", opt.isDebug);
    runClient(opt);

} catch (std::exception &e) {
    LOGe("Controller: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("Controller: caught other error.");
    return 1;
}

/* end of file */
