/**
 * @file
 * @brief WalB controller tool.
 */
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
#include "net_util.hpp"
#include "controller.hpp"
#include "walb_util.hpp"

using namespace walb;

const std::map<std::string, protocol::ClientHandler> controllerHandlerMap = {
    { statusCN, c2xGetStrVecClient },
    { initVolCN, c2xInitVolClient },
    { clearVolCN, c2xClearVolClient },
    { resetVolCN, c2xResetVolClient },
    { fullBkpCN, c2sFullBkpClient },
    { hashBkpCN, c2sHashBkpClient },
    { restoreCN, c2aRestoreClient },
    { delRestoredCN, c2aDelRestoredClient },
    { startCN, c2xStartClient },
    { stopCN, c2xStopClient },
    { archiveInfoCN, c2pArchiveInfoClient },
    { snapshotCN, c2sSnapshotClient },
    { replicateCN, c2aReplicateClient },
    { applyCN, c2aApplyClient },
    { mergeCN, c2aMergeClient },
    { resizeCN, c2xResizeClient },
    { kickCN, c2xKickClient },
    { blockHashCN, c2aBlockHashClient },
    { dbgReloadMetadataCN, c2aReloadMetadataClient },
    { dbgSetUuid, c2aSetUuidClient },
    { dbgSetState, c2aSetStateClient },
    { dbgSetBase, c2aSetBaseClient },
    { getCN, c2xGetClient },
    { execCN, c2xGetStrVecClient },
};

struct Option
{
    std::string addr;
    uint16_t port;
    std::string cmd;
    std::vector<std::string> params;
    std::string ctrlId;
    bool isDebug;
    size_t socketTimeout;
    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setDelimiter("---", &params);
        opt.appendMust(&addr, "a", "host name or address");
        opt.appendMust(&port, "p", "port number");
        opt.appendParam(&cmd, "command", "command name");
        opt.appendParamVec(&params, "parameters", "command parameters");
        opt.appendBoolOpt(&isDebug, "debug", "put debug message.");
        opt.appendOpt(&socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "Socket timeout [sec].");

        std::string hostName = cybozu::net::getHostName();
        opt.appendOpt(&ctrlId, hostName, "id", "controller identfier");

        opt.appendHelp("h");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

void runClient(Option &opt)
{
    cybozu::Socket sock;
    util::connectWithTimeout(sock, cybozu::SocketAddr(opt.addr, opt.port), opt.socketTimeout);
    std::string serverId = protocol::run1stNegotiateAsClient(
        sock, opt.ctrlId, opt.cmd);
    ProtocolLogger logger(opt.ctrlId, serverId);
    protocol::clientDispatch(opt.cmd, sock, logger, opt.params, controllerHandlerMap);
}

int main(int argc, char *argv[])
try {
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    runClient(opt);
} catch (std::exception &e) {
    LOGe("walbc error: %s", e.what());
    return 1;
}
