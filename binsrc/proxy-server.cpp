/**
 * @file
 * @brief WalB proxy daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include "thread_util.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/option.hpp"
#include "file_path.hpp"
#include "net_util.hpp"
#include "server_util.hpp"
#include "walb_util.hpp"
#include "proxy.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb/proxy";
const std::string DEFAULT_LOG_FILE = "-";

using namespace walb;

/**
 * Request worker.
 */
class ProxyRequestWorker : public server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        const std::map<std::string, protocol::ServerHandler> h = {
            { getStateCN, c2pGetStateServer },
            { statusCN, c2pStatusServer },
            { listVolCN, c2pListVolServer },
            { startCN, c2pStartServer },
            { stopCN, c2pStopServer },
            { archiveInfoCN, c2pArchiveInfoServer },
            { clearVolCN, c2pClearVolServer },
            { wlogTransferPN, s2pWlogTransferServer },
            { resizeCN, c2pResizeServer },
            { hostTypeCN, c2pHostTypeServer },
        };
        protocol::serverDispatch(sock_, nodeId_, procStat_, h);
    }
};

struct Option : cybozu::Option
{
    uint16_t port;
    std::string logFileStr;
    bool isDebug;
    size_t maxBackgroundTasks;
    bool isStopped;
    Option() {
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendBoolOpt(&isDebug, "debug", "put debug message.");
        appendOpt(&maxBackgroundTasks, DEFAULT_MAX_BACKGROUND_TASKS, "maxBgTasks", "num of max concurrent background tasks.");
        appendBoolOpt(&isStopped, "stop", "Start a daemon in stopped state for all volumes.");

        ProxySingleton &p = getProxyGlobal();
        appendOpt(&p.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "maxFgTasks", "num of max concurrent foreground tasks.");
        appendOpt(&p.maxWdiffSendMb, DEFAULT_MAX_WDIFF_SEND_MB, "maxWdiffSendMb", "max size of wdiff files to send [MB].");
        appendOpt(&p.delaySecForRetry, DEFAULT_DELAY_SEC_FOR_RETRY, "delay", "Waiting time for next retry [sec].");
        appendOpt(&p.retryTimeout, DEFAULT_RETRY_TIMEOUT_SEC, "retryTimeout", "Retry timeout (total period) [sec].");
        appendOpt(&p.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory");
        appendOpt(&p.maxConversionMb, DEFAULT_MAX_CONVERSION_MB, "maxConversionMb", "max memory size of wlog-wdiff conversion [MB].");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&p.nodeId, hostName, "id", "node identifier");
        appendOpt(&p.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "sockTimeout", "Socket timeout [sec].");

        appendHelp("h");
    }
    std::string logFilePath() const {
        if (logFileStr == "-") return logFileStr;
        return (cybozu::FilePath(gp.baseDirStr) + logFileStr).str();
    }
};

void initializeProxy(Option &opt)
{
    util::makeDir(gp.baseDirStr, "proxyServer", false);

    // Start each volume if necessary
    if (!opt.isStopped) {
        for (const std::string &volId : util::getDirNameList(gp.baseDirStr)) {
            try {
                startProxyVol(volId);
            } catch (std::exception &e) {
                LOGs.error() << "initializeProxy:start failed" << volId << e.what();
            }
        }
    }

    // Start a task dispatch thread.
    ProxySingleton &g = getProxyGlobal();
    g.dispatcher.reset(new DispatchTask<ProxyTask, ProxyWorker>(g.taskQueue, opt.maxBackgroundTasks));
}

void finalizeProxy()
{
    // Stop the task dispatch thread.
    ProxySingleton &g = getProxyGlobal();
    g.taskQueue.quit();
    g.dispatcher.reset();
}

int main(int argc, char *argv[]) try
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    util::setLogSetting(opt.logFilePath(), opt.isDebug);
    initializeProxy(opt);
    auto createRequestWorker = [&](
        cybozu::Socket &&sock,
        std::atomic<server::ProcessStatus> &procStat) {
        return std::make_shared<ProxyRequestWorker>(
            std::move(sock), gp.nodeId, procStat);
    };

    ProxySingleton &g = getProxyGlobal();
    const size_t concurrency = g.maxForegroundTasks > 0 ? g.maxForegroundTasks + 1 : 0;
    server::MultiThreadedServer server(g.forceQuit, concurrency);
    server.run<ProxyRequestWorker>(opt.port, createRequestWorker);
    finalizeProxy();
} catch (std::exception &e) {
    LOGe("ProxyServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ProxyServer: caught other error.");
    return 1;
}

/* end of file */
