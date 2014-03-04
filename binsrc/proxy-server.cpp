/**
 * @file
 * @brief WalB proxy daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <chrono>
#include <thread>
#include <string>
#include <atomic>
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
            { "status", c2pStatusServer },
            { "start", c2pStartServer },
            { "stop", c2pStopServer },
            { "archive-info", c2pArchiveInfoServer },
            { "clear-vol", c2pClearVolServer },
            { "wlog-transfer", s2pWlogTransferServer },
        };
        protocol::serverDispatch(sock_, nodeId_, procStat_, h);
    }
};

struct Option : cybozu::Option
{
    uint16_t port;
    std::string logFileStr;
    bool isDebug;
    size_t maxConnections;
    size_t maxBackgroundTasks;
    size_t maxWdiffSendMb;
    size_t waitForRetry;
    size_t retryTimeout;
    bool isStopped;
    Option() {
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendBoolOpt(&isDebug, "debug", "put debug message.");
        appendOpt(&maxConnections, DEFAULT_MAX_CONNECTIONS, "maxConn", "num of max connections.");
        appendOpt(&maxBackgroundTasks, DEFAULT_MAX_BACKGROUND_TASKS, "maxBgTasks", "num of max background tasks.");
        appendOpt(&maxWdiffSendMb, DEFAULT_MAX_WDIFF_SEND_MB, "maxWdiffSendMb", "max size of wdiff files to send [MB].");
        appendOpt(&waitForRetry, DEFAULT_WAIT_FOR_RETRY, "waitForRetry", "Waiting time for next retry [sec].");
        appendOpt(&retryTimeout, DEFAULT_RETRY_TIMEOUT, "retryTimeout", "Retry timeout (total period) [sec].");
        appendBoolOpt(&isStopped, "stop", "Start a daemon in stopped state for all volumes.");

        ProxySingleton &p = getProxyGlobal();
        appendOpt(&p.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&p.nodeId, hostName, "id", "node identifier");

        appendHelp("h");
    }
    std::string logFilePath() const {
        if (logFileStr == "-") return logFileStr;
        return (cybozu::FilePath(gp.baseDirStr) + logFileStr).str();
    }
};

void initSingleton(Option &/*opt*/)
{
    //ProxySingleton &p = ProxySingleton::getInstance();

    // QQQ
}

void initializeProxy(Option &opt)
{
    util::makeDir(gp.baseDirStr, "proxyServer", false);
    initSingleton(opt);

    // Check all the volumes directories.
    // For each volume:
    //   If opt.stop is true, set stopped state for the volume.
    //   Otherwise, set started state for the volume and enqueue a background task.
    //   If master/*.wdiff exist,
    //   check they have been copied to the each archive directory.

    // Start a task dispatch thread.
    ProxySingleton &g = getProxyGlobal();
    g.dispatcher.reset(new util::DispatchTask<ProxyTask, ProxyWorker>(g.taskQueue, opt.maxBackgroundTasks));
}

void finalizeProxy()
{
    // Stop the task dispatch thread.
    ProxySingleton &g = getProxyGlobal();
    g.dispatcher.reset();

    // QQQ
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
    server::MultiThreadedServer server(
        getProxyGlobal().forceQuit, opt.maxConnections);
    server.run(opt.port, createRequestWorker);
    finalizeProxy();

} catch (std::exception &e) {
    LOGe("ProxyServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ProxyServer: caught other error.");
    return 1;
}

/* end of file */
