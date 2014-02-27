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

namespace walb {

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
        };
        protocol::serverDispatch(
            sock_, nodeId_, forceQuit_, procStat_, h);
    }
};

} // namespace walb

struct Option : cybozu::Option
{
    uint16_t port;
    std::string baseDirStr;
    std::string logFileStr;
    std::string nodeId;
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
        appendOpt(&baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendBoolOpt(&isDebug, "debug", "put debug message.");
        appendOpt(&maxConnections, walb::DEFAULT_MAX_CONNECTIONS, "maxConn", "num of max connections.");
        appendOpt(&maxBackgroundTasks, walb::DEFAULT_MAX_BACKGROUND_TASKS, "maxBgTasks", "num of max background tasks.");
        appendOpt(&maxWdiffSendMb, walb::DEFAULT_MAX_WDIFF_SEND_MB, "maxWdiffSendMb", "max size of wdiff files to send [MB].");
        appendOpt(&waitForRetry, walb::DEFAULT_WAIT_FOR_RETRY, "waitForRetry", "Waiting time for next retry [sec].");
        appendOpt(&retryTimeout, walb::DEFAULT_RETRY_TIMEOUT, "retryTimeout", "Retry timeout (total period) [sec].");
        appendBoolOpt(&isStopped, "stop", "Start a daemon in stopped state for all volumes.");

        std::string hostName = cybozu::net::getHostName();
        appendOpt(&nodeId, hostName, "id", "node identifier");

        appendHelp("h");
    }
    std::string logFilePath() const {
        if (logFileStr == "-") return logFileStr;
        return (cybozu::FilePath(baseDirStr) + logFileStr).str();
    }
};

void initSingleton(Option &opt)
{
    walb::ProxySingleton &s = walb::ProxySingleton::getInstance();

    s.nodeId = opt.nodeId;
    s.baseDirStr = opt.baseDirStr;

    // QQQ
}

void initializeProxy(Option &opt)
{
    initSingleton(opt);

    // Check all the volumes directories.
    // For each volume:
    //   If opt.stop is true, set stopped state for the volume.
    //   Otherwise, set started state for the volume and enqueue a background task.

    // Start a task dispatch thread.

    // QQQ
}

void finalizeProxy()
{
    // Stop the task dispatch thread.

    // QQQ
}

int main(int argc, char *argv[]) try
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    walb::util::makeDir(opt.baseDirStr, "proxyServer", false);
    walb::util::setLogSetting(opt.logFilePath(), opt.isDebug);
    initializeProxy(opt);
    auto createRequestWorker = [&](
        cybozu::Socket &&sock, const std::atomic<bool> &forceQuit,
        std::atomic<walb::server::ProcessStatus> &procStat) {
        return std::make_shared<walb::ProxyRequestWorker>(
            std::move(sock), opt.nodeId, forceQuit, procStat);
    };
    walb::server::MultiThreadedServer server(opt.maxConnections);
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
