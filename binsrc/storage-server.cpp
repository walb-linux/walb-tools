/**
 * @file
 * @brief WalB storage daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
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
#include "serializer.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"
#include "storage.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb/storage";
const std::string DEFAULT_LOG_FILE = "-";

using namespace walb;

/**
 * Request worker.
 */
class StorageRequestWorker : public server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        const std::map<std::string, protocol::ServerHandler> h = {
            { statusPN, c2sStatusServer },
            { listVolPN, c2sListVolServer },
            { initVolPN, c2sInitVolServer },
            { clearVolPN, c2sClearVolServer },
            { startPN, c2sStartServer },
            { stopPN, c2sStopServer },
            { fullBkpPN, c2sFullSyncServer },
            { snapshotPN, c2sSnapshotServer },
            { hostTypePN, c2sHostTypeServer },
        };
        protocol::serverDispatch(sock_, nodeId_, procStat_, h);
    }
};

struct Option : cybozu::Option
{
    uint16_t port;
    std::string logFileStr;
    std::string archiveDStr;
    std::string multiProxyDStr;
    bool isDebug;
    size_t maxConnections;
    size_t maxBackgroundTasks;
    Option() {
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendMust(&archiveDStr, "archive", "archive daemon (host:port)");
        appendMust(&multiProxyDStr, "proxy", "proxy daemons (host:port,host:port,...)");
        appendBoolOpt(&isDebug, "debug", "put debug message.");
        appendOpt(&maxConnections, DEFAULT_MAX_CONNECTIONS, "maxConn", "num of max connections.");

        StorageSingleton &s = getStorageGlobal();
        appendOpt(&s.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&s.nodeId, hostName, "id", "node identifier");
        appendOpt(&maxBackgroundTasks, DEFAULT_MAX_BACKGROUND_TASKS, "maxBgTasks", "num of max background tasks.");
        appendOpt(&s.maxWlogSendMb, DEFAULT_MAX_WLOG_SEND_MB, "maxWlogSendMb", "max wlog size to send at once.");
        appendOpt(&s.delaySecForRetry, DEFAULT_DELAY_SEC_FOR_RETRY, "delay", "Waiting time for next retry [sec].");

        appendHelp("h");
    }
    std::string logFilePath() const {
        if (logFileStr == "-") return logFileStr;
        return (cybozu::FilePath(gs.baseDirStr) + logFileStr).str();
    }
};

void initializeStorage(Option &opt)
{
    util::makeDir(gs.baseDirStr, "storageServer", false);
    StorageSingleton &g = getStorageGlobal();
    g.archive = parseSocketAddr(opt.archiveDStr);
    g.proxyV = parseMultiSocketAddr(opt.multiProxyDStr);
    g.proxyManager.add(g.proxyV);

    // QQQ

    g.dispatcher.reset(new DispatchTask<std::string, StorageWorker>(g.taskQueue, opt.maxBackgroundTasks));
    g.wdevMonitor.reset(new std::thread(wdevMonitorWorker));
    g.proxyMonitor.reset(new std::thread(proxyMonitorWorker));
}

void finalizeStorage()
{
    StorageSingleton &g = getStorageGlobal();

    g.quitProxyMonitor = true;
    g.proxyMonitor->join();
    g.proxyMonitor.reset();

    g.quitWdevMonitor = true;
    g.wdevMonitor->join();
    g.wdevMonitor.reset();

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
    initializeStorage(opt);
    auto createRequestWorker = [&](
        cybozu::Socket &&sock,
        std::atomic<server::ProcessStatus> &procStat) {
        return std::make_shared<StorageRequestWorker>(
            std::move(sock), gs.nodeId, procStat);
    };
    server::MultiThreadedServer server(getStorageGlobal().forceQuit, opt.maxConnections);
    server.run(opt.port, createRequestWorker);
    finalizeStorage();

} catch (std::exception &e) {
    LOGe("StorageServer: error: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("StorageServer: caught other error.");
    return 1;
}

/* end of file */
