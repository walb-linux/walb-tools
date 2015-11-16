/**
 * @file
 * @brief WalB storage daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
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
#include "version.hpp"
#include "build_date.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb/storage";
const std::string DEFAULT_LOG_FILE = "-";

using namespace walb;

struct Option
{
    uint16_t port;
    std::string logFileStr;
    std::string archiveDStr;
    std::string multiProxyDStr;
    bool isDebug;
    size_t maxBackgroundTasks;
    uint64_t defaultFullScanBytesPerSec;
    cybozu::Option opt;

    Option(int argc, char *argv[]) {
        opt.setDescription(cybozu::util::formatString("walb storage server version %s", getWalbToolsVersion()));

        opt.appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        opt.appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        opt.appendMust(&archiveDStr, "archive", "archive daemon (host:port)");
        opt.appendMust(&multiProxyDStr, "proxy", "proxy daemons (host:port,host:port,...)");
        opt.appendBoolOpt(&isDebug, "debug", "put debug message.");
        opt.appendOpt(&maxBackgroundTasks, DEFAULT_MAX_BACKGROUND_TASKS, "bg", "num of max concurrent background tasks.");

        StorageSingleton &s = getStorageGlobal();
        opt.appendOpt(&s.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "fg", "num of max concurrent foregroud tasks.");
        opt.appendOpt(&s.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        std::string hostName = cybozu::net::getHostName();
        opt.appendOpt(&s.nodeId, hostName, "id", "node identifier");
        opt.appendOpt(&s.maxWlogSendMb, DEFAULT_MAX_WLOG_SEND_MB, "wl", "max wlog size to send at once [MiB].");
        opt.appendOpt(&s.delaySecForRetry, DEFAULT_DELAY_SEC_FOR_RETRY, "delay", "Waiting time for next retry [sec].");
        opt.appendOpt(&s.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "Socket timeout [sec].");
        opt.appendOpt(&defaultFullScanBytesPerSec, DEFAULT_FULL_SCAN_BYTES_PER_SEC, "fst", "Default full scan throughput [bytes/s]");
#ifdef ENABLE_EXEC_PROTOCOL
        opt.appendBoolOpt(&s.allowExec, "allow-exec", "Allow exec protocol for test. This is NOT SECURE.");
#endif
        util::setKeepAliveOptions(opt, s.keepAliveParams);

        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        util::verifyNotZero(maxBackgroundTasks, "maxBackgroundTasks");
        util::verifyNotZero(s.maxForegroundTasks, "maxForegroundTasks");
        util::verifyNotZero(s.maxWlogSendMb, "maxWlogSendMb");
        s.keepAliveParams.verify();
        s.fullScanLbPerSec = defaultFullScanBytesPerSec / LOGICAL_BLOCK_SIZE;
    }
};

struct StorageThreads {
    explicit StorageThreads(Option &opt)
    {
        util::makeDir(gs.baseDirStr, "storageServer", false);
        StorageSingleton &g = getStorageGlobal();
        g.archive = parseSocketAddr(opt.archiveDStr);
        g.proxyV = parseMultiSocketAddr(opt.multiProxyDStr);
        g.proxyManager.add(g.proxyV);

        for (const std::string &volId : util::getDirNameList(gs.baseDirStr)) {
            try {
                startIfNecessary(volId);
            } catch (std::exception &e) {
                LOGs.error() << "initializeStorage:start failed" << volId << e.what();
                ::exit(1);
            }
        }

        g.dispatcher.reset(new DispatchTask<std::string, StorageWorker>(g.taskQueue, opt.maxBackgroundTasks));
        g.wdevMonitor.reset(new std::thread(wdevMonitorWorker));
        g.proxyMonitor.reset(new std::thread(proxyMonitorWorker));
    }

    ~StorageThreads()
        try
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
    } catch (std::exception& e) {
        LOGe("~StorageThreads err %s", e.what());
    }
};

int main(int argc, char *argv[]) try
{
    Option opt(argc, argv);
    StorageSingleton &g = getStorageGlobal();
    util::setLogSetting(createLogFilePath(opt.logFileStr, g.baseDirStr), opt.isDebug);
    LOGs.info() << "starting walb storage server";
    LOGs.info() << "version" << getWalbToolsVersion();
    LOGs.info() << "build date" << getWalbToolsBuildDate();
    LOGs.info() << opt.opt;
    {
        StorageThreads threads(opt);
        server::MultiThreadedServer server;
        const size_t concurrency = g.maxForegroundTasks + 5;
        server.run(g.ps, opt.port, g.nodeId, storageHandlerMap, concurrency, g.keepAliveParams, g.socketTimeout);
    }
    LOGs.info() << "shutdown walb storage server";

} catch (std::exception &e) {
    LOGe("StorageServer: error: %s\n", e.what());
    ::fprintf(::stderr, "StorageServer: error: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("StorageServer: caught other error.");
    ::fprintf(::stderr, "StorageServer: caught other error.");
    return 1;
}

/* end of file */
