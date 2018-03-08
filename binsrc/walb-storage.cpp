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
#include "host_info.hpp"
#include "storage.hpp"
#include "version.hpp"

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
    uint64_t defaultFullScanBytesPerSec;
    std::string cmprOptForSyncStr;
    cybozu::Option opt;

    Option(int argc, char *argv[]) {
        opt.setDescription(util::getDescription("walb storage server"));

        opt.appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "PORT : listen port");
        opt.appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "PATH : log file name.");
        opt.appendMust(&archiveDStr, "archive", "HOST_PORT : archive daemon (host:port)");
        opt.appendMust(&multiProxyDStr, "proxy", "HOST_PORT_LIST : proxy daemons (host:port,host:port,...)");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug message.");

        StorageSingleton &s = getStorageGlobal();
        opt.appendOpt(&s.maxConnections, DEFAULT_MAX_CONNECTIONS, "maxconn", "NUM : num of max connections.");
        opt.appendOpt(&s.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "fg", "NUM : num of max concurrent foregroud tasks.");
        opt.appendOpt(&s.maxBackgroundTasks, DEFAULT_MAX_BACKGROUND_TASKS, "bg", "NUM : num of max concurrent background tasks.");
        opt.appendOpt(&s.baseDirStr, DEFAULT_BASE_DIR, "b", "PATH : base directory (full path)");
        std::string hostName = cybozu::net::getHostName();
        opt.appendOpt(&s.nodeId, hostName, "id", "STRING : node identifier");
        opt.appendOpt(&s.maxWlogSendMb, DEFAULT_MAX_WLOG_SEND_MB, "wl", "SIZE : max wlog size to send at once [MiB].");
        opt.appendOpt(&s.implicitSnapshotIntervalSec, DEFAULT_IMPLICIT_SNAPSHOT_INTERVAL_SEC, "snapintvl"
                      , "PERIOD : implicit snapshot interval [sec].");
        opt.appendOpt(&s.delaySecForRetry, DEFAULT_MIN_DELAY_SEC_FOR_RETRY, "delay", "PERIOD : waiting time for next retry [sec].");
        opt.appendOpt(&s.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "PERIOD : socket timeout [sec].");
        opt.appendOpt(&defaultFullScanBytesPerSec, DEFAULT_FULL_SCAN_BYTES_PER_SEC, "fst", "SIZE : default full scan throughput [bytes/s]");
        opt.appendOpt(&s.tsDeltaGetterIntervalSec, DEFAULT_TS_DELTA_INTERVAL_SEC, "tsdintvl", "PERIOD : ts-delta getter interval [sec].");
        opt.appendOpt(&cmprOptForSyncStr, DEFAULT_CMPR_OPT_FOR_SYNC, "sync-cmpr", "COMPRESSION_OPT : compression option for full/hash sync like 'snappy:0:1'.");
#ifdef ENABLE_EXEC_PROTOCOL
        opt.appendBoolOpt(&s.allowExec, "allow-exec", ": allow exec protocol for test. This is NOT SECURE.");
#endif
        util::setKeepAliveOptions(opt, s.keepAliveParams);

        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        util::verifyNotZero(s.maxBackgroundTasks, "maxBackgroundTasks");
        util::verifyNotZero(s.maxForegroundTasks, "maxForegroundTasks");
        util::verifyNotZero(s.maxWlogSendMb, "maxWlogSendMb");
        util::verifyNotZero(s.implicitSnapshotIntervalSec, "implicitSnapshotIntervalSec");
        util::verifyNotZero(s.tsDeltaGetterIntervalSec, "tsDeltaGetterIntervalSec");
        s.keepAliveParams.verify();
        s.fullScanLbPerSec = defaultFullScanBytesPerSec / LOGICAL_BLOCK_SIZE;
        s.cmprOptForSync = parseCompressOpt(cmprOptForSyncStr);
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

        g.dispatcher.reset(new DispatchTask<std::string, StorageWorker>(g.taskQueue, g.maxBackgroundTasks));
        g.wdevMonitor.reset(new std::thread(wdevMonitorWorker));
        g.proxyMonitor.reset(new std::thread(proxyMonitorWorker));
        g.tsDeltaGetter.reset(new std::thread(tsDeltaGetterWorker));
    }

    ~StorageThreads()
        try
    {
        StorageSingleton &g = getStorageGlobal();

        g.quitTsDeltaGetter = true;
        g.tsDeltaGetter->join();
        g.tsDeltaGetter.reset();

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
    LOGs.info() << util::getDescription("starting walb storage server");
    LOGs.info() << opt.opt;
    {
        StorageThreads threads(opt);
        server::MultiThreadedServer server;
        const size_t concurrency = g.maxConnections;
        server.run(g.ps, opt.port, g.nodeId, storageHandlerMap, g.handlerStatMgr,
                   concurrency, g.keepAliveParams, g.socketTimeout);
    }
    LOGs.info() << util::getDescription("shutdown walb storage server");

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
