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

struct Option
{
    uint16_t port;
    std::string logFileStr;
    bool isDebug;
    size_t maxBackgroundTasks;
    bool isStopped;
    cybozu::Option opt;

    Option(int argc, char *argv[]) {

        opt.appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        opt.appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        opt.appendBoolOpt(&isDebug, "debug", "put debug message.");
        opt.appendOpt(&maxBackgroundTasks, DEFAULT_MAX_BACKGROUND_TASKS, "bg", "num of max concurrent background tasks.");
        opt.appendBoolOpt(&isStopped, "stop", "Start a daemon in stopped state for all volumes.");

        ProxySingleton &p = getProxyGlobal();
        opt.appendOpt(&p.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "fg", "num of max concurrent foreground tasks.");
        opt.appendOpt(&p.maxWdiffSendMb, DEFAULT_MAX_WDIFF_SEND_MB, "wd", "max size of wdiff files to send [MiB].");
        opt.appendOpt(&p.maxWdiffSendNr, DEFAULT_MAX_WDIFF_SEND_NR, "wn", "max number of wdiff files to send.");
        opt.appendOpt(&p.delaySecForRetry, DEFAULT_DELAY_SEC_FOR_RETRY, "delay", "Waiting time for next retry [sec].");
        opt.appendOpt(&p.retryTimeout, DEFAULT_RETRY_TIMEOUT_SEC, "rto", "Retry timeout (total period) [sec].");
        opt.appendOpt(&p.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory");
        opt.appendOpt(&p.maxConversionMb, DEFAULT_MAX_CONVERSION_MB, "wl", "max memory size of wlog-wdiff conversion [MiB].");
        std::string hostName = cybozu::net::getHostName();
        opt.appendOpt(&p.nodeId, hostName, "id", "node identifier");
        opt.appendOpt(&p.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "to", "Socket timeout [sec].");

        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        util::verifyNotZero(maxBackgroundTasks, "maxBackgroundtasks");
        util::verifyNotZero(p.maxForegroundTasks, "maxForegroundtasks");
        util::verifyNotZero(p.maxWdiffSendMb, "maxWdiffSendMb");
        util::verifyNotZero(p.maxWdiffSendNr, "maxWdiffSendNr");
        util::verifyNotZero(p.maxConversionMb, "maxConversionMb");
    }
};

struct ProxyThreads
{
    explicit ProxyThreads(Option &opt) {
        util::makeDir(gp.baseDirStr, "proxyServer", false);

        // Start each volume if necessary
        if (!opt.isStopped) {
            LOGs.info() << "search volume metadata directories" << gp.baseDirStr;
            for (const std::string &volId : util::getDirNameList(gp.baseDirStr)) {
                LOGs.info() << "found volume" << volId;
                try {
                    startProxyVol(volId);
                } catch (std::exception &e) {
                    LOGs.error() << "initializeProxy:start failed" << volId << e.what();
                    ::exit(1);
                }
            }
        }

        // Start a task dispatch thread.
        ProxySingleton &g = getProxyGlobal();
        g.dispatcher.reset(new DispatchTask<ProxyTask, ProxyWorker>(g.taskQueue, opt.maxBackgroundTasks));
    }
    ~ProxyThreads() try {
        // Stop the task dispatch thread.
        ProxySingleton &g = getProxyGlobal();
        g.taskQueue.quit();
        g.dispatcher.reset();
    } catch (std::exception &e) {
        LOGe("ProxyThreads error: %s", e.what());
    }
};

int main(int argc, char *argv[]) try
{
    Option opt(argc, argv);
    ProxySingleton &g = getProxyGlobal();
    util::setLogSetting(createLogFilePath(opt.logFileStr, g.baseDirStr), opt.isDebug);
    LOGs.info() << "starting walb proxy server";
    LOGs.info() << opt.opt;
    {
        ProxyThreads threads(opt);
        server::MultiThreadedServer server;
        const size_t concurrency = g.maxForegroundTasks + 5;
        server.run(g.ps, opt.port, g.nodeId, proxyHandlerMap, concurrency);
    }
    LOGs.info() << "shutdown walb proxy server";

} catch (std::exception &e) {
    LOGe("ProxyServer: error: %s", e.what());
    ::fprintf(::stderr, "ProxyServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ProxyServer: caught other error.");
    ::fprintf(::stderr, "ProxyServer: caught other error.");
    return 1;
}

/* end of file */
