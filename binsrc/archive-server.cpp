/**
 * @file
 * @brief WalB archive daemon.
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
#include "archive.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb/archive";
const std::string DEFAULT_LOG_FILE = "-";
const std::string DEFAULT_VG = "vg";

using namespace walb;

/**
 * Request worker.
 */
class ArchiveRequestWorker : public server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        const std::map<std::string, protocol::ServerHandler> h = {
            { statusPN, c2aStatusServer },
            { listVolPN, c2aListVolServer },
            { initVolPN, c2aInitVolServer },
            { clearVolPN, c2aClearVolServer },
            { startPN, c2aStartServer },
            { stopPN, c2aStopServer },
            { dirtyFullSyncPN, x2aDirtyFullSyncServer },
            { restorePN, c2aRestoreServer },
            { wdiffTransferPN, x2aWdiffTransferServer },
            { dbgReloadMetadataPN, c2aReloadMetadataServer },
            { resizePN, c2aResizeServer },
            { hostTypePN, c2aHostTypeServer },
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

    Option() {
        ArchiveSingleton &a = getArchiveGlobal();
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&a.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendOpt(&a.volumeGroup, DEFAULT_VG, "vg", "lvm volume group.");
        appendBoolOpt(&isDebug, "debug", "put debug message.");
        appendOpt(&maxConnections, DEFAULT_MAX_CONNECTIONS, "maxConn", "num of max connections.");

        std::string hostName = cybozu::net::getHostName();
        appendOpt(&a.nodeId, hostName, "id", "node identifier");

        appendHelp("h");
    }
    std::string logFilePath() const {
        if (logFileStr == "-") return logFileStr;
        return (cybozu::FilePath(ga.baseDirStr) + logFileStr).str();
    }
};

void initializeArchive(Option &/*opt*/)
{
    util::makeDir(ga.baseDirStr, "archiveServer", false);

    // Start task dispatcher thread.

    // QQQ
}

void finalizeArchive()
{
    // Stop task dispatcher thread.

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
    initializeArchive(opt);
    auto createRequestWorker = [&](
        cybozu::Socket &&sock,
        std::atomic<server::ProcessStatus> &procStat) {
        return std::make_shared<ArchiveRequestWorker>(
            std::move(sock), ga.nodeId, procStat);
    };

    server::MultiThreadedServer server(
        getArchiveGlobal().forceQuit, opt.maxConnections);
    server.run(opt.port, createRequestWorker);
    finalizeArchive();

} catch (std::exception &e) {
    LOGe("ArchiveServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ArchiveServer: caught other error.");
    return 1;
}

/* end of file */
