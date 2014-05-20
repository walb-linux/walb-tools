/**
 * @file
 * @brief WalB archive daemon.
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
            { statusCN, c2aStatusServer },
            { listVolCN, c2aListVolServer },
            { initVolCN, c2aInitVolServer },
            { clearVolCN, c2aClearVolServer },
            { resetVolCN, c2aResetVolServer },
            { startCN, c2aStartServer },
            { stopCN, c2aStopServer },
            { dirtyFullSyncPN, x2aDirtyFullSyncServer },
            { dirtyHashSyncPN, x2aDirtyHashSyncServer },
            { restoreCN, c2aRestoreServer },
            { delRestoredCN, c2aDelRestoredServer },
            { wdiffTransferPN, x2aWdiffTransferServer },
            { dbgReloadMetadataCN, c2aReloadMetadataServer },
            { replicateCN, c2aReplicateServer },
            { replSyncPN, a2aReplSyncServer },
            { applyCN, c2aApplyServer },
            { mergeCN, c2aMergeServer },
            { resizeCN, c2aResizeServer },
            { hostTypeCN, c2aHostTypeServer },
        };
        protocol::serverDispatch(sock_, nodeId_, procStat_, h);
    }
};

struct Option : cybozu::Option
{
    uint16_t port;
    std::string logFileStr;
    bool isDebug;

    Option() {
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendBoolOpt(&isDebug, "debug", "put debug message.");

        ArchiveSingleton &a = getArchiveGlobal();
        appendOpt(&a.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&a.volumeGroup, DEFAULT_VG, "vg", "lvm volume group.");
        appendOpt(&a.maxForegroundTasks, DEFAULT_MAX_FOREGROUND_TASKS, "maxFgTasks", "num of max concurrent foreground tasks.");
        std::string hostName = cybozu::net::getHostName();
        appendOpt(&a.nodeId, hostName, "id", "node identifier");
        appendOpt(&a.socketTimeout, DEFAULT_SOCKET_TIMEOUT_SEC, "sockTimeout", "Socket timeout [sec].");

        appendHelp("h");
    }
    std::string logFilePath() const {
        if (logFileStr == "-") return logFileStr;
        return (cybozu::FilePath(ga.baseDirStr) + logFileStr).str();
    }
};

int main(int argc, char *argv[]) try
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    util::setLogSetting(opt.logFilePath(), opt.isDebug);
    util::makeDir(ga.baseDirStr, "ArchiveServer", false);
    auto createRequestWorker = [&](
        cybozu::Socket &&sock,
        std::atomic<server::ProcessStatus> &procStat) {
        return std::make_shared<ArchiveRequestWorker>(
            std::move(sock), ga.nodeId, procStat);
    };

    ArchiveSingleton &g = getArchiveGlobal();
    const size_t concurrency = g.maxForegroundTasks > 0 ? g.maxForegroundTasks + 1 : 0;
    server::MultiThreadedServer server(g.forceQuit, concurrency);
    server.run<ArchiveRequestWorker>(opt.port, createRequestWorker);
} catch (std::exception &e) {
    LOGe("ArchiveServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ArchiveServer: caught other error.");
    return 1;
}

/* end of file */
