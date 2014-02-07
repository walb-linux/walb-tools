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
const std::string DEFAULT_LOG_FILE = "walb-storage.log";

namespace walb {

/**
 * Request worker.
 */
class StorageRequestWorker : public server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        const std::map<std::string, protocol::ServerHandler> h = {
            { "status", c2sStatusServer },
            { "storage-init-vol", c2sInitVolServer },
        };
        protocol::serverDispatch(
            sock_, serverId_, baseDirStr_, forceQuit_, procStat_, h);
    }
};

} // namespace walb

struct Option : cybozu::Option
{
    uint16_t port;
    std::string baseDirStr;
    std::string logFileStr;
    std::string serverId;
    Option() {
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");

        std::string hostName = cybozu::net::getHostName();
        appendOpt(&serverId, hostName, "id", "server identifier");

        appendHelp("h");
    }
    std::string logFilePath() const {
        return (cybozu::FilePath(baseDirStr) + cybozu::FilePath(logFileStr)).str();
    }
};

int main(int argc, char *argv[]) try
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    cybozu::OpenLogFile(opt.logFilePath());
    walb::util::makeDir(opt.baseDirStr, "storageServer", false);
    auto createRequestWorker = [&](
        cybozu::Socket &&sock, const std::atomic<bool> &forceQuit,
        std::atomic<walb::server::ProcessStatus> &procStat) {
        return std::make_shared<walb::StorageRequestWorker>(
            std::move(sock), opt.serverId, opt.baseDirStr, forceQuit, procStat);
    };
    walb::server::MultiThreadedServer server;
    server.run(opt.port, createRequestWorker);

} catch (std::exception &e) {
    LOGe("StorageServer: error: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("StorageServer: caught other error.");
    return 1;
}

/* end of file */
