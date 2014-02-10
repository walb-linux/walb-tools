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
const std::string DEFAULT_LOG_FILE = "walb-archive.log";
const std::string DEFAULT_VG = "vg";

namespace walb {

/**
 * Request worker.
 */
class ArchiveRequestWorker : public server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        const std::map<std::string, protocol::ServerHandler> h = {
            { "status", c2aStatusServer },
            { "archive-init-vol", c2aInitVolServer },
        };
        protocol::serverDispatch(
            sock_, nodeId_, forceQuit_, procStat_, h);
    }
};

} // namespace walb

struct Option : cybozu::Option
{
    uint16_t port;
    std::string logFileStr;

    Option() {
        walb::ArchiveSingleton &a = walb::getArchiveGlobal();
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&a.baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
        appendOpt(&a.volumeGroup, DEFAULT_VG, "vg", "lvm volume group.");

        std::string hostName = cybozu::net::getHostName();
        appendOpt(&a.nodeId, hostName, "id", "node identifier");

        appendHelp("h");
    }
    std::string logFilePath() const {
        return (cybozu::FilePath(walb::ga.baseDirStr) + cybozu::FilePath(logFileStr)).str();
    }
};

int main(int argc, char *argv[]) try
{
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    walb::util::makeDir(walb::ga.baseDirStr, "archiveServer", false);
    cybozu::OpenLogFile(opt.logFilePath());
    auto createRequestWorker = [&](
        cybozu::Socket &&sock, const std::atomic<bool> &forceQuit,
        std::atomic<walb::server::ProcessStatus> &procStat) {
        return std::make_shared<walb::ArchiveRequestWorker>(
            std::move(sock), walb::ga.nodeId, forceQuit, procStat);
    };
    walb::server::MultiThreadedServer server;
    server.run(opt.port, createRequestWorker);

} catch (std::exception &e) {
    LOGe("ArchiveServer: error: %s", e.what());
    return 1;
} catch (...) {
    LOGe("ArchiveServer: caught other error.");
    return 1;
}

/* end of file */
