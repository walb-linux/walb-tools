/**
 * @file
 * @brief WalB server daemon.
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
#include "protocol.hpp"
#include "net_util.hpp"
#include "file_path.hpp"
#include "server_util.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb";
const std::string DEFAULT_LOG_FILE = "server.log";

using CtrlFlag = std::atomic<walb::server::ControlFlag>;

class ServerRequestWorker : public walb::server::RequestWorker
{
public:
    using RequestWorker :: RequestWorker;
    void run() override {
        walb::protocol::serverDispatch(
            sock_, serverId_, baseDir_.str(), forceQuit_, ctrlFlag_);
    }
};

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

#if 0
    if (daemon(0, 0) < 0) {
        LOGe("daemon() failed");
        return 1;
    }
#endif

    cybozu::FilePath baseDir(opt.baseDirStr);
    if (!baseDir.stat().exists()) {
        if (!baseDir.mkdir()) {
            LOGe("mkdir base directory %s failed.", baseDir.cStr());
            return 1;
        }
    }
    if (!baseDir.stat(true).isDirectory()) {
        LOGe("base directory %s is not directory.", baseDir.cStr());
        return 1;
    }

    auto createRequestWorker = [&](
        cybozu::Socket &&sock, const std::atomic<bool> &forceQuit, CtrlFlag &ctrlFlag) {
        return std::make_shared<ServerRequestWorker>(
            std::move(sock), opt.serverId, baseDir, forceQuit, ctrlFlag);
    };
    walb::server::MultiThreadedServer server;
    server.run(opt.port, createRequestWorker);

    return 0;
} catch (std::exception &e) {
    ::fprintf(::stderr, "error: %s\n", e.what());
    return 1;
}

/* end of file */
