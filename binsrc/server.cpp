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

#include "sys_logger.hpp"

#include "thread_util.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/option.hpp"
#include "file_path.hpp"
#include "protocol.hpp"
#include "net_util.hpp"
#include "file_path.hpp"

/* These should be defined in the parameter header. */
const uint16_t DEFAULT_LISTEN_PORT = 5000;
const std::string DEFAULT_BASE_DIR = "/var/forest/walb";
const std::string DEFAULT_LOG_FILE = "server.log";

/**
 * Request worker.
 */
class RequestWorker : public cybozu::thread::Runnable
{
private:
    cybozu::Socket sock_;
    std::string serverId_;
    cybozu::FilePath baseDir_;
public:
    RequestWorker(cybozu::Socket &&sock, const std::string &serverId,
                  const cybozu::FilePath &baseDir)
        : sock_(std::move(sock))
        , serverId_(serverId)
        , baseDir_(baseDir) {
    }
    void operator()() noexcept override {
        try {
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
        sock_.close();
    }
    void run() {
        walb::runProtocolAsServer(sock_, serverId_, baseDir_.str());
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

void logErrors(std::vector<std::exception_ptr> &&v)
{
    for (std::exception_ptr ep : v) {
        try {
            std::rethrow_exception(ep);
        } catch (std::exception &e) {
            LOGe("RequestWorker caught an error: %s.", e.what());
        } catch (...) {
            LOGe("RequestWokrer caught an unknown error.");
        }
    }
}

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

    cybozu::Socket ssock;
    ssock.bind(opt.port);

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

    cybozu::thread::ThreadRunnerPool pool;
    while (true) {
        while (!ssock.queryAccept()) {
        }
        cybozu::Socket sock;
        ssock.accept(sock);
        pool.add(std::make_shared<RequestWorker>(std::move(sock), opt.serverId, baseDir));
        logErrors(pool.gc());
        LOGi("pool size %zu", pool.size());
    }
    pool.waitForAll();
    return 0;
} catch (std::exception &e) {
    ::fprintf(::stderr, "error: %s\n", e.what());
    return 1;
}

/* end of file */
