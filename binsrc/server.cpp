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

#include "sys_logger.hpp"

#include "thread_util.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
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
public:
    explicit RequestWorker(cybozu::Socket &&sock)
        : sock_(sock) /* sock will be invalid. */ {
    }
    void operator()() noexcept override {
        try {
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
    void run() {
        uint32_t i;
        sock_.read(&i, sizeof(i));
        LOGd("recv %u", i);
        i++;
        sock_.write(&i, sizeof(i));
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
};

struct Option : cybozu::Option
{
    uint16_t port;
    std::string baseDirStr;
    std::string logFileStr;
    Option() {
        //setUsage();
        appendOpt(&port, DEFAULT_LISTEN_PORT, "p", "listen port");
        appendOpt(&baseDirStr, DEFAULT_BASE_DIR, "b", "base directory (full path)");
        appendOpt(&logFileStr, DEFAULT_LOG_FILE, "l", "log file name.");
    }
    bool parse(int argc, char *argv[]) {
        return cybozu::Option::parse(argc, argv);
    }
    std::string logFilePath() const {
        return (cybozu::FilePath(baseDirStr) + cybozu::FilePath(logFileStr)).str();
    }
};

int main(int argc, char *argv[])
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

    cybozu::thread::ThreadRunnerPool pool;
    while (true) {
        while (!ssock.queryAccept()) {
        }
        cybozu::Socket sock;
        ssock.accept(sock);
        pool.add(std::make_shared<RequestWorker>(std::move(sock)));
        pool.gc();
        LOGi("pool size %zu", pool.size());
    }
    pool.join();
    return 0;
}

/* end of file */
