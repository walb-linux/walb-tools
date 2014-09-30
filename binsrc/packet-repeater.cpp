/*
    packet repeater
*/
#include <cybozu/option.hpp>
#include <cybozu/socket.hpp>
#include <cybozu/log.hpp>
#include <cybozu/time.hpp>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include "sma.hpp"

std::atomic<int> g_quit;
std::atomic<bool> g_stop;
const bool dontThrow = true;

struct Option {
    std::string serverAddr;
    uint16_t serverPort;
    uint16_t recvPort;
    uint16_t cmdPort;
    uint32_t delaySec;
    double rateMbps;
    size_t threadNum;
    size_t socketTimeoutS;
    bool verbose;
    Option(int argc, char *argv[])
        : serverPort(0)
        , cmdPort(0)
        , delaySec(0)
        , rateMbps(0)
        , threadNum(0)
        , verbose(false)
    {
        cybozu::SetLogPriority(cybozu::LogInfo);
        cybozu::Option opt;
        bool vv = false;
        std::string logPath;
        opt.appendParam(&serverAddr, "server", ": server address");
        opt.appendParam(&serverPort, "port", ": server port");
        opt.appendParam(&recvPort, "recvPort", ": port to receive");
        opt.appendParam(&cmdPort, "cmdPort", ": port for command");
        opt.appendOpt(&delaySec, 0, "d", ": delay second");
        opt.appendOpt(&rateMbps, 0, "r", ": data rate(mega bit per second)");
        opt.appendOpt(&threadNum, 10, "t", ": num of thread");
        opt.appendOpt(&logPath, "-", "l", ": log path (default stderr)");
        opt.appendOpt(&socketTimeoutS, 0, "to", ": socket timeout [sec] (default no)");
        opt.appendBoolOpt(&verbose, "v", ": verbose message");
        opt.appendBoolOpt(&vv, "vv", ": more verbose message");
        opt.appendHelp("h");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
        if (vv) cybozu::SetLogPriority(cybozu::LogDebug);
        if (logPath == "-") {
            cybozu::SetLogFILE(::stderr);
        } else {
            cybozu::OpenLogFile(logPath);
        }
        opt.put();
    }
};

void setSocketTimeout(cybozu::Socket& socket, size_t timeoutS)
{
    if (timeoutS == 0) return;
    socket.setSendTimeout(timeoutS * 1000);
    socket.setReceiveTimeout(timeoutS * 1000);
}

class ThreadRunner {
    std::thread thread_;
public:
    void set(std::thread&& thread)
    {
        thread_ = std::move(thread);
    }
    ~ThreadRunner() noexcept
        try
    {
        g_quit = true;
        if (thread_.joinable()) thread_.join();
    } catch (std::exception& e) {
        cybozu::PutLog(cybozu::LogError, "ThreadRunner: error: %s", e.what());
    }
};

void cmdThread(const Option& opt)
    try
{
    if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "cmdThread start port=%d", opt.cmdPort);
    cybozu::Socket server;
    server.bind(opt.cmdPort);
    while (!g_quit) {
        while (!server.queryAccept()) {
            if (g_quit) break;
        }
        if (g_quit) break;
        try {
            cybozu::SocketAddr addr;
            cybozu::Socket client;
            server.accept(client, &addr);
            if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "cmdThread accept addr %s", addr.toStr().c_str());
            char buf[128];
            size_t readSize = client.readSome(buf, sizeof(buf));
            if (readSize > 0) {
                if (buf[readSize - 1] == '\n') readSize--;
                if (readSize > 0 && buf[readSize - 1] == '\r') readSize--;
                const std::string cmd(buf, readSize);
                if (cmd == "quit") {
                    if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "cmdThread quit");
                    g_quit = true;
                } else
                if (cmd == "stop") {
                    if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "cmdThread stop");
                    g_stop = true;
                } else
                if (cmd == "start") {
                    if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "cmdThread start");
                    g_stop = false;
                } else
                {
                    if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "bad command `%s'", cmd.c_str());
                }
            }
            const char ack = 'a';
            client.write(&ack, 1);
        } catch (std::exception& e) {
            cybozu::PutLog(cybozu::LogInfo, "cmdThread ERR %s (continue)", e.what());
        }
    }
    if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "cmdThread stop");
} catch (std::exception& e) {
    cybozu::PutLog(cybozu::LogInfo, "cmdThread ERR %s", e.what());
    exit(1);
}

void waitMsec(int msec)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(msec));
}

class Repeater {
    cybozu::Socket s_[2]; // s_[0] : client, s_[1] : server
    enum {
        Sleep = -2,
        Ready = -1,
        Running2 = 2,
        Running1 = 1,
        Running0 = 0
    };
    const int id_;
    const Option& opt_;
    std::atomic<int> state_;
    ThreadRunner threadRunner_[2];
    void loop(int dir)
        try
    {
        if (opt_.verbose) cybozu::PutLog(cybozu::LogInfo, "[%d] loop %d start", id_, dir);
        assert(dir == 0 || dir == 1);
        cybozu::Socket &from = s_[dir];
        cybozu::Socket &to = s_[1 - dir];
        const int intervalSec = 3;
        SMAverage sma(intervalSec);
        std::vector<char> buf(1024);
        while (!g_quit) {
            switch ((int)state_) {
            case Sleep:
                waitMsec(10);
                continue;
            case Ready:
                waitMsec(1);
                continue;
            case Running2:
            case Running1:
                if (!from.isValid()) {
                    waitMsec(1);
                    continue;
                }
                try {
                    while (!g_quit && !from.queryAccept()) {
                    }
                    if (g_quit) continue;
                    size_t readSize = from.readSome(buf.data(), buf.size());
                    if (opt_.rateMbps > 0) {
                        sma.append(readSize, cybozu::GetCurrentTimeSec());
                        while (double rate = sma.getBps(cybozu::GetCurrentTimeSec()) > opt_.rateMbps * 1e6) {
                            if (opt_.verbose) cybozu::PutLog(cybozu::LogDebug, "[%d] loop %d %d rate %f", id_, dir, (int)state_, rate);
                            waitMsec(1);
                        }
                    }
                    if (readSize > 0) {
                        if (!g_stop && to.isValid()) {
                            if (opt_.delaySec) {
                                waitMsec(opt_.delaySec * 1000);
                            }
                            to.write(buf.data(), readSize);
                        }
                    } else {
                        to.shutdown(1, dontThrow); // write disallow
                        from.close();
                        state_--;
                    }
                } catch (std::exception& e) {
                    cybozu::PutLog(cybozu::LogInfo, "[%d] loop %d %d ERR %s", id_, dir, (int)state_, e.what());
                    from.close(dontThrow);
                    state_--;
                }
                break;
            case Running0:
                {
                    int expected = Running0;
                    state_.compare_exchange_strong(expected, Sleep);
                }
                break;
            }
        }
        if (opt_.verbose) cybozu::PutLog(cybozu::LogInfo, "[%d] loop %d end", id_, dir);
    } catch (std::exception& e) {
        cybozu::PutLog(cybozu::LogError, "[%d] caught an error and terminate: %s", id_, e.what());
        exit(1);
    }
public:
    Repeater(const Option& opt, int id)
        : id_(id)
        , opt_(opt)
        , state_(Sleep)
        , threadRunner_()
    {
        for (size_t i = 0; i < 2; i++) {
            threadRunner_[i].set(std::thread(&Repeater::loop, this, i));
        }
    }
    bool tryAndRun(cybozu::Socket& client)
    {
        int expected = Sleep;
        if (!state_.compare_exchange_strong(expected, Ready)) return false;
        try {
            s_[0].moveFrom(client);
            s_[1].connect(opt_.serverAddr, opt_.serverPort, opt_.socketTimeoutS * 1000);
            setSocketTimeout(s_[1], opt_.socketTimeoutS);
            state_ = Running2;
        } catch (std::exception& e) {
            s_[0].close();
            s_[1].close();
            state_ = Sleep;
        }
        return true;
    }
};

int main(int argc, char *argv[])
    try
{
    const Option opt(argc, argv);
    cybozu::Socket server;
    server.bind(opt.recvPort);
    ThreadRunner cmdRunner;
    cmdRunner.set(std::thread(cmdThread, opt));
    std::vector<std::unique_ptr<Repeater>> worker;
    for (size_t i = 0; i < opt.threadNum; i++) {
        worker.emplace_back(new Repeater(opt, (int)i));
    }
    for (;;) {
RETRY:
        while (!g_quit && !server.queryAccept()) {
        }
        if (g_quit) break;
        cybozu::SocketAddr addr;
        cybozu::Socket client;
        server.accept(client, &addr);
        setSocketTimeout(client, opt.socketTimeoutS);
        if (opt.verbose) cybozu::PutLog(cybozu::LogInfo, "accept addr %s", addr.toStr().c_str());
        while (!g_quit) {
            for (size_t i = 0; i < opt.threadNum; i++) {
                if (worker[i]->tryAndRun(client)) {
                    goto RETRY;
                }
            }
            waitMsec(100);
        }
        waitMsec(100);
    }
    if (opt.verbose) puts("main end");
} catch (std::exception& e) {
    cybozu::PutLog(cybozu::LogError, "error: %s", e.what());
    return 1;
}
