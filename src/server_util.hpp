#pragma once
/**
 * @file
 * @brief Utility for server functionality.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <functional>
#include <atomic>
#include <string>
#include "thread_util.hpp"
#include "cybozu/socket.hpp"
#include "file_path.hpp"
#include "walb_logger.hpp"

namespace walb {
namespace server {

enum class ProcessStatus
{
    RUNNING, GRACEFUL_SHUTDOWN, FORCE_SHUTDOWN,
};

/**
 * Multi threaded server.
 */
class MultiThreadedServer
{
private:
    const size_t maxNumThreads_;
    std::atomic<bool> &forceQuit_;

public:
    using RequestWorkerGenerator =
        std::function<std::shared_ptr<cybozu::thread::Runnable>(
            cybozu::Socket &&, std::atomic<ProcessStatus> &)>;

    MultiThreadedServer(std::atomic<bool> &forceQuit, size_t maxNumThreads = 0)
        : maxNumThreads_(maxNumThreads), forceQuit_(forceQuit) {
    }
    void run(uint16_t port, const RequestWorkerGenerator &gen) noexcept {
        forceQuit_ = false;
        cybozu::Socket ssock;
        ssock.bind(port);
        cybozu::thread::ThreadRunnerPool<> pool(maxNumThreads_);
        std::atomic<ProcessStatus> st(ProcessStatus::RUNNING);
        while (st == ProcessStatus::RUNNING) {
            while (!ssock.queryAccept() && st == ProcessStatus::RUNNING) {}
            if (st != ProcessStatus::RUNNING) break;
            cybozu::Socket sock;
            ssock.accept(sock);
            pool.add(gen(std::move(sock), st));
            logErrors(pool.gc());
            //LOGi("pool size %zu", pool.size());
        }
        if (st == ProcessStatus::FORCE_SHUTDOWN) {
            size_t nCanceled = pool.cancelAll();
            forceQuit_ = true;
            LOGi("Canceled %zu tasks.", nCanceled);
        }
        logErrors(pool.gc());
        LOGi("Waiting for %zu remaining tasks...", pool.size());
        logErrors(pool.waitForAll());
    }
private:
    void logErrors(std::vector<std::exception_ptr> &&v) {
        for (std::exception_ptr ep : v) {
            LOGe("REQUEST_WORKER_ERROR:%s", cybozu::thread::exceptionPtrToStr(ep).c_str());
        }
    }
};

/**
 * Request worker for daemons.
 *
 * Override run().
 */
class RequestWorker : public cybozu::thread::Runnable
{
protected:
    cybozu::Socket sock_;
    std::string nodeId_;
    std::atomic<ProcessStatus> &procStat_;
public:
    RequestWorker(cybozu::Socket &&sock, const std::string &nodeId,
                  std::atomic<ProcessStatus> &procStat)
        : sock_(std::move(sock))
        , nodeId_(nodeId)
        , procStat_(procStat) {}
    void operator()() noexcept override try {
        run();
        sock_.close();
        done();
    } catch (...) {
        throwErrorLater();
        sock_.close();
    }
    virtual void run() = 0;
};

}} //namespace walb::server
