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

enum class ControlFlag
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

public:
    using RequestWorkerGenerator =
        std::function<std::shared_ptr<cybozu::thread::Runnable>(
            cybozu::Socket &&, const std::atomic<bool> &, std::atomic<ControlFlag> &)>;

    explicit MultiThreadedServer(size_t maxNumThreads = 0)
        : maxNumThreads_(maxNumThreads) {}
    void run(uint16_t port, const RequestWorkerGenerator &gen) noexcept {
        cybozu::Socket ssock;
        ssock.bind(port);
        cybozu::thread::ThreadRunnerPool pool(maxNumThreads_);
        std::atomic<ControlFlag> flag(ControlFlag::RUNNING);
        std::atomic<bool> forceQuit(false);
        while (flag == ControlFlag::RUNNING) {
            while (!ssock.queryAccept()) {}
            cybozu::Socket sock;
            ssock.accept(sock);
            pool.add(gen(std::move(sock), forceQuit, flag));
            logErrors(pool.gc());
            //LOGi("pool size %zu", pool.size());
        }
        if (flag == ControlFlag::FORCE_SHUTDOWN) {
            size_t nCanceled = pool.cancelAll();
            forceQuit = true;
            LOGi("Canceled %zu tasks.", nCanceled);
        }
        logErrors(pool.gc());
        LOGi("Waiting for %zu remaining tasks...", pool.size());
        logErrors(pool.waitForAll());
    }
private:
    void logErrors(std::vector<std::exception_ptr> &&v) {
        for (std::exception_ptr ep : v) {
            try {
                std::rethrow_exception(ep);
            } catch (std::exception &e) {
                LOGe("REQUEST_WORKER_ERROR: %s.", e.what());
            } catch (...) {
                LOGe("REQUEST_WORKER_ERROR: an unknown error.");
            }
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
    std::string serverId_;
    std::string baseDirStr_;
    const std::atomic<bool> &forceQuit_;
    std::atomic<ControlFlag> &ctrlFlag_;
public:
    RequestWorker(cybozu::Socket &&sock, const std::string &serverId,
                  const std::string &baseDirStr,
                  const std::atomic<bool> &forceQuit,
                  std::atomic<ControlFlag> &ctrlFlag)
        : sock_(std::move(sock))
        , serverId_(serverId)
        , baseDirStr_(baseDirStr)
        , forceQuit_(forceQuit)
        , ctrlFlag_(ctrlFlag) {}
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
