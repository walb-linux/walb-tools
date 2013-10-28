#pragma once
/**
 * @file
 * @brief Utility for server functionality.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "sys_logger.hpp"

#include <memory>
#include <functional>
#include <atomic>
#include "thread_util.hpp"
#include "cybozu/socket.hpp"

namespace cybozu {
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

}} //namespace cybozu::server
