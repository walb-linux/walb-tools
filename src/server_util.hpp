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
#include "constant.hpp"
#include "walb_util.hpp"

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

} //namespace server

enum StopState {
    NotStopping,
    Stopping,
    ForceStopping
};

class Stopper
{
private:
    std::atomic<int> &stopState;
    bool success;
public:
    Stopper(std::atomic<int> &stopState, bool isForce)
        : stopState(stopState), success(false) {
        int before = NotStopping;
        const int after = isForce ? ForceStopping : Stopping;
        success = stopState.compare_exchange_strong(before, after);
    }
    ~Stopper() noexcept {
        if (success) {
            assert(stopState != NotStopping);
            stopState = NotStopping;
        }
    }
    bool isSuccess() const {
        return success;
    }
};

/**
 * return when pred() is true.
 */
template <typename Mutex, typename Pred>
void waitUntil(Mutex &mu, Pred pred, const char *msg, size_t timeout = DEFAULT_TIMEOUT)
{
    for (size_t c = 0; c < timeout; c++) {
        if (pred()) return;
        mu.unlock();
        util::sleepMs(1000);
        mu.lock();
    }
    throw cybozu::Exception(msg) << "timeout" << timeout;
}

/**
 * This instance starts a worker thread in the constructor,
 * and joins it in the destructor.
 *
 * The worker thread will pop tasks from a task queue and
 * run them using a thread pool.
 * Number of concurrent running tasks will be limited by
 * maxBackgroundTasks parameter.
 *
 * User can specify Task data and Worker function object.
 *
 * Task must satisfy TaskQueue constraint.
 * See TaskQueue definition.
 *
 * Worker must have constructor of type (*)(const Task &),
 * and operator()() of type void (*)().
 */
template <typename Task, typename Worker>
class DispatchTask
{
private:
    std::atomic<bool> shouldStop;
    TaskQueue<Task> &tq;
    size_t maxBackgroundTasks;
    std::thread th;

public:
    DispatchTask(TaskQueue<Task> &tq,
                 size_t maxBackgroundTasks)
        : shouldStop(false)
        , tq(tq)
        , maxBackgroundTasks(maxBackgroundTasks)
        , th(std::ref(*this)) {
    }
    ~DispatchTask() noexcept {
        shouldStop = true;
        th.join();
    }
    void operator()() noexcept try {
        cybozu::thread::ThreadRunnerPool<Worker> pool(maxBackgroundTasks);
        Task task;
        while (!shouldStop) {
            bool doWait = false;
            if (pool.getNumActiveThreads() >= maxBackgroundTasks) {
                doWait = true;
            }
            if (!doWait) {
                doWait = !tq.pop(task);
            }
            if (doWait) {
                util::sleepMs(1000);
                continue;
            }
            pool.add(std::make_shared<Worker>(task));
            for (std::exception_ptr ep : pool.gc()) {
                LOGs.error() << cybozu::thread::exceptionPtrToStr(ep);
            }
        }
        for (std::exception_ptr ep : pool.waitForAll()) {
            LOGs.error() << cybozu::thread::exceptionPtrToStr(ep);
        }
    } catch (std::exception &e) {
        LOGs.error() << "dispatchTask" << e.what();
        ::exit(1);
    } catch (...) {
        LOGe("dispatchTask:other error");
        ::exit(1);
    }
};

inline void verifyNotStopping(
    const std::atomic<int> &stopState,
    const std::string &volId, const char *msg)
{
    int st = stopState.load();
    if (st != NotStopping) {
        cybozu::Exception e(msg);
        e << "must be NotStopping" << volId << st;
        throw e;
    }
}

/**
 * C must be Container<std::string> type.
 */
template <typename C>
void verifyNoActionRunning(const ActionCounters& ac, const C& actions, const char *msg)
{
    std::vector<int> v = ac.getValues(actions);
    assert(v.size() == actions.size());
    for (size_t i = 0; i < v.size(); i++) {
        if (v[i] != 0) {
            typename C::const_iterator itr = actions.begin();
            std::advance(itr, i);
            throw cybozu::Exception(msg)
                 << "there are running action"
                 << *itr << v[i];
        }
    }
}

} //namespace walb
