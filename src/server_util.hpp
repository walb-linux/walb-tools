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
#include <signal.h>
#include "thread_util.hpp"
#include "cybozu/socket.hpp"
#include "file_path.hpp"
#include "walb_logger.hpp"
#include "constant.hpp"
#include "walb_util.hpp"
#include "counter.hpp"
#include "meta.hpp"
#include "protocol.hpp"

namespace walb {
namespace server {

/**
 * Multi threaded server.
 */
class MultiThreadedServer
{
private:
    static ProcessStatus *pps_;
    static inline void quitHandler(int) noexcept
    {
        if (pps_) {
            pps_->setForceShutdown();
        }
    }
    void setQuitHandler()
    {
        struct sigaction sa;
        sa.sa_handler = &quitHandler;
        sigfillset(&sa.sa_mask);
        sa.sa_flags = 0;
        bool isOK = (sigaction(SIGINT, &sa, NULL) == 0)
            && (sigaction(SIGQUIT, &sa, NULL) == 0)
            && (sigaction(SIGABRT, &sa, NULL) == 0)
            && (sigaction(SIGTERM, &sa, NULL) == 0);
		if (!isOK) {
            LOGs.error() << "can't set sigaction";
			exit(1);
		}
    }

public:
    void run(ProcessStatus &ps, uint16_t port, const std::string& nodeId, const protocol::Str2ServerHandler& handlers, size_t maxNumThreads = 0, size_t socketTimeout = 10) {
        const char *const FUNC = __func__;
        pps_ = &ps;
        setQuitHandler();
        cybozu::Socket ssock;
        ssock.bind(port);
        cybozu::thread::ThreadRunnerFixedPool pool;
        pool.start(maxNumThreads);
        LOGs.info() << FUNC << "Ready to accept connections";
        for (;;) {
            for (;;) {
                if (!ps.isRunning()) goto quit;
                int ret = ssock.queryAcceptNoThrow();
                if (ret > 0) break; // accepted
                if (ret == 0) continue; // timeout
                if (ret == -EINTR) {
                    LOGs.info() << FUNC << "queryAccept:interrupted";
                    goto quit;
                }
                throw cybozu::Exception(FUNC) << "queryAccept" << cybozu::NetErrorNo(-ret);
            }
            cybozu::Socket sock;
            ssock.accept(sock);
            sock.setSendTimeout(socketTimeout * 1000);
            sock.setReceiveTimeout(socketTimeout * 1000);
            logErrors(pool.gc());
            if (!pool.add(protocol::RequestWorker(std::move(sock), nodeId, ps, handlers))) {
                LOGs.warn() << FUNC << "Exceeds max concurrency" <<  maxNumThreads;
                // The socket will be closed.
            }
        }
    quit:
        LOGs.info() << FUNC << "Waiting for remaining tasks";
        pool.stop();
        logErrors(pool.gc());
    }
private:
    void logErrors(std::vector<std::exception_ptr> &&v) {
        for (std::exception_ptr ep : v) {
            LOGs.error()
                << "REQUEST_WORKER_ERROR"
                << cybozu::thread::exceptionPtrToStr(ep);
        }
    }
};

ProcessStatus *MultiThreadedServer::pps_;

} //namespace server

enum StopState {
    NotStopping = 1,
    WaitingForEmpty = 2,
    Stopping = 4,
    ForceStopping = 8
};

inline const char* stopStateToStr(StopState st)
{
    switch (st) {
    case NotStopping:
        return "NotStopping";
    case WaitingForEmpty:
        return "WAitingForEmpty";
    case Stopping:
        return "Stopping";
    case ForceStopping:
        return "ForceStopping";
    default:
        throw cybozu::Exception(__func__) << "bad state" << st;
    }
}

class Stopper
{
private:
    std::atomic<int> &stopState;
public:
    Stopper(std::atomic<int> &stopState)
        : stopState(stopState) {
    }
    bool changeFromNotStopping(StopState toState) {
        if (toState == NotStopping) throw cybozu::Exception("Stopper:changeFromNotStopping:bad state") << toState;
        int fromState = NotStopping;
        return stopState.compare_exchange_strong(fromState, toState);
    }
    bool changeFromWaitingForEmpty(StopState toState) {
        if (toState != Stopping && toState != ForceStopping) throw cybozu::Exception("Stopper:changeFromWaitingForEmpty:bad state") << toState;
        int fromState = WaitingForEmpty;
        return stopState.compare_exchange_strong(fromState, toState);
    }
    ~Stopper() noexcept {
        stopState = NotStopping;
    }
};

/**
 * Wait until pred() becomes true.
 * Mutex must be locked at entering the function and it will be locked at exiting it.
 *
 * @mu lock such as std::unique_lock<std::mutex> and std::lock_guard<std::mutex>.
 * @pred predicate. waitUntil will wait until pred() becomes true.
 * @msg error message prefix.
 * @timeout timeout [sec]. 0 means no timeout.
 */
template <typename Mutex, typename Pred>
void waitUntil(Mutex &mu, Pred pred, const char *msg, size_t timeout = DEFAULT_TIMEOUT_SEC)
{
    for (size_t c = 0; timeout == 0 || c < timeout; c++) {
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
 * Worker must have Worker(const Task &),
 * and void operator()().
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
    void logErrors(const std::vector<std::exception_ptr> &v) const {
        for (const std::exception_ptr &ep : v) {
            LOGs.error() << cybozu::thread::exceptionPtrToStr(ep);
        }
    }
    void operator()() noexcept try {
        LOGs.info() << "dispatchTask begin";
        cybozu::thread::ThreadRunnerFixedPool pool;
        pool.start(maxBackgroundTasks);
        std::queue<Task> taskQ;
        while (!shouldStop) {
            LOGs.debug() << "dispatchTask nrRunning" << pool.nrRunning();
            logErrors(pool.gc());
            if (taskQ.empty()) {
                Task task;
                if (!tq.pop(task)) {
                    util::sleepMs(1000);
                    continue;
                }
                LOGs.debug() << "dispatchTask pop" << task;
                taskQ.push(task);
            }
            Task &task = taskQ.front();
            if (!pool.add(Worker(task))) {
                util::sleepMs(1000);
                continue;
            }
            LOGs.debug() << "dispatchTask dispatch task" << task;
            taskQ.pop();
        }
        pool.stop();
        logErrors(pool.gc());
        LOGs.info() << "dispatchTask end";
    } catch (std::exception &e) {
        LOGs.error() << "dispatchTask" << e.what();
        ::exit(1);
    } catch (...) {
        LOGe("dispatchTask:unknown error");
        ::exit(1);
    }
};

inline void verifyStopState(
    const std::atomic<int> &stopState, int acceptState, const std::string& volId, const char *msg)
{
    int st = stopState;
    if ((st & acceptState) == 0) {
        throw cybozu::Exception(msg) << __func__ << volId << st << acceptState;
    }
}

inline void verifyNotStopping(const std::atomic<int> & stopState, const std::string& volId, const char *msg)
{
    verifyStopState(stopState, NotStopping, volId, msg);
}

/**
 * RETURN:
 *   true if a specified state is found in a specified list.
 */
inline bool isStateIn(const std::string &state, const StrVec &v)
{
    for (const std::string &st : v) {
        if (state == st) return true;
    }
    return false;
}

inline void verifyStateIn(const std::string &state, const StrVec &v, const char *msg)
{
    if (!isStateIn(state, v)) {
        throw cybozu::Exception(msg) << "bad state" << state;
    }
}

/**
 * C must be Container<std::string> type.
 */
template <typename C>
void verifyActionNotRunning(const ActionCounters& ac, const C& actions, const char *msg)
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

const int ForegroundCounterType = 0;
using ForegroundCounterTransaction = counter::CounterTransaction<ForegroundCounterType>;

inline void verifyMaxForegroundTasks(size_t maxForegroundTasks, const char *msg)
{
    if (counter::getCounter<ForegroundCounterType>() > maxForegroundTasks) {
        throw cybozu::Exception(msg)
            << "exceeds max foreground tasks" << maxForegroundTasks;
    }
}

inline std::string formatActions(const char *prefix, ActionCounters &ac, const StrVec &actionV)
{
    const std::vector<int> numV = ac.getValues(actionV);
    std::string ret(prefix);
    for (size_t i = 0; i < actionV.size(); i++) {
        ret += cybozu::util::formatString(" %s %d", actionV[i].c_str(), numV[i]);
    }
    return ret;
}

/**
 * C must be Container<std::string> type.
 */
template <typename C>
inline int getTotalNumActions(ActionCounters &ac, const C &actions)
{
    const std::vector<int> v = ac.getValues(actions);
    int total = 0;
    for (int i : v) total += i;
    return total;
}

inline std::string formatMetaDiff(const char *prefix, const MetaDiff &diff)
{
    return cybozu::util::formatString(
        "%s%s %c%c %s %" PRIu64 ""
        , prefix, diff.str().c_str()
        , diff.isMergeable ? 'M' : '-'
        , diff.isCompDiff ? 'C' : '-'
        , util::timeToPrintable(diff.timestamp).c_str()
        , diff.dataSize);
}

inline std::string createLogFilePath(const std::string &fileStr, const std::string &baseDirStr)
{
    if (fileStr == "-") return fileStr;
    if (cybozu::FilePath(fileStr).isFull()) return fileStr;
    return (cybozu::FilePath(baseDirStr) + fileStr).str();

}

} //namespace walb
