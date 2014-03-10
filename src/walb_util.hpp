#pragma once
/**
 * @file
 * @brief File utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include <atomic>
#include <chrono>
#include <thread>
#include "util.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"
#include "constant.hpp"
#include "task_queue.hpp"
#include "action_counter.hpp"
#include "thread_util.hpp"
#include "walb_logger.hpp"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
#include "cybozu/atoi.hpp"
#include "cybozu/itoa.hpp"
#include "cybozu/log.hpp"
#include "cybozu/file.hpp"
#include "cybozu/serializer.hpp"

namespace walb {

typedef std::vector<std::string> StrVec;
typedef std::unique_lock<std::recursive_mutex> UniqueLock;

enum StopState {
    NotStopping,
    Stopping,
    ForceStopping
};

namespace util {

/**
 * Make a directory.
 *
 * If not exists, make a specified directory.
   If exists,
 *   ensureNotExistance == false
 *     check the directory existance.
 *   ensureNotExistance == true
 *     throw an error.
 */
void makeDir(const std::string &dirStr, const char *msg,
             bool ensureNotExistance = false)
{
    cybozu::FilePath dir(dirStr);
    if (dir.stat().exists()) {
        if (ensureNotExistance) {
            throw cybozu::Exception(msg) << "already exists" << dirStr;
        } else {
            if (dir.stat().isDirectory()) {
                return;
            } else {
                throw cybozu::Exception(msg) << "not directory" << dirStr;
            }
        }
    }
    if (!dir.mkdir()) {
        throw cybozu::Exception(msg) << "mkdir failed" << dirStr;
    }
}

inline std::vector<std::string> getFileNameList(const std::string &dirStr, const char *ext)
{
    std::vector<std::string> ret;
    std::vector<cybozu::FileInfo> list = cybozu::GetFileList(dirStr, ext);
    for (cybozu::FileInfo &info : list) {
        if (info.name == "." || info.name == ".." || !info.isFile)
            continue;
        ret.push_back(info.name);
    }
    return ret;
}

template <typename T>
void saveFile(const cybozu::FilePath &dir, const std::string &fname, const T &t)
{
    cybozu::TmpFile tmp(dir.str());
    cybozu::save(tmp, t);
    tmp.save((dir + fname).str());
}

template <typename T>
void loadFile(const cybozu::FilePath &dir, const std::string &fname, T &t)
{
    cybozu::util::FileReader r((dir + fname).str(), O_RDONLY);
    cybozu::load(t, r);
}

inline cybozu::SocketAddr parseSocketAddr(const std::string &addrPort)
{
    const StrVec v = cybozu::Split(addrPort, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception("parseSocketAddr:parse error") << addrPort;
    }
    return cybozu::SocketAddr(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}

inline std::vector<cybozu::SocketAddr> parseMultiSocketAddr(const std::string &multiAddrPort)
{
    std::vector<cybozu::SocketAddr> ret;
    const StrVec v = cybozu::Split(multiAddrPort, ',');
    for (const std::string &addrPort : v) {
        ret.emplace_back(parseSocketAddr(addrPort));
    }
    return ret;
}

inline void setLogSetting(const std::string &pathStr, bool isDebug)
{
    if (pathStr == "-") {
        cybozu::SetLogFILE(::stderr);
    } else {
        cybozu::OpenLogFile(pathStr);
    }
    if (isDebug) {
        cybozu::SetLogPriority(cybozu::LogDebug);
    } else {
        cybozu::SetLogPriority(cybozu::LogInfo);
    }
}

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

inline void sleepMs(size_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/**
 * return when pred() is true.
 */
template <typename Mutex, typename Pred>
void waitUntil(Mutex &mu, Pred pred, const char *msg, size_t timeout = DEFAULT_TIMEOUT)
{
    for (size_t c = 0; c < timeout; c++) {
        if (pred()) return;
        mu.unlock();
        sleepMs(1000);
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
                LOGe("%s", cybozu::thread::exceptionPtrToStr(ep).c_str());
            }
        }
        for (std::exception_ptr ep : pool.waitForAll()) {
            LOGe("%s", cybozu::thread::exceptionPtrToStr(ep).c_str());
        }
    } catch (std::exception &e) {
        LOGe("dispatchTask:%s", e.what());
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

/**
 * Hex string.
 */
inline std::string binaryToStr(const void *data, size_t size)
{
    std::string s;
    const uint8_t *p = static_cast<const uint8_t *>(data);
    s.resize(size * 2);
    for (size_t i = 0; i < size; i++) {
        cybozu::itohex(&s[i * 2], 2, p[i], false);
    }
    return s;
}

}} // walb::util
