#pragma once
#include <mutex>
#include <chrono>
#include <map>
#include <set>

namespace walb {

template <typename Task>
class RunTask;

/**
 * Task class must have
 *   - default  constructor.
 *   - copy constructor and assignment operator.
 *   - equality operators.
 */
template <typename Task>
class TaskQueue
{
private:
    friend RunTask<Task>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = typename Clock::time_point;
    using Seconds = std::chrono::seconds;
    using AutoLock = std::lock_guard<std::mutex>;

    struct TaskPlus {
        Task task;
        TimePoint startTime;

        void setStartTime(size_t delaySec) {
            startTime = Clock::now() + Seconds(delaySec);
        }
    };

    using Mmap = std::multimap<TimePoint, TaskPlus>;

    std::mutex mu_;
    Mmap waiting_;
    Mmap running_;
    bool isStopped_;

public:
    /**
     * Add a task.
     * RETURN:
     *   false if already the same task is in the waiting queue.
     */
    bool add(const Task &task, size_t delaySec = 0) {
        // QQQ
        return false;
    }
    /**
     * Get a top priority task to run.
     * RETURN:
     *   false if there is no task.
     */
    bool get(Task &task) {
        // QQQ
        return false;
    }
    /**
     * Stop the queue.
     * During stopped, add() and get() always fail.
     */
    void stop() {
        AutoLock lk(mu_);
        if (isStopped_) return;
        // QQQ
    }
    /**
     * Start the queue.
     */
    void start() {
        AutoLock lk(mu_);
        if (!isStopped_) return;
        // QQQ
    }
    /**
     * Cancel waiting tasks where pred(task) is true.
     */
    template <typename Pred>
    void cancelPred(Pred pred) {
        AutoLock lk(mu_);
        // QQQ
    }
    /**
     * Cancel all waiting tasks.
     */
    void cancelAll() {
        AutoLock lk(mu_);
        // QQQ
    }
};

/**
 * RAII for starting tasks and erasing them from task queues.
 * This is used by task dispatcher.
 */
template <typename Task>
class RunTask
{
private:
    TaskQueue<Task> &tq_;
    Task task_;

public:
    RunTask(TaskQueue<Task> &tq, const Task &task) : tq_(tq), Task(task) {
        // move the task from the waiting queue to the running queue.
        // QQQ
    }
    ~RunTask() noexcept {
        // delete the task from the running queue.
        // QQQ
    }
};

} // namespace walb
