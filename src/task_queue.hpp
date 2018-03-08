#pragma once
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <vector>
#include <map>
#include <set>
#include <cassert>

namespace walb {

/**
 * Task must be copyable and have operators "==" and "<".
 */
template <typename Task>
class TaskQueue
{
private:
    using Clock = std::chrono::steady_clock;
    using TimePoint = typename Clock::time_point;
    using MilliSeconds = std::chrono::milliseconds;
    using AutoLock = std::lock_guard<std::mutex>;
    using UniqueLock = std::unique_lock<std::mutex>;

    using Map = std::map<Task, TimePoint>;
    using Rmap = std::multimap<TimePoint, Task>;

    mutable std::mutex mu_;
    mutable std::condition_variable cv_;
    Map map_;
    Rmap rmap_;
    bool isStopped_;

public:
    TaskQueue()
        : mu_(), cv_(), map_(), rmap_(), isStopped_(false) {
    }
    ~TaskQueue() noexcept {
        quit();
    }
    /**
     * Push a task with current time (or with a delay).
     * If the same task already exists in the queue,
     * it will do nothing.
     * After quit, it always do nothing.
     */
    void push(const Task &task, size_t delayMs = 0) {
        AutoLock lk(mu_);
        if (isStopped_) return;
        TimePoint ts = Clock::now() + MilliSeconds(delayMs);
        typename Map::iterator itr;
        bool maked;
        std::tie(itr, maked) = map_.insert(std::make_pair(task, ts));
        if (maked) rmap_.insert(std::make_pair(ts, task));
        assert(map_.size() == rmap_.size());
        cv_.notify_all();
    }
    /**
     * Push a task with a delay.
     * If the smae task already exists,
     * it will be overwritten by the new timestamp.
     * After quit, it always do nothing.
     */
    void pushForce(const Task &task, size_t delayMs) {
        AutoLock lk(mu_);
        if (isStopped_) return;
        TimePoint ts = Clock::now() + MilliSeconds(delayMs);
        typename Map::iterator itr = map_.find(task);
        if (itr != map_.end()) {
            eraseFromRmap(task, itr->second);
            itr->second = ts;
        } else {
            map_[task] = ts;
        }
        rmap_.insert(std::make_pair(ts, task));
        assert(map_.size() == rmap_.size());
        cv_.notify_all();
    }
    /**
     * Pop a task with the oldest timestamp if the timestamp
     * is not greater than now.
     * RETURN:
     *   false if there is no task satisfying the condition.
     */
    bool pop(Task &task, size_t timeoutMs = 0) {
        UniqueLock lk(mu_);
        typename Rmap::iterator itr = rmap_.begin();
        auto canPop = [&](const TimePoint& now) -> bool {
            return itr != rmap_.end() && (isStopped_ || now >= itr->first);
        };
        TimePoint now = Clock::now();
        if (canPop(now)) {
            popInternal(itr, task);
            return true;
        }

        // Reduce timeout to avoid oversleep.
        auto timeout = MilliSeconds(timeoutMs);
        if (itr != rmap_.end()) {
            if (now < itr->first) {
                auto delayToNext = std::chrono::duration_cast<MilliSeconds>(itr->first - now);
                timeout = std::min(delayToNext, timeout);
            }
        }
        cv_.wait_for(lk, timeout);

        itr = rmap_.begin();
        if (canPop(Clock::now())) {
            popInternal(itr, task);
            return true;
        }
        return false;
    }
    /**
     * Push will do nothing after quit.
     */
    void quit() {
        AutoLock lk(mu_);
        isStopped_ = true;
        cv_.notify_all();
    }
    /**
     * Cancel waiting tasks where pred(task) is true.
     */
    template <typename Pred>
    void remove(Pred pred) {
        AutoLock lk(mu_);
        typename Map::iterator itr = map_.begin();
        while (itr != map_.end()) {
            const Task &task = itr->first;
            TimePoint ts = itr->second;
            if (pred(task)) {
                eraseFromRmap(task, ts);
                itr = map_.erase(itr);
            } else {
                ++itr;
            }
        }
        assert(map_.size() == rmap_.size());
        cv_.notify_all();
    }
    /**
     * RETURN:
     *   first: task
     *   second: delay [msec]. Negative value means it should be run.
     */
    std::vector<std::pair<Task, int64_t> > getAll() const {
        TimePoint now = Clock::now();
        AutoLock lk(mu_);
        std::vector<std::pair<Task, int64_t> > ret;
        for (const typename Map::value_type &pair : map_) {
            const int64_t diff = std::chrono::duration_cast<MilliSeconds>(pair.second - now).count();
            ret.push_back(std::make_pair(pair.first, diff));
        }
        return ret;
    }
private:
    void eraseFromRmap(const Task &task, TimePoint ts) {
        typename Rmap::iterator itr, end;
        std::tie(itr, end) = rmap_.equal_range(ts);
        while (itr != end) {
            if (itr->second == task) {
                rmap_.erase(itr);
                break;
            }
            ++itr;
        }
    }
    void popInternal(typename Rmap::iterator itr, Task& task) {
        // lock must be held.
        assert(itr != rmap_.end());
        task = itr->second;
        rmap_.erase(itr);
        map_.erase(task);
        assert(map_.size() == rmap_.size());
    }
};

} // namespace walb
