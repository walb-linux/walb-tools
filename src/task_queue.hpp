#pragma once
#include <mutex>
#include <chrono>
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

    using Map = std::map<Task, TimePoint>;
    using Rmap = std::multimap<TimePoint, Task>;

    mutable std::mutex mu_;
    Map map_;
    Rmap rmap_;
    bool isStopped_;

public:
    TaskQueue()
        : mu_(), map_(), rmap_(), isStopped_(false) {
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
    }
    /**
     * Pop a task with the oldest timestamp and the timestamp
     * is not greater than now.
     * RETURN:
     *   false if there is no task satisfying the condition.
     */
    bool pop(Task &task) {
        AutoLock lk(mu_);
        typename Rmap::iterator itr = rmap_.begin();
        if (itr == rmap_.end()) return false;
        if (!isStopped_ && Clock::now() < itr->first) return false;
        task = itr->second;
        rmap_.erase(itr);
        map_.erase(task);
        assert(map_.size() == rmap_.size());
        return true;
    }
    /**
     * Push will do nothing after quit.
     */
    void quit() {
        AutoLock lk(mu_);
        isStopped_ = true;
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
};

} // namespace walb
