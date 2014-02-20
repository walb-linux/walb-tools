#pragma once
#include <mutex>
#include <map>
#include <utility>
#include <cassert>
#include "cybozu/exception.hpp"

namespace walb {

/**
 * Usage:
 *
 * <pre>
 * std::mutex mu;
 * RaiiCounter c;
 * c.setMutex(&mu);
 *
 * // thread A
 * {
 *   std::lock_guard<RaiiCounter> lk(c); // c is incremented.
 *   // do task
 * }
 * // c is deremented.
 *
 * // thread B
 * {
 *   std::lock_guard<std::mutex> lk(mu);
 *   c.get()
 * }
 * </pre>
 */
class RaiiCounter
{
private:
    std::mutex *muP_;
    int c_;

public:
    RaiiCounter() : muP_(nullptr), c_(0) {
    }
    /**
     * muP: Shared lock.
     */
    void setMutex(std::mutex *muP) {
        muP_ = muP;
        check();
    }
    void reset() {
        check();
        std::lock_guard<std::mutex> lk(*muP_);
        c_ = 0;
    }
    void lock() {
        check();
        std::lock_guard<std::mutex> lk(*muP_);
        ++c_;
    }
    void unlock() {
        check();
        std::lock_guard<std::mutex> lk(*muP_);
        --c_;
    }
    /**
     * You must lock the mutex by yourself to call get().
     */
    int get() const {
        return c_;
    }
private:
    void check() const {
        if (!muP_) {
            throw cybozu::Exception("Counter:muP_ is null");
        }
    }
};

class MultiRaiiCounter
{
private:
    using Map = std::map<std::string, RaiiCounter>;
    Map map_;
    std::mutex &mu_;
public:
    explicit MultiRaiiCounter(std::mutex &mu) : map_(), mu_(mu) {
    }
    /**
     * Increment the counter indicated by a specified name.
     * RETURN:
     *   Returned value's destructor will decrement the counter.
     */
    std::unique_lock<RaiiCounter> getLock(const std::string &name) {
        RaiiCounter *p;
        {
            std::lock_guard<std::mutex> lk(mu_);
            p = get(name);
        }
        assert(p);
        return std::unique_lock<RaiiCounter>(*p);
    }
    /**
     * Get values atomically.
     */
    std::vector<int> getValues(const std::vector<std::string> &nameV) {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<int> ret;
        for (const std::string &name : nameV) {
            ret.push_back(get(name)->get());
        }
        return ret;
    }
private:
    RaiiCounter *get(const std::string &name) {
        typename Map::iterator it;
        it = map_.find(name);
        if (it == map_.end()) {
            bool maked;
            std::tie(it, maked) = map_.emplace(name, RaiiCounter());
            if (!maked) {
                throw cybozu::Exception("MultiRaiiCounter::get:map emplace failed.");
            }
            it->second.setMutex(&mu_);
        }
        return &it->second;
    }
};

} // namespace walb
