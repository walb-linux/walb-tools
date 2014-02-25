#pragma once
#include <mutex>
#include <map>
#include <utility>
#include <cassert>
#include "cybozu/exception.hpp"

namespace walb {

class ActionCounterTransaction;

class ActionCounters
{
private:
    friend ActionCounterTransaction;
    using Map = std::map<std::string, int>;
    using AutoLock = std::lock_guard<std::recursive_mutex>;
    mutable Map map_;
    std::recursive_mutex &mu_;
public:
    explicit ActionCounters(std::recursive_mutex &mu) : map_(), mu_(mu) {
    }
    /**
     * Get values atomically.
     */
    std::vector<int> getValues(const std::vector<std::string> &nameV) const {
        AutoLock lk(mu_);
        std::vector<int> ret;
        for (const std::string &name : nameV) {
            ret.push_back(*get(name));
        }
        return ret;
    }
    int getValue(const std::string &name) const {
        AutoLock lk(mu_);
        return *get(name);
    }
    bool isAllZero(const std::vector<std::string> &nameV) const {
        std::vector<int> v = getValues(nameV);
        return std::all_of(v.begin(), v.end(), [](int i) { return i == 0; });
    }
private:
    const int *get(const std::string &name) const {
        return &map_[name];
    }
    int *get(const std::string &name) {
        return &map_[name];
    }
};

class ActionCounterTransaction
{
    std::recursive_mutex &mu_;
    int *p_;
public:
    ActionCounterTransaction(ActionCounters &ac, const std::string &name)
        : mu_(ac.mu_), p_(ac.get(name)) {
        std::lock_guard<std::recursive_mutex> lk(mu_);
        (*p_)++;
    }
    ~ActionCounterTransaction() noexcept {
        std::lock_guard<std::recursive_mutex> lk(mu_);
        (*p_)--;
    }
};

} // namespace walb
