#pragma once
#include <mutex>
#include <map>
#include <utility>
#include <cassert>
#include "cybozu/exception.hpp"

namespace walb {

class ActionCounterTransaction;

struct ActionCounterItem
{
    int count;
    uint64_t bgn_time; // the latest beginning time. unix time. 0 means empty.
    ActionCounterItem() : count(0), bgn_time(0) {}
};

class ActionCounters
{
private:
    friend ActionCounterTransaction;
    using Map = std::map<std::string, ActionCounterItem>;
    using AutoLock = std::lock_guard<std::recursive_mutex>;
    mutable Map map_;
    std::recursive_mutex &mu_;
public:
    explicit ActionCounters(std::recursive_mutex &mu) : map_(), mu_(mu) {
    }
    /**
     * Get values atomically.
     * C must be Container<std::string> type.
     */
    template <typename C>
    std::vector<int> getValues(const C &nameV) const {
        AutoLock lk(mu_);
        std::vector<int> ret;
        for (const std::string &name : nameV) {
            ret.push_back(get(name)->count);
        }
        return ret;
    }
    int getValue(const std::string &name) const {
        AutoLock lk(mu_);
        return get(name)->count;
    }
    template <typename C>
    bool isAllZero(const C &nameV) const {
        std::vector<int> v = getValues(nameV);
        return std::all_of(v.begin(), v.end(), [](int i) { return i == 0; });
    }
    template <typename C>
    std::vector<ActionCounterItem> getItems(const C &nameV) const {
        AutoLock lk(mu_);
        std::vector<ActionCounterItem> ret;
        for (const std::string &name : nameV) {
            ret.push_back(*get(name));
        }
        return ret;
    }
private:
    const ActionCounterItem *get(const std::string &name) const {
        return &map_[name];
    }
    ActionCounterItem *get(const std::string &name) {
        return &map_[name];
    }
};

class ActionCounterTransaction
{
    std::recursive_mutex &mu_;
    ActionCounterItem *p_;
    bool closed_;
    size_t count_; // first transaction will have 0.
public:
    ActionCounterTransaction(ActionCounters &ac, const std::string &name)
        : mu_(ac.mu_), p_(ac.get(name)), closed_(false), count_(0) {
        std::lock_guard<std::recursive_mutex> lk(mu_);
        count_ = p_->count;
        p_->count++;
        p_->bgn_time = ::time(0);
    }
    void close() {
        if (closed_) return;
        std::lock_guard<std::recursive_mutex> lk(mu_);
        p_->count--;
        closed_ = true;
    }
    ~ActionCounterTransaction() noexcept {
        close();
    }
    size_t count() const { return count_; }
};

} // namespace walb
