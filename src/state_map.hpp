#pragma once
#include <map>
#include <memory>
#include <mutex>
#include "cybozu/exception.hpp"

namespace walb {

template<class State>
class StateMap {
    std::mutex mu_;
    using Map = std::map<std::string, std::unique_ptr<State>>;
    using AutoLock = std::lock_guard<std::mutex>;
    Map map_;
public:
    std::pair<State *, bool> get(const std::string& volId)
    {
        AutoLock al(mu_);
        bool maked;
        typename Map::iterator itr;
        std::tie(itr, maked) = map_.insert(std::make_pair(volId, nullptr));
        if (maked) {
            itr->second = std::unique_ptr<State>(new State());
        }
        return std::make_pair(itr->second.get(), maked);
    }
    std::unique_ptr<State> del(const std::string& volId)
    {
        AutoLock al(mu_);
        typename Map::iterator i = map_.find(volId);
        if (i == map_.end()) throw cybozu::Exception("StateMap:del:not found") << volId;
        std::unique_ptr<State> p = std::move(i->second);
        map_.erase(i);
        return p;
    }
};

} // walb

