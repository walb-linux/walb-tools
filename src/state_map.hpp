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
    State& get(const std::string& volId)
    {
        AutoLock al(mu_);
        bool maked;
        typename Map::iterator itr;
        std::tie(itr, maked) = map_.insert(std::make_pair(volId, nullptr));
        if (maked) {
            itr->second = std::unique_ptr<State>(new State(volId));
        }
        return *itr->second;
    }

    // We can not remove instances of State
    // because we can not ensure uniqueness of state instance per volId.
};

} // walb
