#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <cassert>
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
        typename Map::iterator itr;
        itr = map_.find(volId);
        if (itr == map_.end()) {
            std::unique_ptr<State> ptr(new State(volId));
            bool maked;
            std::tie(itr, maked) = map_.emplace(volId, std::move(ptr));
            assert(maked);
        }
        assert(itr->second);
        return *itr->second;
    }

    // We can not remove instances of State
    // because we can not ensure uniqueness of state instance per volId.
};

} // walb
