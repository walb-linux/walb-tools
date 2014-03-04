#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <cassert>
#include "cybozu/exception.hpp"

namespace walb {

template<class State>
class AtomicMap {
    std::mutex mu_;
    using Map = std::map<std::string, std::unique_ptr<State>>;
    using AutoLock = std::lock_guard<std::mutex>;
    Map map_;
public:
    State& get(const std::string& id)
    {
        AutoLock al(mu_);
        typename Map::iterator itr;
        itr = map_.find(id);
        if (itr == map_.end()) {
            std::unique_ptr<State> ptr(new State(id));
            bool maked;
            std::tie(itr, maked) = map_.emplace(id, std::move(ptr));
            assert(maked);
        }
        return *itr->second;
    }

    // We can not remove instances of State
    // because we can not ensure uniqueness of state instance per id.
};

} // walb
