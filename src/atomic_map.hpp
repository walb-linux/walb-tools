#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <cassert>
#include "cybozu/exception.hpp"

namespace walb {

template<class Value>
class AtomicMap
{
    std::mutex mu_;
    using Map = std::map<std::string, std::unique_ptr<Value>>;
    using AutoLock = std::lock_guard<std::mutex>;
    Map map_;
public:
    Value& get(const std::string& key) {
        AutoLock al(mu_);
        typename Map::iterator itr;
        itr = map_.find(key);
        if (itr == map_.end()) {
            std::unique_ptr<Value> ptr(new Value(key));
            bool maked;
            std::tie(itr, maked) = map_.emplace(key, std::move(ptr));
            assert(maked);
        }
        return *itr->second;
    }
    // We can not remove instances of Value
    // because we can not ensure uniqueness of Value instance per id.
};

} // walb
