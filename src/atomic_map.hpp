#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <cassert>
#include <vector>
#include <string>
#include "cybozu/exception.hpp"

namespace walb {

template<class Value>
class AtomicMap
{
    mutable std::mutex mu_;
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
    std::vector<std::string> getKeyList() const {
        AutoLock al(mu_);
        std::vector<std::string> ret;
        for (const typename Map::value_type &p : map_) {
            ret.push_back(p.first);
        }
        return ret;
    }
    // We can not remove instances of Value
    // because we can not ensure uniqueness of Value instance per id.
};

} // walb
