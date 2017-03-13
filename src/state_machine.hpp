#pragma once
#include <map>
#include <set>
#include <string>
#include <mutex>
#include <assert.h>
#include "cybozu/exception.hpp"

namespace walb {

class StateMachineTransaction;

class StateMachine
{
    friend StateMachineTransaction;
    using StrSet = std::set<std::string>;
    using Map = std::map<std::string, StrSet>;
    using AutoLock = std::lock_guard<std::recursive_mutex>;
    std::recursive_mutex &m_;
    Map map_;
    Map::iterator cur_;
    bool inTrans_; // true if in [tryChange, commit or transaction.dstr]
    bool changeNoLock(Map::iterator& cur, const std::string& from, const std::string& to) {
        if (cur->first != from) return false;
        if (cur->second.find(to) == cur->second.end()) return false;
        cur = map_.find(to);
        assert(cur != map_.end());
        return true;
    }
    void verifyLocked() const {
        if (inTrans_) throw cybozu::Exception("StateMachine:locked");
    }
    void addEdgeNoLock(const std::string &src, const std::string &dst) {
        map_[src].insert(dst);
        map_.insert(std::make_pair(dst, StrSet()));
    }
public:
    explicit StateMachine(std::recursive_mutex& m)
        : m_(m)
        , cur_(map_.end())
        , inTrans_(false) {
    }
    void addEdge(const std::string &src, const std::string &dst) {
        AutoLock al(m_);
        verifyLocked();
        addEdgeNoLock(src, dst);
    }
    struct Pair {
        const char *from;
        const char *to;
    };
    template<size_t N>
    void init(const Pair (&tbl)[N]) {
        AutoLock al(m_);
        verifyLocked();
        for (size_t i = 0; i < N; i++) {
            addEdgeNoLock(tbl[i].from, tbl[i].to);
        }
    }
    void set(const std::string& state) {
        AutoLock al(m_);
        verifyLocked();
        Map::iterator i = map_.find(state);
        if (i == map_.end()) throw cybozu::Exception("StateMachine:setState:not found") << state;
        cur_ = i;
    }
    const std::string &get(bool doThrow = true) const {
        static const std::string noState("NO_STATE");
        AutoLock al(m_);
        if (cur_ == map_.end()) {
            if (doThrow) {
                throw cybozu::Exception("StateMachine:get:not set");
            } else {
                return noState;
            }
        }
        return cur_->first;
    }
    bool change(const std::string& from, const std::string& to) {
        AutoLock al(m_);
        verifyLocked();
        return changeNoLock(cur_, from, to);
    }
};

class StateMachineTransaction
{
private:
    StateMachine &sm_;
    StateMachine::Map::iterator from_;
    std::string errMsg_;
    bool calledTryChange_;
public:
    explicit StateMachineTransaction(StateMachine &sm)
        : sm_(sm), calledTryChange_(false) {
    }
    ~StateMachineTransaction() noexcept {
        StateMachine::AutoLock al(sm_.m_);
        if (!sm_.inTrans_) return;
        if (!calledTryChange_) return;
        sm_.cur_ = from_;
        sm_.inTrans_ = false;
    }
    StateMachineTransaction(StateMachine &sm, const std::string& from, const std::string& to, const std::string &errMsg = "")
        : sm_(sm), errMsg_(errMsg), calledTryChange_(false) {
        if (!tryChange(from, to)) {
            throw cybozu::Exception(errMsg)
                << "StateMachineTransaction:bad state"
                << sm.get(false) << from << to;
        }
    }
    bool tryChange(const std::string& from, const std::string& pass) {
        StateMachine::AutoLock al(sm_.m_);
        if (sm_.inTrans_) {
            throw cybozu::Exception(errMsg_)
                << "StateMachineTransaction:tryChange:already called"
                << sm_.get(false) << from << pass;
        }
        from_ = sm_.cur_;
        if (!sm_.changeNoLock(sm_.cur_, from, pass)) return false;
        sm_.inTrans_ = true;
        calledTryChange_ = true;
        return true;
    }
    void commit(const std::string& to) {
        StateMachine::AutoLock al(sm_.m_);
        if (!sm_.inTrans_) {
            throw cybozu::Exception(errMsg_)
                << "StateMachineTransaction:commit:tryChange is not called"
                << sm_.get(false) << to;
        }
        if (!sm_.changeNoLock(sm_.cur_, sm_.cur_->first, to)) {
            throw cybozu::Exception(errMsg_)
                << "StateMachineTransaction:commit:can't change" << to;
        }
        sm_.inTrans_ = false;
    }
    void setErrorMessage(const std::string &msg) {
        errMsg_ = msg;
    }
};

} // walb
