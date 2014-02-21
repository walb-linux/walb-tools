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
	using AutoLock = std::lock_guard<std::mutex>;
	mutable std::mutex m_;
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
public:
    StateMachine()
        : inTrans_(false) {
    }
    void addEdge(const std::string &src, const std::string &dst) {
		AutoLock al(m_);
        verifyLocked();
		map_[src].insert(dst);
    }
	void set(const std::string& state) {
		AutoLock al(m_);
        verifyLocked();
		Map::iterator i = map_.find(state);
		if (i == map_.end()) throw cybozu::Exception("StateMachine:setState:not found") << state;
        cur_ = i;
	}
    const std::string &get() const {
		AutoLock al(m_);
		if (cur_ == map_.end()) throw cybozu::Exception("StateMachine:get:not set");
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
public:
    StateMachineTransaction(StateMachine &sm)
		: sm_(sm) {
    }
    ~StateMachineTransaction() noexcept {
        StateMachine::AutoLock al(sm_.m_);
        if (!sm_.inTrans_) return;
        sm_.cur_ = from_;
        sm_.inTrans_ = false;
    }
    bool tryChange(const std::string& from, const std::string& pass) {
        StateMachine::AutoLock al(sm_.m_);
        if (sm_.inTrans_) throw cybozu::Exception("StateMachineTransaction:tryChange:already called");
        from_ = sm_.cur_;
        if (!sm_.changeNoLock(sm_.cur_, from, pass)) return false;
        sm_.inTrans_ = true;
        return true;
    }
    void commit(const std::string& to) {
        StateMachine::AutoLock al(sm_.m_);
        if (!sm_.inTrans_) throw cybozu::Exception("StateMachineTransaction:commit:tryChange is not called");
        if (!sm_.changeNoLock(sm_.cur_, sm_.cur_->first, to)) {
            throw cybozu::Exception("StateMachineTransaction:commit:can't change") << to;
        }
        sm_.inTrans_ = false;
    }
};

} // walb

