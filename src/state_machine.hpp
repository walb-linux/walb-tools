#pragma once
#include <map>
#include <string>

class StateMachineTransaction
{
private:
    StateMachine &sm_;
    std::string prev_;
    std::string curr_;
public:
    StateMachineTransaction(StateMachine &sm) : sm_(sm) {
    }
    ~StateMachineTransaction() noexcept {
        // If commit has not been called, go back the previous state.
    }
    bool tryChange(const std::string &) {
        // check the currrent state and save it to prev_.
        // check the transition is possible.
        // change state to the specified state and save it to curr_.
        return false;
    }
    void commit(const std::string &) {
        // check the transition from curr_ to the specified state.
        // change state to the specified state.
    }
};

class StateMachine
{
private:
    std::map<std::string, std::vector<std::string> > map_;
public:
    void addEdge(const std::string &src, const std::string &dst) {
        src;
        dst;
    }
    const std::string &get() const {
        return "";
    }
    bool change(const std::string &) {
        return false;
    }
};
