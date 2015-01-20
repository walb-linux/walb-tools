#pragma once
#include <string>
#include <iostream>
#include "cybozu/exception.hpp"
#include "walb_types.hpp"

namespace walb {

enum class StopType
{
    Graceful, Empty, Force,
};

struct StopOpt
{
    StopType type;

    StopOpt() : type(StopType::Graceful) {
    }
    std::string str() const {
        switch (type) {
        case StopType::Graceful: return "graceful";
        case StopType::Empty: return "empty";
        case StopType::Force: return "force";
        }
        throw cybozu::Exception(__func__) << "bug" << int(type);
    }
    void parse(const std::string &s) {
        type = StopType::Graceful;
        if (s.empty()) return;
        if (s[0] == 'f') {
            type = StopType::Force;
        } else if (s[0] == 'e') {
            type = StopType::Empty;
        }
    }
    bool isForce() const { return type == StopType::Force; }
    bool isGraceful() const { return type == StopType::Graceful; }
    bool isEmpty() const { return type == StopType::Empty; }
    friend inline std::ostream &operator<<(std::ostream &os, const StopOpt &opt) {
        os << opt.str();
        return os;
    }
};

inline std::pair<std::string, StopOpt> parseStopParams(const StrVec &v, const char *msg)
{
    if (v.empty()) throw cybozu::Exception(msg) << "empty";
    if (v[0].empty()) throw cybozu::Exception(msg) << "empty volId";
    StopOpt opt;
    if (v.size() >= 2) opt.parse(v[1]);
    return {v[0], opt};
}

} // namespace walb
