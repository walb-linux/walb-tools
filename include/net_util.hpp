#pragma once
/**
 * @file
 * @brief Network utility.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstring>
#include <string>
#include <stdexcept>
#include <cassert>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include "util.hpp"

namespace cybozu {
namespace net {

inline std::string getHostName()
{
    std::string s;
    s.resize(1024);
    int r = ::gethostname(&s[0], s.size());
    if (r) throw std::runtime_error("gethostname() failed.");
    size_t len = ::strlen(&s[0]);
    s.resize(len);
    return s;
}

/**
 * parse "HOST:PORT" format.
 */
inline std::pair<std::string, uint16_t> parseHostPortStr(const std::string &s)
{
    auto throwError = [&]() {
        std::string msg = cybozu::util::formatString(
            "parseHostColonPortStr: invalid format '%s'", s.c_str());
        throw std::runtime_error(msg);
    };
    size_t n = s.find(":");
    if (n == std::string::npos) throwError();
    std::string hostStr = s.substr(0, n);
    if (hostStr.empty()) throwError();
    std::string portStr = s.substr(n + 1);
    if (portStr.empty()) throwError();
    uint16_t port = cybozu::atoi(portStr);
    return {hostStr, port};
}

}} //namespace cybozu::net
