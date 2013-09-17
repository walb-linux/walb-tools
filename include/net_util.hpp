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

namespace cybozu {
namespace net {

std::string getHostName()
{
    std::string s;
    s.resize(1024);
    int r = ::gethostname(&s[0], s.size());
    if (r) throw std::runtime_error("gethostname() failed.");
    size_t len = ::strlen(&s[0]);
    s.resize(len);
    return s;
}

}} //namespace cybozu::net
