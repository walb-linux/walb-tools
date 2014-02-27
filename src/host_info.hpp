#pragma once
#include <string>
#include <cassert>
#include <iostream>
#include "walb_diff_base.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/exception.hpp"

namespace walb {

/**
 * Host identifier for connection.
 */
struct HostInfo
{
    std::string name; /* must be unique in the system. */
    std::string addr; /* what cybozu::SocketAddr can treat. */
    uint16_t port;
    uint8_t compressionType; /* wdiff compression type. */
    uint8_t compressionLevel; /* wdiff compression level. */

    HostInfo() : HostInfo("", "", 0) {}
    HostInfo(const std::string &name, const std::string &addr, uint16_t port,
               uint8_t type = ::WALB_DIFF_CMPR_SNAPPY, uint8_t level = 0)
        : name(name), addr(addr), port(port)
        , compressionType(type), compressionLevel(level) {}
    bool operator==(const HostInfo &rhs) const {
        return name == rhs.name;
    }
    bool operator!=(const HostInfo &rhs) const {
        return name != rhs.name;
    }
    void verify() const {
        if (name.empty()) {
            throw cybozu::Exception("HostInfo::verify::name is empty");
        }
        if (addr.empty()) {
            throw cybozu::Exception("HostInfo::verify::addr is empty");
        }
        if (compressionType >= ::WALB_DIFF_CMPR_MAX) {
            throw cybozu::Exception("HostInfo::verify:invalid compression type")
                << compressionType;
        }
        if (compressionLevel > 9) {
            throw cybozu::Exception("HostInfo::verify::invalid compression level")
                << compressionLevel;
        }
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, name);
        cybozu::save(os, addr);
        cybozu::save(os, port);
        cybozu::save(os, compressionType);
        cybozu::save(os, compressionLevel);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(name, is);
        cybozu::load(addr, is);
        cybozu::load(port, is);
        cybozu::load(compressionType, is);
        cybozu::load(compressionLevel, is);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const HostInfo &s) {
        os << s.name << "(" << s.addr << ":" << s.port << ")"
           << s.compressionType << ":" << s.compressionLevel;
        return os;
    }
};

} //namespace walb
