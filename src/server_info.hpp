#pragma once
/**
 * @file
 * @brief Server information header.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cassert>
#include <iostream>
#include "walb_diff_base.hpp"
#include "cybozu/serializer.hpp"

namespace walb {

/**
 * Server identifier for connection.
 */
class ServerInfo
{
private:
    std::string name_; /* must be unique in the system. */
    std::string addr_; /* what cybozu::SocketAddr can treat. */
    uint16_t port_;
    uint8_t compressionType_; /* wdiff compression type. */
    uint8_t compressionLevel_; /* wdiff compression level. */
public:
    ServerInfo()
        : name_(), addr_(), port_(0)
        , compressionType_(::WALB_DIFF_CMPR_SNAPPY), compressionLevel_(0) {}
    ServerInfo(const std::string &name, const std::string &addr, uint16_t port)
        : name_(name), addr_(addr), port_(port)
        , compressionType_(::WALB_DIFF_CMPR_SNAPPY), compressionLevel_(0) {}
    const std::string &name() const { return name_; }
    const std::string &addr() const { return addr_; }
    uint16_t port() const { return port_; }
    bool operator==(const ServerInfo &rhs) const {
        return name_ == rhs.name_;
    }
    bool operator!=(const ServerInfo &rhs) const {
        return name_ != rhs.name_;
    }
    void setComprssionType(uint8_t type) {
        assert(type < ::WALB_DIFF_CMPR_MAX);
        compressionType_ = type;
    }
    void setCompressionLevel(uint8_t level) {
        compressionLevel_ = level;
    }
    uint8_t compressionType() const { return compressionType_; }
    uint8_t compressionLevel() const { return compressionLevel_; }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, name_);
        cybozu::save(os, addr_);
        cybozu::save(os, port_);
        cybozu::save(os, compressionType_);
        cybozu::save(os, compressionLevel_);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(name_, is);
        cybozu::load(addr_, is);
        cybozu::load(port_, is);
        cybozu::load(compressionType_, is);
        cybozu::load(compressionLevel_, is);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const ServerInfo &s) {
        os << s.name_ << "(" << s.addr_ << ":" << s.port_ << ")"
           << s.compressionType_ << ":" << s.compressionLevel_;
        return os;
    }
};

} //namespace walb
