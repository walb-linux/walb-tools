#pragma once
#include <string>
#include <cassert>
#include <iostream>
#include "walb_diff_base.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"

namespace walb {

/**
 * Host identifier for connection.
 */
struct HostInfo
{
    std::string addr; /* what cybozu::SocketAddr can treat. */
    uint16_t port;
    uint8_t compressionType; /* wdiff compression type. */
    uint8_t compressionLevel; /* wdiff compression level. */

    HostInfo() : HostInfo("", 0) {}
    HostInfo(const std::string &addr, uint16_t port,
             uint8_t type = ::WALB_DIFF_CMPR_SNAPPY, uint8_t level = 0)
        : addr(addr), port(port)
        , compressionType(type), compressionLevel(level) {}
    bool operator==(const HostInfo &rhs) const {
        return addr == rhs.addr && port == rhs.port
            && compressionType == rhs.compressionType
            && compressionLevel == rhs.compressionLevel;
    }
    bool operator!=(const HostInfo &rhs) const {
        return addr != rhs.addr || port != rhs.port
            || compressionType != rhs.compressionType
            || compressionLevel != rhs.compressionLevel;
    }
    void verify() const {
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
        cybozu::save(os, addr);
        cybozu::save(os, port);
        cybozu::save(os, compressionType);
        cybozu::save(os, compressionLevel);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(addr, is);
        cybozu::load(port, is);
        cybozu::load(compressionType, is);
        cybozu::load(compressionLevel, is);
        verify();
    }
    std::string str() const;
    void parse(const std::string &, const std::string &);
    void parse(const std::string &);
    friend inline std::ostream &operator<<(std::ostream &os, const HostInfo &s) {
        os << s.str();
        return os;
    }
};

namespace host_info_local {

struct Pair
{
    std::string typeStr;
    int type;
};

static const Pair compressionTypeTable[] = {
    { "none", ::WALB_DIFF_CMPR_NONE },
    { "snappy", ::WALB_DIFF_CMPR_SNAPPY },
    { "gzip", ::WALB_DIFF_CMPR_GZIP },
    { "lzma", ::WALB_DIFF_CMPR_LZMA },
};

} // namespace host_info_local

inline int parseCompressionType(const std::string &typeStr)
{
    namespace lo = host_info_local;
    for (const lo::Pair &p : lo::compressionTypeTable) {
        if (p.typeStr == typeStr) {
            return p.type;
        }
    }
    throw cybozu::Exception("parseCompressionType:wrong type") << typeStr;
}

inline const std::string &compressionTypeToStr(int type)
{
    namespace lo = host_info_local;
    for (const lo::Pair &p : lo::compressionTypeTable) {
        if (p.type == type) {
            return p.typeStr;
        }
    }
    throw cybozu::Exception("compressionTypeToStr:wrong type") << type;
}

/**
 * Parse three strings into a HostInfo.
 */
inline HostInfo parseHostInfo(
    const std::string &addrPort, const std::string &compressOpt)
{
    HostInfo hi;
    {
        std::vector<std::string> v = cybozu::Split(addrPort, ':', 2);
        if (v.size() != 2) {
            throw cybozu::Exception("parseHostInfo:parse error") << addrPort;
        }
        hi.addr = v[0];
        hi.port = static_cast<uint16_t>(cybozu::atoi(v[1]));
    }
    {
        std::vector<std::string> v = cybozu::Split(compressOpt, ':', 2);
        if (v.size() != 2) {
            throw cybozu::Exception("parseHostInfo:parse error") << compressOpt;
        }
        hi.compressionType = parseCompressionType(v[0]);
        hi.compressionLevel = static_cast<uint8_t>(cybozu::atoi(v[1]));
    }
    hi.verify();
    return hi;
}

/**
 * Parse a string into a HostInfo.
 * Input string is like "addr:port compressionType:compresionLevel".
 */
inline HostInfo parseHostInfo(const std::string &s)
{
    HostInfo hi;
    std::vector<std::string> v = cybozu::Split(s, ' ');
    auto itr = std::remove_if(v.begin(), v.end(), [](const std::string &s) {
            return s.empty();
        });
    v.erase(itr, v.end());
    if (v.size() != 2) {
        throw cybozu::Exception("parseHostInfo:not 2 tokens.")
            << s;
    }
    return parseHostInfo(v[0], v[1]);
}

inline void HostInfo::parse(
    const std::string &addrPort, const std::string &compressOpt)
{
    *this = parseHostInfo(addrPort, compressOpt);
}

inline void HostInfo::parse(const std::string &s)
{
    *this = parseHostInfo(s);
}

inline std::string HostInfo::str() const
{
    return cybozu::util::formatString(
        "%s:%u %s:%u"
        , addr.c_str(), port
        , compressionTypeToStr(compressionType).c_str()
        , compressionLevel);
}

} //namespace walb
