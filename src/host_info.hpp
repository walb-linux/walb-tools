#pragma once
#include <string>
#include <cassert>
#include <iostream>
#include "walb_util.hpp"
#include "walb_diff_base.hpp"
#include "compression_type.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"

namespace walb {

struct AddrPort
{
    std::string addr; /* what cybozu::SocketAddr can treat. */
    uint16_t port;

    AddrPort() : addr(), port(0) {
    }
    AddrPort(const std::string &addr, uint16_t port) : addr(addr), port(port) {
        verify();
    }
    void verify() const {
        const char *const msg = "AddrPort::verify";
        if (addr.empty()) {
            throw cybozu::Exception(msg) << "addr is empty";
        }
        if (port == 0) {
            throw cybozu::Exception(msg) << "port must not be 0";
        }
    }
    bool operator==(const AddrPort &rhs) const {
        return addr == rhs.addr && port == rhs.port;
    }
    bool operator!=(const AddrPort &rhs) const {
        return addr != rhs.addr || port != rhs.port;
    }
    void set(const std::string &addr, uint16_t port) {
        this->addr = addr;
        this->port = port;
        verify();
    }
    cybozu::SocketAddr getSocketAddr() const {
        return cybozu::SocketAddr(addr, port);
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, addr);
        cybozu::save(os, port);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(addr, is);
        cybozu::load(port, is);
        verify();
    }
    std::string str() const;
    void parse(const std::string &);
    friend inline std::ostream &operator<<(std::ostream &os, const AddrPort &s) {
        os << s.str();
        return os;
    }
};

struct CompressOpt
{
    uint8_t type; /* wdiff compression type. */
    uint8_t level; /* wdiff compression level. */
    uint8_t numCpu; /* number of compression threads. */

    explicit CompressOpt(uint8_t type = ::WALB_DIFF_CMPR_SNAPPY, uint8_t level = 0, uint8_t numCpu = 1)
        : type(type), level(level), numCpu(numCpu) {
        verify();
    }
    bool operator==(const CompressOpt &rhs) const {
        return type == rhs.type && level == rhs.level && numCpu == rhs.numCpu;
    }
    bool operator!=(const CompressOpt &rhs) const {
        return type != rhs.type || level != rhs.level || numCpu != rhs.numCpu;
    }
    void verify() const {
        const char *const msg = "CompressOpt::verify";
        if (type >= ::WALB_DIFF_CMPR_MAX) {
            throw cybozu::Exception(msg)
                << "invalid type" << type;
        }
        if (level > 9) {
            throw cybozu::Exception(msg)
                << "invalid level" << level;
        }
        if (numCpu == 0) {
            throw cybozu::Exception(msg)
                << "invalid num cpu" << numCpu;
        }
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, type);
        cybozu::save(os, level);
        cybozu::save(os, numCpu);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(type, is);
        cybozu::load(level, is);
        cybozu::load(numCpu, is);
        verify();
    }
    std::string str() const;
    void parse(const std::string &);
    friend inline std::ostream &operator<<(std::ostream &os, const CompressOpt &s) {
        os << s.str();
        return os;
    }
};

/**
 * managed by proxy permanently to execute wdiff-transfer.
 */
struct HostInfoForBkp
{
    AddrPort addrPort;
    CompressOpt cmpr;
    uint32_t wdiffSendDelaySec;

    HostInfoForBkp() : addrPort(), cmpr(), wdiffSendDelaySec(0) {}
    HostInfoForBkp(const std::string &addr, uint16_t port, const CompressOpt &cmpr = CompressOpt(),
             uint32_t wdiffSendDelaySec = 0)
        : addrPort(addr, port)
        , cmpr(cmpr)
        , wdiffSendDelaySec(wdiffSendDelaySec) {
    }
    bool operator==(const HostInfoForBkp &rhs) const {
        return addrPort == rhs.addrPort
            && cmpr == rhs.cmpr
            && wdiffSendDelaySec == rhs.wdiffSendDelaySec;
    }
    bool operator!=(const HostInfoForBkp &rhs) const {
        return addrPort != rhs.addrPort
            || cmpr != rhs.cmpr
            || wdiffSendDelaySec != rhs.wdiffSendDelaySec;
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, addrPort);
        cybozu::save(os, cmpr);
        cybozu::save(os, wdiffSendDelaySec);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(addrPort, is);
        cybozu::load(cmpr, is);
        cybozu::load(wdiffSendDelaySec, is);
    }
    std::string str() const;
    void parse(const StrVec &, size_t);
    friend inline std::ostream &operator<<(std::ostream &os, const HostInfoForBkp &s) {
        os << s.str();
        return os;
    }
};

inline CompressOpt parseCompressOpt(const std::string &comprOpt)
{
    StrVec v = cybozu::Split(comprOpt, ':', 3);
    if (v.size() != 3) {
        throw cybozu::Exception("parseCompressOpt:parse error") << comprOpt;
    }
    CompressOpt cmpr;
    cmpr.type = parseCompressionType(v[0]);
    cmpr.level = static_cast<uint8_t>(cybozu::atoi(v[1]));
    cmpr.numCpu = static_cast<uint8_t>(cybozu::atoi(v[2]));
    cmpr.verify();
    return cmpr;
}

inline AddrPort parseAddrPort(const std::string &addrPort)
{
    StrVec v = cybozu::Split(addrPort, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception("parseAddrPort:parse error") << addrPort;
    }
    return AddrPort(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}

/**
 * Parse three strings into a HostInfoForBkp.
 */
inline HostInfoForBkp parseHostInfoForBkp(const StrVec &v, size_t pos = 0)
{
    std::string addrPort;
    std::string compressOpt = "snappy:0:1";
    std::string delay = "0";

    if (v.size() <= pos) throw cybozu::Exception(__func__) << "addr:port is required";
    cybozu::util::parseStrVec(v, pos, 1, {&addrPort, &compressOpt, &delay});

    HostInfoForBkp hi;
    hi.addrPort = parseAddrPort(addrPort);
    hi.cmpr = parseCompressOpt(compressOpt);
    hi.wdiffSendDelaySec = static_cast<uint32_t>(cybozu::atoi(delay));
    return hi;
}

inline void CompressOpt::parse(const std::string &str)
{
    *this = parseCompressOpt(str);
}

inline void AddrPort::parse(const std::string &addrPort)
{
    *this = parseAddrPort(addrPort);
}

inline void HostInfoForBkp::parse(const StrVec &v, size_t pos = 0)
{
    *this = parseHostInfoForBkp(v, pos);
}

inline std::string AddrPort::str() const
{
    return cybozu::util::formatString("%s:%u", addr.c_str(), port);
}

inline std::string CompressOpt::str() const
{
    return cybozu::util::formatString(
        "%s:%u:%u"
        , compressionTypeToStr(type).c_str()
        , level
        , numCpu);
}

inline std::string HostInfoForBkp::str() const
{
    return cybozu::util::formatString(
        "%s %s %u"
        , addrPort.str().c_str()
        , cmpr.str().c_str()
        , wdiffSendDelaySec);
}

/**
 * managed by archive temporarly to execute repl-sync.
 */
struct HostInfoForRepl
{
    AddrPort addrPort; // archive host info.
    bool doResync; // do resync if necessary.
    CompressOpt cmpr; // compression parameters for diff-repl.
    uint64_t maxWdiffMergeSize; // max wdiff size in bytes to merge for diff-repl.
    uint64_t bulkLb; // bulk size in logical block for full-repl/hash-repl. [logical block]

    HostInfoForRepl() : addrPort(), doResync(false), cmpr(), maxWdiffMergeSize(0), bulkLb(0) {
    }
    HostInfoForRepl(const std::string &addr, uint64_t port, bool doResync = false, const CompressOpt &cmpr = CompressOpt(),
                    uint64_t maxWdiffMergeSize = DEFAULT_MAX_WDIFF_MERGE_MB * MEBI,
                    uint64_t bulkLb = DEFAULT_BULK_LB)
        : addrPort(addr, port), doResync(doResync), cmpr(cmpr)
        , maxWdiffMergeSize(maxWdiffMergeSize)
        , bulkLb(bulkLb) {
        verify();
    }
    bool operator==(const HostInfoForRepl &rhs) const {
        return addrPort == rhs.addrPort
            && doResync == rhs.doResync
            && cmpr == rhs.cmpr
            && maxWdiffMergeSize == rhs.maxWdiffMergeSize
            && bulkLb == rhs.bulkLb;
    }
    bool operator!=(const HostInfoForRepl &rhs) const {
        return addrPort != rhs.addrPort
            || doResync != rhs.doResync
            || cmpr != rhs.cmpr
            || maxWdiffMergeSize != rhs.maxWdiffMergeSize
            || bulkLb != rhs.bulkLb;
    }
    void verify() const {
        const char *const msg = "HostInfoForRepl::verify";
        if (bulkLb == 0) throw cybozu::Exception(msg) << "bulkLb must not be 0";
        if (maxWdiffMergeSize == 0) {
            throw cybozu::Exception(msg) << "maxWdiffMergeSize must not be 0";
        }
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, addrPort);
        cybozu::save(os, doResync);
        cybozu::save(os, cmpr);
        cybozu::save(os, maxWdiffMergeSize);
        cybozu::save(os, bulkLb);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(addrPort, is);
        cybozu::load(doResync, is);
        cybozu::load(cmpr, is);
        cybozu::load(maxWdiffMergeSize, is);
        cybozu::load(bulkLb, is);
        verify();
    }
    std::string str() const;
    void parse(const StrVec &, size_t);
    friend inline std::ostream &operator<<(std::ostream &os, const HostInfoForRepl &s) {
        os << s.str();
        return os;
    }
};

inline HostInfoForRepl parseHostInfoForRepl(const StrVec &v, size_t pos = 0)
{
    std::string addrPortStr;
    std::string doResyncStr = "0";
    std::string cmprStr = CompressOpt().str();
    std::string maxWdiffMergeSizeStr = cybozu::util::toUnitIntString(DEFAULT_MAX_WDIFF_MERGE_MB * MEBI);
    std::string bulkSizeStr = cybozu::util::toUnitIntString(DEFAULT_BULK_LB * LOGICAL_BLOCK_SIZE);
    cybozu::util::parseStrVec(
        v, pos, 1, {&addrPortStr, &doResyncStr, &cmprStr, &maxWdiffMergeSizeStr, &bulkSizeStr});

    HostInfoForRepl hi;
    hi.addrPort = parseAddrPort(addrPortStr);
    hi.doResync = static_cast<int>(cybozu::atoi(doResyncStr)) != 0;
    hi.cmpr = parseCompressOpt(cmprStr);
    hi.maxWdiffMergeSize = cybozu::util::fromUnitIntString(maxWdiffMergeSizeStr);
    hi.bulkLb = util::parseBulkLb(bulkSizeStr, __func__);
    hi.verify();
    return hi;
}

inline void HostInfoForRepl::parse(const StrVec &v, size_t pos = 0)
{
    *this = parseHostInfoForRepl(v, pos);
}

inline std::string HostInfoForRepl::str() const
{
    return cybozu::util::formatString(
        "%s %d %s %s %s"
        , addrPort.str().c_str()
        , doResync ? 1 : 0
        , cmpr.str().c_str()
        , cybozu::util::toUnitIntString(maxWdiffMergeSize).c_str()
        , cybozu::util::toUnitIntString(bulkLb * LOGICAL_BLOCK_SIZE).c_str());
}

} //namespace walb
