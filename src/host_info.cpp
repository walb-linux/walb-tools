#include "host_info.hpp"

namespace walb {

void AddrPort::verify() const
{
    const char *const msg = "AddrPort::verify";
    if (addr.empty()) {
        throw cybozu::Exception(msg) << "addr is empty";
    }
    if (port == 0) {
        throw cybozu::Exception(msg) << "port must not be 0";
    }
}

void CompressOpt::verify() const
{
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

CompressOpt parseCompressOpt(const std::string &comprOpt)
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

AddrPort parseAddrPort(const std::string &addrPort)
{
    StrVec v = cybozu::Split(addrPort, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception("parseAddrPort:parse error") << addrPort;
    }
    return AddrPort(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}

HostInfoForBkp parseHostInfoForBkp(const StrVec &v, size_t pos)
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

HostInfoForRepl parseHostInfoForRepl(const StrVec &v, size_t pos)
{
    std::string addrPortStr;
    std::string doResyncStr = "0";
    std::string dontMergeStr = "0";
    std::string cmprStr = CompressOpt().str();
    std::string maxWdiffMergeSizeStr = cybozu::util::toUnitIntString(DEFAULT_MAX_WDIFF_MERGE_MB * MEBI);
    std::string bulkSizeStr = cybozu::util::toUnitIntString(DEFAULT_BULK_LB * LOGICAL_BLOCK_SIZE);
    cybozu::util::parseStrVec(
        v, pos, 1, {&addrPortStr, &doResyncStr, &dontMergeStr, &cmprStr, &maxWdiffMergeSizeStr, &bulkSizeStr});

    HostInfoForRepl hi;
    hi.addrPort = parseAddrPort(addrPortStr);
    hi.doResync = static_cast<int>(cybozu::atoi(doResyncStr)) != 0;
    hi.dontMerge = static_cast<int>(cybozu::atoi(dontMergeStr)) != 0;
    hi.cmpr = parseCompressOpt(cmprStr);
    hi.maxWdiffMergeSize = cybozu::util::fromUnitIntString(maxWdiffMergeSizeStr);
    hi.bulkLb = util::parseBulkLb(bulkSizeStr, __func__);
    hi.verify();
    return hi;
}

std::string HostInfoForRepl::str() const
{
    return cybozu::util::formatString(
        "%s %d %d %s %s %s"
        , addrPort.str().c_str()
        , doResync ? 1 : 0
        , dontMerge ? 1 : 0
        , cmpr.str().c_str()
        , cybozu::util::toUnitIntString(maxWdiffMergeSize).c_str()
        , cybozu::util::toUnitIntString(bulkLb * LOGICAL_BLOCK_SIZE).c_str());
}

} //namespace walb
