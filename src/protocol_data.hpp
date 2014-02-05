#pragma once

namespace walb {
namespace protocol {

/**
 * Utility class for protocols.
 */
class ProtocolData
{
protected:
    const std::string &protocolName_;
    cybozu::Socket &sock_;
    Logger &logger_;
    const std::atomic<bool> &forceQuit_;
    const std::vector<std::string> &params_;
public:
    ProtocolData(const std::string &protocolName,
                 cybozu::Socket &sock, Logger &logger,
                 const std::atomic<bool> &forceQuit,
                 const std::vector<std::string> &params)
        : protocolName_(protocolName)
        , sock_(sock), logger_(logger)
        , forceQuit_(forceQuit), params_(params) {}
    virtual ~ProtocolData() noexcept = default;

    void logAndThrow(const char *fmt, ...) const {
        va_list args;
        va_start(args, fmt);
        std::string msg = cybozu::util::formatStringV(fmt, args);
        va_end(args);
        logger_.error(msg);
        throw std::runtime_error(msg);
    }
};

}} // walb::protocol
