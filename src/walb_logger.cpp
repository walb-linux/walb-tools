#include "walb_logger.hpp"

namespace walb {

void Logger::writeV(cybozu::LogPriority pri, const char *format, va_list args) const noexcept
{
    try {
        std::string msg;
        cybozu::vformat(msg, format, args);
        writeS(pri, msg);
    } catch (...) {
        write(pri, "Logger::write() error.");
    }
}

void Logger::writeF(cybozu::LogPriority pri, const char *format, ...) const noexcept
{
    try {
        va_list args;
        va_start(args, format);
        writeV(pri, format, args);
        va_end(args);
    } catch (...) {
        write(pri, "Logger::write() error.");
    }
}

void Logger::debug(const char *format, ...) const noexcept
{
    va_list args;
    va_start(args, format);
    writeV(cybozu::LogDebug, format, args);
    va_end(args);
}

void Logger::info(const char *format, ...) const noexcept
{
    va_list args;
    va_start(args, format);
    writeV(cybozu::LogInfo, format, args);
    va_end(args);
}

void Logger::warn(const char *format, ...) const noexcept
{
    va_list args;
    va_start(args, format);
    writeV(cybozu::LogWarning, format, args);
    va_end(args);
}

void Logger::error(const char *format, ...) const noexcept
{
    va_list args;
    va_start(args, format);
    writeV(cybozu::LogError, format, args);
    va_end(args);
}

namespace logger_local {

const char *getPriStr(cybozu::LogPriority pri)
{
    static const std::pair<int, const char *> tbl[] = {
        { cybozu::LogDebug, "DEBUG" },
        { cybozu::LogInfo, "INFO" },
        { cybozu::LogWarning, "WARNING" },
        { cybozu::LogError, "ERROR" },
    };
    for (const std::pair<int, const char *> &p : tbl) {
        if (pri == p.first) return p.second;
    }
    throw cybozu::Exception("getPriStr:bug");
}

} // namespace logger_local

void putErrorLogIfNecessary(std::exception_ptr ep, Logger &logger, const char *msg) noexcept
{
    if (!ep) return;
    try {
        std::rethrow_exception(ep);
    } catch (std::exception &e) {
        logger.error() << msg << e.what();
    } catch (...) {
        logger.error() << msg << "unknown error";
    }
}

} //namespace walb
