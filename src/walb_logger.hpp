#pragma once
/**
 * @file
 * @brief wrapper of cybozu logger.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/log.hpp"
#include "util.hpp"
#include <sstream>

#define LOGd(...) LOGd2(__VA_ARGS__, "")
#define LOGd2(fmt, ...) \
    cybozu::PutLog(cybozu::LogDebug, "DEBUG (%s:%d) " fmt "%s", __func__, __LINE__, __VA_ARGS__)
#define LOGi(...) cybozu::PutLog(cybozu::LogInfo, "INFO " __VA_ARGS__)
#define LOGw(...) cybozu::PutLog(cybozu::LogWarning, "WARNING " __VA_ARGS__)
#define LOGe(...) cybozu::PutLog(cybozu::LogError, "ERROR " __VA_ARGS__)

#define LOGd_(...)
#define LOGi_(...)
#define LOGw_(...)
#define LOGe_(...)

#define LOGs walb::SimpleLogger()

namespace walb {

/**
 * Logger interface.
 *
 * You can call one of them to change output target:
 *   cybozu::OpenLogFile() for a file.
 *   cybozu::SetLogFILE() for FILE pointer.
 *   default is syslog.
 *   These are not thread-safe. You must call one of them at once.
 * You can change priority of putting logs with:
 *   cybozu::SetLogPriority().
 */
class Logger
{
public:
    virtual ~Logger() noexcept = default;
    virtual void write(cybozu::LogPriority pri, const char *msg) const noexcept = 0;

    void writeS(cybozu::LogPriority pri, const std::string &msg) const noexcept {
        write(pri, msg.c_str());
    }
    void writeV(cybozu::LogPriority pri, const char *format, va_list args) const noexcept;

#ifdef __GNUC__
    void writeF(cybozu::LogPriority pri, const char *format, ...) const noexcept __attribute__((format(printf, 3, 4)));
    #define WALB_LOGGER_FORMAT_ATTR __attribute__((format(printf, 2, 3)))
#else
    void writeF(cybozu::LogPriority pri, const char *format, ...) const noexcept;
    #define WALB_LOGGER_FORMAT_ATTR
#endif

    void debug(const std::string &msg) const noexcept { writeS(cybozu::LogDebug, msg); }
    void info(const std::string &msg) const noexcept { writeS(cybozu::LogInfo, msg); }
    void warn(const std::string &msg) const noexcept { writeS(cybozu::LogWarning, msg); }
    void error(const std::string &msg) const noexcept { writeS(cybozu::LogError, msg); }

    void debug(const char *format, ...) const noexcept WALB_LOGGER_FORMAT_ATTR;
    void info(const char *format, ...) const noexcept WALB_LOGGER_FORMAT_ATTR;
    void warn(const char *format, ...) const noexcept WALB_LOGGER_FORMAT_ATTR;
    void error(const char *format, ...) const noexcept WALB_LOGGER_FORMAT_ATTR;

    template <typename E>
    void writeAndThrow(cybozu::LogPriority pri, const E &e) const {
        write(pri, e.what());
        throw e;
    }
    template <typename E>
    void throwError(const E &e) const {
        writeAndThrow(cybozu::LogError, e);
    }

    template <cybozu::LogPriority priority>
    struct Sync
    {
    private:
        const Logger &logger_;
        std::string s_;
    public:
        explicit Sync(const Logger& logger) : logger_(logger) {}
        ~Sync() noexcept {
            if (!s_.empty()) {
                logger_.writeS(priority, s_);
            }
        }
        Sync(Sync&& rhs)
            : logger_(rhs.logger_), s_(std::move(rhs.s_)) {}
        template <typename T>
        Sync& operator<<(const T& t) {
            std::ostringstream os;
            if (!s_.empty()) os << ':';
            os << t;
            s_ += os.str();
            return *this;
        }
    };
    using DebugSync = Sync<cybozu::LogDebug>;
    using InfoSync = Sync<cybozu::LogInfo>;
    using WarnSync = Sync<cybozu::LogWarning>;
    using ErrorSync = Sync<cybozu::LogError>;

    DebugSync debug() const { return DebugSync(*this); }
    InfoSync info() const { return InfoSync(*this); }
    WarnSync warn() const { return WarnSync(*this); }
    ErrorSync error() const { return ErrorSync(*this); }

    template <typename T>
    InfoSync operator<<(const T& t) const {
        InfoSync s(*this);
        s << t;
        return s;
    }
};

namespace logger_local {

const char *getPriStr(cybozu::LogPriority pri);

} // namespace logger_local

/**
 * Simple logger.
 */
class SimpleLogger : public Logger
{
public:
    void write(cybozu::LogPriority pri, const char *msg) const noexcept override {
        cybozu::PutLog(pri, "%s %s", logger_local::getPriStr(pri), msg);
    }
};

/**
 * Logger for protocols.
 */
class ProtocolLogger : public Logger
{
private:
    std::string selfId_;
    std::string remoteId_;
public:
    ProtocolLogger(const std::string &selfId, const std::string &remoteId)
        : selfId_(selfId), remoteId_(remoteId) {}
    void write(cybozu::LogPriority pri, const char *msg) const noexcept override {
        cybozu::PutLog(pri, "%s [%s][%s] %s", logger_local::getPriStr(pri)
                       , selfId_.c_str(), remoteId_.c_str(), msg);
    }
};

void putErrorLogIfNecessary(std::exception_ptr ep, Logger &logger, const char *msg) noexcept;

} //namespace walb
