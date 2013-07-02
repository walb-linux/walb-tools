/**
 * @file
 * @brief Simple logger.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef CYBOZU_SIMPLE_LOGGER_HPP
#define CYBOZU_SIMPLE_LOGGER_HPP

#include <cstdio>
#include <cstdarg>
#include <thread>
#include <mutex>
#include <stdexcept>
#include <map>
#include <vector>
#include <chrono>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <iostream>

namespace {

class FpOpener
{
private:
    ::FILE *fp_;

public:
    FpOpener(const std::string &filePath, const char *mode)
        : fp_(staticOpen(filePath, mode)) {}

    ::FILE *fp() { return fp_; }

    ~FpOpener() noexcept {
        ::fclose(fp_);
    }
private:
    static ::FILE *staticOpen(const std::string &filePath, const char *mode) {
        ::FILE *fp = ::fopen(filePath.c_str(), mode);
        if (fp == nullptr) {
            throw std::runtime_error("fopen failed.");
        }
        return fp;
    }
};

} // anonymous namespace

namespace cybozu { namespace logger {

/**
 * Thread-safe simple logger.
 */
class SimpleLogger
{
private:
    typedef std::map<std::string, std::unique_ptr<SimpleLogger> > Map;

    static std::once_flag flag_;
    static std::shared_ptr<FpOpener> fpop_;
    static std::mutex mutex_;
    static Map map_;
    static std::string logPath_;
    static bool isStderr_;
    static std::vector<std::string> levelStr_;

    std::string name_;
    int level_;

public:
    enum {
        LOG_ERROR = 0,
        LOG_WARN,
        LOG_NOTICE,
        LOG_INFO,
        LOG_DEBUG,
        LOG_MAX,
    };

    /**
     * Call this before calling get().
     * "-" means stderr.
     */
    static void setPath(const std::string &path) {
        std::call_once(flag_, [&]() { init(); });
        std::lock_guard<std::mutex> lock(mutex_);
        logPath_ = path;
        isStderr_ = (path == "-");
        if (!isStderr_) { fpop_.reset(new FpOpener(logPath_, "a+")); }
    }

    /**
     * You can call this multi-time.
     */
    static SimpleLogger &get(const std::string &name = "default") {
        std::call_once(flag_, [&]() { init(); });
        std::lock_guard<std::mutex> lock(mutex_);
        Map::iterator it = map_.find(name);
        if (it != map_.end()) {
            return *it->second;
        }
        std::unique_ptr<SimpleLogger> p(new SimpleLogger(name));
        SimpleLogger &ret = *p;
        map_.insert(std::make_pair(name, std::move(p)));
        return ret;
    }

    void setLevel(int level) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (level < 0 || LOG_MAX <= level) { return; }
        level_ = level;
    }

    void write(int level, std::string &&msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        ::fprintf(fp(), "%s [%s] [%s] %s\n"
                  , timeStr().c_str(), name_.c_str(), levelStr_[level].c_str()
                  , msg.c_str());
    }

    void write(int level, const char *fmt, va_list ap) {
        std::lock_guard<std::mutex> lock(mutex_);
        ::fprintf(fp(), "%s [%s] [%s] "
                  , timeStr().c_str(), name_.c_str(), levelStr_[level].c_str());
        ::vfprintf(fp(), fmt, ap);
    }

    void write(int level, const char *fmt, ...) {
        va_list ap;
        va_start(ap, fmt);
        write(level, fmt, ap);
        va_end(ap);
    }

private:
    SimpleLogger(const std::string &name, int level = LOG_INFO)
        : name_(name), level_(level) {}
    SimpleLogger(const SimpleLogger&) = delete;
    SimpleLogger() = delete;
    SimpleLogger &operator=(const SimpleLogger&) = delete;

    static std::string timeStr() {
        time_t t = ::time(0);
        struct tm tm;
        ::localtime_r(&t, &tm);
        char buf[1024];
        if (::strftime(buf, 1024, "%Y-%m-%d %H:%M:%S %Z", &tm) == 0) {
            throw std::runtime_error("timeStr() buffer is not enough.");
        }
        return std::string(buf);
    }

    static ::FILE *fp() {
        if (isStderr_) {
            return ::stderr;
        } else {
            return fpop_->fp();
        }
    }

    static void init() {
        logPath_ = "-";
        isStderr_ = true;
        levelStr_.clear();
        levelStr_.push_back("ERROR");
        levelStr_.push_back("WARN");
        levelStr_.push_back("NOTICE");
        levelStr_.push_back("INFO");
        levelStr_.push_back("DEBUG");
        levelStr_.push_back("");
    }
};

std::once_flag SimpleLogger::flag_;
std::shared_ptr<FpOpener> SimpleLogger::fpop_;
std::mutex SimpleLogger::mutex_;
SimpleLogger::Map SimpleLogger::map_;
std::string SimpleLogger::logPath_;
bool SimpleLogger::isStderr_;
std::vector<std::string> SimpleLogger::levelStr_;


}} // namespace cybozu::logger

#ifdef LOG
#undef LOG
#endif
#ifdef LOGe
#undef LOGe
#endif
#ifdef LOGw
#undef LOGw
#endif
#ifdef LOGn
#undef LOGn
#endif
#ifdef LOGi
#undef LOGi
#endif
#ifdef LOGd
#undef LOGd
#endif
#ifdef LOGe_
#undef LOGe_
#endif
#ifdef LOGw_
#undef LOGw_
#endif
#ifdef LOGn_
#undef LOGn_
#endif
#ifdef LOGi_
#undef LOGi_
#endif
#ifdef LOGd_
#undef LOGd_
#endif

#define LOG(level, fmt, args...)                                        \
    cybozu::logger::SimpleLogger::get().write(                          \
        level, "(%s:%d) " fmt "\n", __func__, __LINE__, ##args)

#define LOGe(fmt, args...) LOG(cybozu::logger::SimpleLogger::LOG_ERROR, fmt, ##args);
#define LOGw(fmt, args...) LOG(cybozu::logger::SimpleLogger::LOG_WARN, fmt, ##args);
#define LOGn(fmt, args...) LOG(cybozu::logger::SimpleLogger::LOG_NOTICE, fmt, ##args);
#define LOGi(fmt, args...) LOG(cybozu::logger::SimpleLogger::LOG_INFO, fmt, ##args);
#define LOGd(fmt, args...) LOG(cybozu::logger::SimpleLogger::LOG_DEBUG, fmt, ##args);
#define LOGe_(fmt, args...)
#define LOGw_(fmt, args...)
#define LOGn_(fmt, args...)
#define LOGi_(fmt, args...)
#define LOGd_(fmt, args...)

#endif  /* CYBOZU_SIMPLE_LOGGER_HPP */
