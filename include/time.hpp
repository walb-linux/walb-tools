#pragma once
/**
 * @file
 * @brief Time utility header.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdlib>
#include <cassert>
#include <stdexcept>
#include <limits>
#include <time.h>
#include <type_traits>
#include <chrono>

namespace cybozu {

/**
 * RETURN:
 *   difference of localtime() and gmtime() [sec].
 */
inline int32_t localTimeDiff()
{
    time_t now = ::time(nullptr);
    struct tm tm0, tm1;
    if (::gmtime_r(&now, &tm0) == nullptr) {
        throw std::runtime_error("gmtime_r failed.");
    }
    if (::localtime_r(&now, &tm1) == nullptr) {
        throw std::runtime_error("localtime_r failed.");
    }
    time_t now0 = ::mktime(&tm0);
    time_t now1 = ::mktime(&tm1);
    int32_t diff = 0;
    if (now0 <= now1) {
        diff = -(now1 - now0);
    } else {
        diff = now0 - now1;
    }
    assert(diff < 60 * 60 * 24);
    assert(-(60 * 60 * 24) < diff);
    return diff;
}

/**
 * Format unix time to string.
 * fmt is of strftime().
 */
inline std::string formatUnixTime(time_t ts, const char *fmt)
{
    struct tm tm;
    if (::gmtime_r(&ts, &tm) == nullptr) {
        throw std::runtime_error("gmtime_r failed.");
    }
    const size_t BUF_SIZE = 1024;
    char buf[BUF_SIZE];
    const size_t size = ::strftime(buf, BUF_SIZE, fmt, &tm);
    if (size == 0) {
        throw std::runtime_error("strftime failed.");
    }
    std::string s(buf);
    assert(s.size() == size);
    return s;
}

/**
 * Convert time_t value to time string as UTC.
 */
inline std::string unixTimeToStr(time_t ts)
{
    std::string s = formatUnixTime(ts, "%Y%m%d%H%M%S");
    assert(s.size() == 14);
    return s;
}

/**
 * Convert time_t value to pretty time string as UTC.
 */
inline std::string unixTimeToPrettyStr(time_t ts)
{
    std::string s = formatUnixTime(ts, "%Y-%m-%dT%H:%M:%S");
    assert(s.size() == 19);
    return s;
}

/**
 * Convert time string as UTC to time_t value.
 */
inline time_t strToUnixTime(const std::string &ts)
{
    if (ts.size() != 14) {
        throw std::runtime_error("invalid time string.");
    }
    for (char c : ts) {
        if (!('0' <= c && c <= '9')) {
            throw std::runtime_error("invalid time string.");
        }
    }
    struct tm tm;
    ::memset(&tm, 0, sizeof(tm));
    int year = ::atoi(ts.substr(0, 4).c_str());
    if (!(1900 <= year && year < 10000)) {
        throw std::runtime_error("year must be between 1900 and 9999.");
    }
    int mon = atoi(ts.substr(4, 2).c_str());
    if (!(1 <= mon && mon <= 12)) {
        throw std::runtime_error("mon must be between 1 and 12.");
    }
    int mday = ::atoi(ts.substr(6, 2).c_str());
    if (!(1 <= mday && mday <= 31)) {
        throw std::runtime_error("mday must be between 1 and 31.");
    }
    int hour = ::atoi(ts.substr(8, 2).c_str());
    if (!(0 <= hour && hour <= 23)) {
        throw std::runtime_error("hour must be between 0 and 23.");
    }
    int min = ::atoi(ts.substr(10, 2).c_str());
    if (!(0 <= min && min <= 59)) {
        throw std::runtime_error("min must be between 0 and 59.");
    }
    int sec = ::atoi(ts.substr(12, 2).c_str());
    if (!(0 <= sec && sec <= 60)) {
        throw std::runtime_error("sec must be between 0 and 60.");
    }
    tm.tm_year = year - 1900;
    tm.tm_mon = mon - 1;
    tm.tm_mday = mday;
    tm.tm_hour = hour;
    tm.tm_min = min;
    tm.tm_sec = sec;
    time_t ts0 = ::mktime(&tm);
    if (ts0 == time_t(-1)) {
        throw std::runtime_error("mktime failed.");
    }
    return ts0 - localTimeDiff();
}

inline std::string getHighResolutionTimeStr(const struct timespec& ts)
{
    std::string ret = unixTimeToPrettyStr(ts.tv_sec);
    char buf[11];
    if (::snprintf(buf, 11, ".%09ld", ts.tv_nsec) >= 11) {
        buf[0] = '\0';
    }
    ret += buf;
    return ret;
}

template <typename Clock, typename Resolution>
class StopwatchT
{
    std::chrono::time_point<Clock> t0, t1;
public:
    StopwatchT() : t0(), t1() {
        reset();
    }
    void reset() {
        t0 = t1 = Clock::now();
    }
    /**
     * RETURN:
     *   Time period in seconds.
     */
    double get() {
        t1 = Clock::now();
        const size_t c = std::chrono::duration_cast<Resolution>(t1 - t0).count();
        t0 = t1; // reset.
        double divider;
        if (std::is_same<Resolution, std::chrono::seconds>::value) {
            divider = 1.0;
        } else if (std::is_same<Resolution, std::chrono::milliseconds>::value) {
            divider = 1000.0;
        } else if (std::is_same<Resolution, std::chrono::microseconds>::value) {
            divider = 1000000.0;
        } else if (std::is_same<Resolution, std::chrono::nanoseconds>::value) {
            divider = 1000000000.0;
        } else {
            throw std::runtime_error("unsupported Resolution type.");
        }
        return c / divider;
    }
};

using Stopwatch = StopwatchT<std::chrono::steady_clock, std::chrono::milliseconds>;
using AccurateStopwatch = StopwatchT<std::chrono::high_resolution_clock, std::chrono::microseconds>;

} //namespace cybozu
