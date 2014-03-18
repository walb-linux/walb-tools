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
 * Convert time_t value to time string as UTC.
 */
inline std::string unixTimeToStr(time_t ts)
{
    struct tm tm;
    if (::gmtime_r(&ts, &tm) == nullptr) {
        throw std::runtime_error("gmtime_r failed.");
    }
    std::string s("YYYYmmddHHMMSS ");
    if (::strftime(&s[0], s.size(), "%Y%m%d%H%M%S", &tm) == 0) {
        throw std::runtime_error("strftime failed.");
    }
    s.resize(s.size() - 1);
    assert(s.size() == 14);
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

} //namespace cybozu
