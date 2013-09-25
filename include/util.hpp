#pragma once
/**
 * @file
 * @brief Utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>
#include <stdexcept>
#include <string>
#include <cstdarg>
#include <cstring>
#include <cstdint>
#include <cinttypes>
#include <cerrno>
#include <cassert>
#include <cstdio>
#include <cctype>

#include <sys/time.h>

#define UNUSED __attribute__((unused))
#define DEPRECATED __attribute__((deprecated))

#define RT_ERR(fmt, args...)                                    \
    std::runtime_error(cybozu::util::formatString(fmt, ##args))

#define CHECKx(cond) cybozu::util::checkCond(cond, __func__, __LINE__)

#define DISABLE_COPY_AND_ASSIGN(ClassName)              \
    ClassName(const ClassName &rhs) = delete;           \
    ClassName &operator=(const ClassName &rhs) = delete

#define DISABLE_MOVE(ClassName)                     \
    ClassName(ClassName &&rhs) = delete;            \
    ClassName &operator=(ClassName &&rhs) = delete

namespace cybozu {
namespace util {

/**
 * Formst string with va_list.
 */
std::string formatStringV(const char *format, va_list ap)
{
    char *p = nullptr;
    int ret = ::vasprintf(&p, format, ap);
    if (ret < 0) throw std::runtime_error("vasprintf failed.");
    try {
        std::string s(p, ret);
        ::free(p);
        return s;
    } catch (...) {
        ::free(p);
        throw;
    }
}

/**
 * Create a std::string using printf() like formatting.
 */
std::string formatString(const char * format, ...)
{
    std::string s;
    std::exception_ptr ep;
    va_list args;
    va_start(args, format);
    try {
        s = formatStringV(format, args);
    } catch (...) {
        ep = std::current_exception();
    }
    va_end(args);
    if (ep) std::rethrow_exception(ep);
    return s;
}

/**
 * formatString() test.
 */
void testFormatString()
{
    {
        std::string st(formatString("%s%c%s", "012", (char)0, "345"));
        for (size_t i = 0; i < st.size(); i++) {
            printf("%0x ", st[i]);
        }
        ::printf("\n size %zu\n", st.size());
        assert(st.size() == 7);
    }

    {
        std::string st(formatString(""));
        ::printf("%s %zu\n", st.c_str(), st.size());
    }

    {
        try {
            std::string st(formatString(nullptr));
            assert(false);
        } catch (std::runtime_error& e) {
        }
    }

    {
        std::string st(formatString("%s%s", "0123456789", "0123456789"));
        ::printf("%s %zu\n", st.c_str(), st.size());
        assert(st.size() == 20);
    }
}

static inline void checkCond(bool cond, const char *name, int line)
{
    if (!cond) {
        throw RT_ERR("check error: %s:%d", name, line);
    }
}

/**
 * Get unix time in double.
 */
static inline double getTime()
{
    struct timeval tv;
    ::gettimeofday(&tv, NULL);
    return static_cast<double>(tv.tv_sec) +
        static_cast<double>(tv.tv_usec) / 1000000.0;
}

/**
 * Libc error wrapper.
 */
class LibcError : public std::exception {
public:
    LibcError() : LibcError(errno) {}

    LibcError(int errnum, const char* msg = "libc_error: ")
        : errnum_(errnum)
        , str_(generateMessage(errnum, msg)) {}

    virtual const char *what() const noexcept {
        return str_.c_str();
    }
private:
    int errnum_;
    std::string str_;

    static std::string generateMessage(int errnum, const char* msg) {
        std::string s(msg);
        const size_t BUF_SIZE = 1024;
        char buf[BUF_SIZE];
        char *c = ::strerror_r(errnum, buf, BUF_SIZE);
        s += c;
        return s;
    }
};

/**
 * Convert size string with unit suffix to unsigned integer.
 */
uint64_t fromUnitIntString(const std::string &valStr)
{
    if (valStr.empty()) throw RT_ERR("Invalid argument.");
    char *endp;
    uint64_t val = ::strtoll(valStr.c_str(), &endp, 10);
    int shift = 0;
    switch (*endp) {
    case 'e': case 'E': shift = 60; break;
    case 'p': case 'P': shift = 50; break;
    case 't': case 'T': shift = 40; break;
    case 'g': case 'G': shift = 30; break;
    case 'm': case 'M': shift = 20; break;
    case 'k': case 'K': shift = 10; break;
    case '\0': break;
    default:
        throw RT_ERR("Invalid suffix charactor.");
    }
    if (((val << shift) >> shift) != val) throw RT_ERR("fromUnitIntString: overflow.");
    return val << shift;
}

/**
 * Unit suffixes:
 *   k: 2^10
 *   m: 2^20
 *   g: 2^30
 *   t: 2^40
 *   p: 2^50
 *   e: 2^60
 */
std::string toUnitIntString(uint64_t val)
{
    uint64_t mask = (1ULL << 10) - 1;
    const char units[] = " kmgtpezy";

    size_t i = 0;
    while (i < sizeof(units)) {
        if ((val & ~mask) != val) { break; }
        i++;
        val >>= 10;
    }

    if (0 < i && i < sizeof(units)) {
        return formatString("%" PRIu64 "%c", val, units[i]);
    } else {
        return formatString("%" PRIu64 "", val);
    }
}

void testUnitIntString()
{
    auto check = [](const std::string &s, uint64_t v) {
        CHECKx(fromUnitIntString(s) == v);
        CHECKx(toUnitIntString(v) == s);
    };
    check("12345", 12345);
    check("1k", 1ULL << 10);
    check("2m", 2ULL << 20);
    check("3g", 3ULL << 30);
    check("4t", 4ULL << 40);
    check("5p", 5ULL << 50);
    check("6e", 6ULL << 60);

    /* Overflow check. */
    try {
        fromUnitIntString("7e");
        CHECKx(true);
        fromUnitIntString("8e");
        CHECKx(false);
    } catch (std::runtime_error &e) {
    }
    try {
        fromUnitIntString("16383p");
        CHECKx(true);
        fromUnitIntString("16384p");
        CHECKx(false);
    } catch (std::runtime_error &e) {
    }
}

/**
 * Print byte array as hex list.
 */
template <typename ByteType>
void printByteArray(::FILE *fp, ByteType *data, size_t size)
{
    for (size_t i = 0; i < size; i++) {
        ::fprintf(fp, "%02x", static_cast<uint8_t>(data[i]));

        if (i % 64 == 63) { ::fprintf(fp, "\n"); }
    }
    if (size % 64 != 0) { ::fprintf(fp, "\n"); }
}

template <typename ByteType>
void printByteArray(ByteType *data, size_t size)
{
    printByteArray<ByteType>(::stdout, data, size);
}

/**
 * "0x" prefix will not be put.
 */
template <typename IntType>
std::string intToHexStr(IntType i)
{
    if (i == 0) return "0";
    std::string s;
    while (0 < i) {
        int m = i % 16;
        if (m < 10) {
            s.push_back(m + '0');
        } else {
            s.push_back(m - 10 + 'a');
        }
        i /= 16;
    }
    std::reverse(s.begin(), s.end());
    return s;
}

/**
 * The function does not assume "0x" prefix.
 */
template <typename IntType>
bool hexStrToInt(const std::string &hexStr, IntType &i)
{
    std::string s1;
    i = 0;
    for (char c : hexStr) {
        c = std::tolower(c);
        s1.push_back(c);
        if ('0' <= c && c <= '9') {
            i = i * 16 + (c - '0');
        } else if ('a' <= c && c <= 'f') {
            i = i * 16 + (c - 'a' + 10);
        } else {
            return false;
        }
    }
    return intToHexStr(i) == s1;
}

/**
 * Trim first and last spaces from a string.
 */
std::string trimSpace(const std::string &str, const std::string &spaces = " \t\r\n")
{
    auto isSpace = [&](char c) -> bool {
        for (char space : spaces) {
            if (c == space) return true;
        }
        return false;
    };

    size_t i0, i1;
    for (i0 = 0; i0 < str.size(); i0++) {
        if (!isSpace(str[i0])) break;
    }
    for (i1 = str.size(); 0 < i1; i1--) {
        if (!isSpace(str[i1 - 1])) break;
    }
    if (i0 < i1) {
        return str.substr(i0, i1 - i0);
    } else {
        return "";
    }
}

/**
 * Split a string with separators.
 */
std::vector<std::string> splitString(
    const std::string &str, const std::string &separators, bool isTrimSpace = true)
{
    auto isSep = [&](int c) -> bool {
        for (char sepChar : separators) {
            if (sepChar == c) return true;
        }
        return false;
    };
    auto findSep = [&](size_t pos) -> size_t {
        for (size_t i = pos; i < str.size(); i++) {
            if (isSep(str[i])) return i;
        }
        return std::string::npos;
    };

    std::vector<std::string> v;
    size_t cur = 0;
    while (true) {
        size_t pos = findSep(cur);
        if (pos == std::string::npos) {
            v.push_back(str.substr(cur));
            break;
        }
        v.push_back(str.substr(cur, pos - cur));
        cur = pos + 1;
    }
    if (isTrimSpace) {
        for (std::string &s : v) s = trimSpace(s);
    }
    return v;
}

bool hasPrefix(const std::string &name, const std::string &prefix)
{
    return name.substr(0, prefix.size()) == prefix;
}

std::string removePrefix(const std::string &name, const std::string &prefix)
{
    assert(hasPrefix(name, prefix));
    return name.substr(prefix.size());
}

/**
 * @name like "XXX_YYYYY"
 * @base like "YYYYY"
 * RETURN:
 *   prefix like "XXX_".
 */
std::string getPrefix(const std::string &name, const std::string &base)
{
    size_t s0 = name.size();
    size_t s1 = base.size();
    if (s0 <= s1) {
        throw std::runtime_error("There is no prefix.");
    }
    if (name.substr(s0 - s1) != base) {
        throw std::runtime_error("Base name differs.");
    }
    return name.substr(0, s0 - s1);
}

template <typename C>
void printList(const C &container)
{
    std::cout << "[";
    if (!container.empty()) {
        typename C::const_iterator it = container.cbegin();
        std::cout << *it;
        ++it;
        while (it != container.cend()) {
            std::cout << ", " << *it;
            ++it;
        }
    }
    std::cout << "]" << std::endl;
}

} //namespace util
} //namespace cybozu
