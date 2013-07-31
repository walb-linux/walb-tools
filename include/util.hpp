/**
 * @file
 * @brief Utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#ifndef UTIL_HPP
#define UTIL_HPP

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

#define CHECKx(cond)                                                \
    do {                                                            \
        if (!(cond)) {                                              \
            throw RT_ERR("check error: %s:%d", __func__, __LINE__); \
        }                                                           \
    } while (0)

#define DISABLE_COPY_AND_ASSIGN(ClassName)              \
    ClassName(const ClassName &rhs) = delete;           \
    ClassName &operator=(const ClassName &rhs) = delete

#define DISABLE_MOVE(ClassName)                     \
    ClassName(ClassName &&rhs) = delete;            \
    ClassName &operator=(ClassName &&rhs) = delete

namespace cybozu {
namespace util {

/**
 * Create a std::string using printf() like formatting.
 */
std::string formatString(const char * format, ...)
{
    char *p = nullptr;
    va_list args;
    va_start(args, format);
    int ret = ::vasprintf(&p, format, args);
    va_end(args);
    if (ret < 0) {
        ::free(p);
        throw std::runtime_error("vasprintf failed.");
    }
    std::string st(p, ret);
    ::free(p);
    return st;
}

/**
 * Formst string with va_list.
 */
std::string formatStringV(const char *format, va_list ap)
{
    char *p = nullptr;
    int ret = ::vasprintf(&p, format, ap);
    if (ret < 0) {
        ::free(p);
        throw std::runtime_error("vasprintf failed.");
    }
    std::string st(p, ret);
    ::free(p);
    return st;
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
    int shift = 0;
    std::string s(valStr);
    const size_t sz = s.size();

    if (sz == 0) { throw RT_ERR("Invalid argument."); }
    switch (s[sz - 1]) {
    case 'e':
    case 'E':
        shift += 10;
    case 'p':
    case 'P':
        shift += 10;
    case 't':
    case 'T':
        shift += 10;
    case 'g':
    case 'G':
        shift += 10;
    case 'm':
    case 'M':
        shift += 10;
    case 'k':
    case 'K':
        shift += 10;
        s.resize(sz - 1);
    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9':
        break;
    default:
        throw RT_ERR("Invalid suffix charactor.");
    }
    assert(shift < 64);

    for (size_t i = 0; i < sz - 1; i++) {
        if (!('0' <= valStr[i] && valStr[i] <= '9')) {
            throw RT_ERR("Not numeric charactor.");
        }
    }
    uint64_t val = ::atoll(s.c_str());
    uint64_t mask = uint64_t(-1);
    if (0 < shift) {
        mask = (1ULL << (64 - shift)) - 1;
    }
    if ((val & mask) != val) {
        throw RT_ERR("fromUnitIntString: overflow.");
    }
    return val << shift;
}

/**
 * Random number generator for UIntType.
 */
template<typename UIntType>
class Rand
{
private:
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_int_distribution<UIntType> dist_;
public:
    Rand()
        : rd_()
        , gen_(rd_())
        , dist_(0, UIntType(-1)) {}

    UIntType get() {
        return dist_(gen_);
    }

    void fill(void *data, size_t size) {
        UIntType *p = reinterpret_cast<UIntType *>(data);
        while (sizeof(*p) <= size) {
            *(p++) = get();
            size -= sizeof(*p);
        }
        if (0 < size) {
            UIntType i = get();
            ::memcpy(p, &i, size);
        }
    }
};

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
    char units[] = " kmgtpezy";

    size_t i = 0;
    while (i < sizeof(units)) {
        if ((val & mask) != val) { break; }
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
    return std::move(s);
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
    const std::string str, const std::string separators, bool isTrimSpace = true)
{
    std::string s(str);
    std::vector<std::string> v;

    auto isSep = [&](int c) -> bool {
        for (char sepChar : separators) {
            if (sepChar == c) return true;
        }
        return false;
    };
    auto findSep = [&](const std::string &s) -> size_t {
        for (size_t i = 0; i < s.size(); i++) {
            if (isSep(s[i])) return i;
        }
        return std::string::npos;
    };

    while (true) {
        size_t pos = findSep(s);
        if (pos == std::string::npos) {
            v.push_back(s);
            break;
        }
        v.push_back(s.substr(0, pos));
        s = s.substr(pos + 1);
    }
    if (isTrimSpace) {
        for (std::string &s : v) s = trimSpace(s);
    }
    return std::move(v);
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

#endif /* UTIL_HPP */
