#pragma once
/**
 * @file
 * @brief Backtrace utility.
 * @author HOSHINO Takashi.
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <cassert>
#include <string>
#include <vector>
#include <memory>
#include <execinfo.h>
#include <cxxabi.h>

#ifdef assert_bt
#error
#endif

#ifdef DEBUG
#define assert_bt(cond) do {                    \
        if (!(cond)) {                          \
            cybozu::printBacktrace();           \
            assert(cond);                       \
        }                                       \
    } while (false)
#else
#define assert_bt(cond)
#endif

namespace cybozu {

/**
 * Get backtrace string list.
 * @depth max number of addresses.
 * @isMangle try to mangle symbols if true.
 *
 * RETURN:
 *   backtrace string list.
 *
 * backtrace line format:
 *   FILE_PATH(SYMBOL+OFFSET) [ADDRESS]
 *   FILE_PATH() [ADDRESS]
 *
 * g++ options '-g -rdynamic' are required to get symbols.
 */
inline std::vector<std::string> getBacktrace(size_t depth = 100, bool isMangle = true)
{
    auto demangle = [](const std::string &str) -> std::string {
        size_t n0 = str.find('(');
        size_t n1 = str.find('+');
        if (n0 == std::string::npos || n1 == std::string::npos) return str;
        std::string s0 = str.substr(0, n0 + 1);
        std::string s = str.substr(n0 + 1, n1 - n0 - 1);
        std::string s1 = str.substr(n1);
        if (s.empty()) return str;
        int status;
        char *p = abi::__cxa_demangle(s.c_str(), nullptr, nullptr, &status);
        if (status != 0) return str;
        std::unique_ptr<char, void (*)(void *) noexcept> up(p, ::free);
        return s0 + std::string(up.get()) + s1;
    };

    std::vector<void *> traces(depth);
    size_t nptrs = ::backtrace(&traces[0], depth);
    char **pp = ::backtrace_symbols(&traces[0], nptrs);
    if (!pp) throw std::bad_alloc();
    std::unique_ptr<char *, void (*)(void *) noexcept> up(pp, ::free);
    std::vector<std::string> ret;
    for (size_t i = 0; i < nptrs; i++) {
        ret.push_back(isMangle ? demangle(up.get()[i]) : up.get()[i]);
    }
    return ret;
};

inline void printBacktrace(::FILE *fp = ::stderr)
{
    for (std::string &s : getBacktrace()) {
        ::fprintf(fp, "%s\n", s.c_str());
    }
}

} //namespace cybozu.
