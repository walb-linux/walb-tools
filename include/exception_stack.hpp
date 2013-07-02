/**
 * @file
 * @brief Definitions exception stack and related macros.
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2009 Cybozu Labs, Inc.
 */
#ifndef EXCEPTION_STACK_HPP
#define EXCEPTION_STACK_HPP

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <cassert>
#include <exception>

#define CHECK_AND_THROW_ERROR_STACK(pred, msg)      \
    do {                                            \
        if (!(pred)) {                              \
            throw cybozu::except::ExceptionStack(   \
                (msg), __FILE__, __LINE__);         \
        }                                           \
    } while (0)

#define THROW_ERROR_STACK(msg) CHECK_AND_THROW_ERROR_STACK(false, msg)

#define RETHROW_ERROR_STACK(e, msg)             \
    do {                                        \
        e.add((msg), __FILE__, __LINE__);       \
        throw;                                  \
    } while (0)

namespace cybozu {
namespace except {

/**
 * @brief Exception for stream operator<< and operator>>.
 *
 * Stacked error messages are supported.
 */
class ExceptionStack
    : public std::exception
{
private:
    std::vector<std::string> msgs_;
    std::vector<std::string> files_;
    std::vector<int> lines_;
    mutable std::string what_;

public:
    ExceptionStack(const std::string& errMessage,
                   const std::string& file, int line)
        : msgs_()
        , files_()
        , lines_()
        , what_() {
        add(errMessage, file, line);
    }

    virtual ~ExceptionStack() throw () {}

    const char *what() throw () {
        what_ = sprint();
        return what_.c_str();
    }

    ExceptionStack &add(const std::string &errMessage,
                        const std::string &file, int line) {
        msgs_.push_back(errMessage);
        files_.push_back(file);
        lines_.push_back(line);
        return *this;
    }

    std::string sprint() const {
        std::stringstream ss;
        print(ss);
        return ss.str();
    }

    void print(std::ostream& os = std::cerr) const {
        const size_t n = msgs_.size();
        assert(n == files_.size());
        assert(n == lines_.size());
        os << "ExcpetionStack: \n";
        for (size_t i = 0; i < n; i++) {
            os << "    " << i << ": " << msgs_[i]
               << " ("<< files_[i]
               << ":" << lines_[i]
               << ")\n";
        }
    }
};

}} //namespace cybozu::except

#endif /* EXCEPTION_STACK_HPP */
