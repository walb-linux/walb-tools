/**
 * @file
 * @brief iostream for file descriptors.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <iostream>
#include <streambuf>
#include <ios>
#include <string>
#include <cstring>
#include <deque>
#include <vector>
#include <cassert>
#include <unistd.h>

#ifndef CYBOZU_FDSTREAM_HPP
#define CYBOZU_FDSTREAM_HPP

namespace cybozu {

namespace fdbuf_local {

constexpr size_t BUFFER_SIZE = 4096;

} //namespace fdbuf_local

/**
 * Stream buffer for file descriptor.
 */
class fdbuf : public std::streambuf
{
private:
    int fd_;
    const bool isSeekable_;
    const bool doClose_;
    std::deque<char> q_; /* for read */
    std::vector<char> buf_; /* for read. */

    using traits = std::char_traits<char>;

public:
    explicit fdbuf(int fd, bool isSeekable = false, bool doClose = false)
        : std::streambuf(), fd_(fd), isSeekable_(isSeekable), doClose_(doClose)
        , q_(), buf_(fdbuf_local::BUFFER_SIZE) {
    }
    ~fdbuf() noexcept override {
        if (doClose_ && 0 <= fd_) ::close(fd_);
    }
    int overflow(int ch = traits::eof()) override {
        if (ch != traits::eof()) {
            char c = ch;
            ssize_t s = ::write(fd_, &c, 1);
            if (s <= 0) throw std::ios_base::failure("write() failed.");
        }
        return ch;
    }
    int uflow() override {
        int c = underflow();
        if (c == traits::eof()) return c;
        assert(!q_.empty());
        q_.pop_front();
        return c;
    }
    int underflow() override {
        if (q_.empty()) {
            std::streamsize s = readsome(fdbuf_local::BUFFER_SIZE);
            for (std::streamsize i = 0; i < s; i++) {
                q_.push_back(buf_[i]);
            }
        }
        if (q_.empty()) {
            return traits::eof();
        } else {
            return q_.front();
        }
    }
    int fd() const { return fd_; }
protected:
    std::streamsize xsputn(const char *str, std::streamsize count) override {
        assert(0 < count);
        std::streamsize s = ::write(fd_, str, count);
        if (s < 0) throw std::ios_base::failure("write() failed.");
        return s;
    }
    std::streamsize xsgetn(char *str, std::streamsize count) override {
        std::streamsize s0 = 0;
        assert(0 < count);
        while (!q_.empty() && 0 < count) {
            *str = q_.front();
            q_.pop_front();
            ++str;
            --count;
            ++s0;
        }
        if (count == 0) return s0;

        std::streamsize s1 = readsome(count - s0);
        ::memcpy(str, &buf_[0], s1);
        return s0 + s1;
    }
    int pbackfail(int c = traits::eof()) override {
        if (c == traits::eof()) return c;
        q_.push_front(c);
        return c;
    }
    std::streampos seekoff(std::streamoff off, std::ios_base::seekdir dir,
                           std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override {
        if (!isSeekable_) return -1;
        q_.clear();
        checkWhich(which);
        int whence = SEEK_CUR;
        if (dir == std::ios_base::beg) {
            whence = SEEK_SET;
        } else if (dir == std::ios_base::end) {
            whence = SEEK_END;
        }
        return ::lseek(fd_, off, whence);
    }
    std::streampos seekpos(std::streampos pos,
                           std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override {
        if (!isSeekable_) return -1;
        q_.clear();
        checkWhich(which);
        return ::lseek(fd_, pos, SEEK_SET);
    }
private:
    void checkWhich(std::ios_base::openmode which) const {
        if (which != (std::ios_base::in | std::ios_base::out)) {
            throw std::ios_base::failure("openmode is not valid.");
        }
    }
    std::streamsize readsome(size_t count) {
        std::streamsize s = ::read(fd_, &buf_[0], std::min(buf_.size(), count));
        if (s < 0) throw std::ios_base::failure("read() failed.");
        return s;
    }
};

class ofdstream : public std::ostream
{
private:
    fdbuf buf_;
public:
    explicit ofdstream(int fd, bool isSeekable = false, bool doClose = false)
        : std::ostream(&buf_), buf_(fd, isSeekable, doClose) {
    }
    int fd() const { return buf_.fd(); }
};

class ifdstream : public std::istream
{
private:
    fdbuf buf_;
public:
    explicit ifdstream(int fd, bool isSeekable = false, bool doClose = false)
        : std::istream(&buf_), buf_(fd, isSeekable, doClose) {
    }
    int fd() const { return buf_.fd(); }
};

} //namespace cybozu

#endif /* CYBOZU_FDSTREAM_HPP */
