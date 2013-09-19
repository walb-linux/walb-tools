#pragma once
/**
 * @file
 * @brief flock wrapper
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <stdexcept>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>

namespace cybozu {
namespace file {

namespace internal {

class Locker
{
private:
    int fd_;
    bool locked_;

public:
    Locker() : fd_(-1), locked_(false) {}

    void lock(const std::string &path, bool isShared) {
        open(path);
        int ret = ::flock(fd_, getFlag(isShared));
        if (ret) {
            throw std::runtime_error("flock failed.");
        }
        locked_ = true;
    }

    bool tryLock(const std::string &path, bool isShared) {
        if (locked_) { return false; }
        open(path);
        int ret = ::flock(fd_, getFlag(isShared) | LOCK_NB);
        locked_ = (ret == 0);
        if (!locked_) { close(); }
        return locked_;
    }

    void unlock() {
        if (locked_) {
            ::flock(fd_, LOCK_UN);
            locked_ = false;
        }
        close();
    }
private:
    void open(const std::string &path) {
        if (0 < fd_) { return; }
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ < 0) { throw std::runtime_error("open failed."); }
    }

    void close() {
        if (fd_ < 0) { return; }
        if (::close(fd_) < 0) { throw std::runtime_error("close failed."); }
        fd_ = -1;
    }

    int getFlag(bool isShared) const {
        return isShared ? LOCK_SH : LOCK_EX;
    }
};

} //namespace internal

class Lock /* final */
{
private:
    internal::Locker l_;
public:
    Lock(const std::string &path) : l_() { l_.lock(path, false); }
    ~Lock() throw () { try { l_.unlock(); } catch (...) {} }
};

class SharedLock /* final */
{
private:
    internal::Locker l_;
public:
    SharedLock(const std::string &path) : l_() { l_.lock(path, true); }
    ~SharedLock() throw () { try { l_.unlock(); } catch (...) {} }
};

class TryLock /* final */
{
private:
    internal::Locker l_;
    std::string path_;
public:
    TryLock(const std::string &path) : l_(), path_(path) {}
    ~TryLock() throw () { try { l_.unlock(); } catch (...) {} }
    bool lock() { return l_.tryLock(path_, false); }
    bool lockShared() { return l_.tryLock(path_, true); }
    void unlock() { l_.unlock(); }
};

}} //namespace cybozu::file
