/**
 * @file
 * @brief flock wrapper
 * @author Takashi HOSHINO <hoshino@labs.cybozu.co.jp>
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef FLOCK_HPP
#define FLOCK_HPP

#include <string>
#include <stdexcept>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

namespace cybozu {
namespace file {

namespace internal {

class Locker
{
private:
    int fd_;
    bool openByMe_;
    bool locked_;

public:
    Locker() : fd_(-1), openByMe_(false), locked_(false) {}

    void lock(const std::string &path, bool isShared) {
        if (locked_) { throw std::runtime_error("already locked."); }
        open(path);
        openByMe_ = true;
        lockDetail(isShared);
    }

    void lock(int fd, bool isShared) {
        if (locked_) { throw std::runtime_error("already locked."); }
        fd_ = fd;
        openByMe_ = false;
        lockDetail(isShared);
    }

    bool tryLock(const std::string &path, bool isShared) {
        if (locked_) { return false; }
        open(path);
        openByMe_ = true;
        bool ret = tryLockDetail(isShared);
        if (!ret) { close(); }
        return ret;
    }

    bool tryLock(int fd, bool isShared) {
        if (locked_) { return false; }
        fd_ = fd;
        openByMe_ = false;
        bool ret = tryLockDetail(isShared);
        if (!ret) { close(); }
        return ret;
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
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0) { throw std::runtime_error("open failed."); }
    }

    void close() {
        if (fd_ < 0) { return; }
        if (openByMe_ && ::close(fd_) < 0) {
            throw std::runtime_error("close failed.");
        }
        fd_ = -1;
    }

    int getFlag(bool isShared) const {
        return isShared ? LOCK_SH : LOCK_EX;
    }

    void lockDetail(bool isShared) {
        int ret = ::flock(fd_, getFlag(isShared));
        if (ret) { throw std::runtime_error("flock failed."); }
        locked_ = true;
    }

    bool tryLockDetail(bool isShared) {
        int ret = ::flock(fd_, getFlag(isShared) | LOCK_NB);
        locked_ = (ret == 0);
        return locked_;
    }
};

} //namespace internal

/**
 * Exclusive lock.
 */
class ExLock /* final */
{
private:
    internal::Locker l_;
public:
    ExLock(const std::string &path) : l_() { l_.lock(path, false); }
    ExLock(int fd) : l_() { l_.lock(fd, false); }
    ~ExLock() throw () { try { l_.unlock(); } catch (...) {} }
};

/**
 * Shared lock.
 */
class ShLock /* final */
{
private:
    internal::Locker l_;
public:
    ShLock(const std::string &path) : l_() { l_.lock(path, true); }
    ShLock(int fd) : l_() { l_.lock(fd, true); }
    ~ShLock() throw () { try { l_.unlock(); } catch (...) {} }
};

/**
 * Try lock.
 */
class TryLock /* final */
{
private:
    int fd_;
    std::string path_;
    internal::Locker l_;
public:
    TryLock(const std::string &path) : fd_(-1), path_(path), l_() {}
    TryLock(int fd) : fd_(fd), path_(), l_() {}
    ~TryLock() throw () { try { l_.unlock(); } catch (...) {} }
    bool tryEx() { return lockDetail(false); }
    bool trySh() { return lockDetail(true); }
private:
    bool lockDetail(bool isShared) {
        if (0 < fd_) {
            return l_.tryLock(fd_, isShared);
        } else {
            return l_.tryLock(path_, isShared);
        }
    }
};

}} //namespace cybozu::file

#endif /* FLOCK_HPP */
