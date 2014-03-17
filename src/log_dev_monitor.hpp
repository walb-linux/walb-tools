#pragma once
/**
 * @file
 * @brief walb log device polling.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <vector>
#include <string>
#include <map>

#include <sys/epoll.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "cybozu/exception.hpp"
#include "wdev_util.hpp"

namespace walb {

/**
 * This is thread safe.
 *
 * Typical usage:
 *   Thread 0: just call poll() and
 *     invoke another tasks with returned wdev name list.
 *   Thread i: call add()/del() when the control command received.
 */
class LogDevMonitor
{
private:
    class Fd
    {
        int fd;
        bool doAutoClose_;
        Fd(const Fd&) = delete;
        void operator=(const Fd&) = delete;
    public:
        /**
         * check fd if errMsg is set
         */
        explicit Fd(int fd, const std::string& errMsg = "")
            : fd(fd)
            , doAutoClose_(true)
        {
            if (!errMsg.empty() && fd < 0) throw cybozu::Exception(errMsg) << cybozu::ErrorNo();
        }
        void close() {
            if (fd < 0) return;
            if (::close(fd) < 0) throw cybozu::Exception("walb:log:Fd:close") << errno;
            fd = -1;
        }
        void dontClose() {
            doAutoClose_ = false;
        }
        ~Fd() noexcept {
            if (fd < 0 || !doAutoClose_) return;
            ::close(fd);
        }
        int operator()() const noexcept { return fd; }
    };
    mutable std::mutex mutex_;
    Fd efd_;
    std::vector<struct epoll_event> ev_;
    std::map<std::string, int> fdMap_; /* name, fd. */
    std::map<int, std::string> nameMap_; /* fd, name. */

public:
    explicit LogDevMonitor(unsigned int maxEvents = 1)
        : mutex_()
        , efd_(::epoll_create(maxEvents), "epoll_create failed.")
        , ev_(maxEvents)
        , fdMap_()
        , nameMap_() {
        assert(0 < maxEvents);
    }
    ~LogDevMonitor() noexcept {
        auto it = fdMap_.begin();
        while (it != fdMap_.end()) {
            int fd = it->second;
            if (::epoll_ctl(efd_(), EPOLL_CTL_DEL, fd, nullptr) < 0) {
                /* failed but do nothing. */
            }
            ::close(fd);
            it = fdMap_.erase(it);
        }
        nameMap_.clear();
    }
    /**
     * DO NOT call this from multiple threads.
     * This will throw an exception if caught SIGINT.
     *
     * TODO: signal handling.
     *   Should we use epoll_pwait()?
     */
    std::vector<std::string> poll(int timeoutMs = -1) {
        std::vector<std::string> v;
        int nfds = ::epoll_wait(efd_(), &ev_[0], ev_.size(), timeoutMs);
        if (nfds < 0) {
            /* TODO: logging. */
            return v;
        }
        std::lock_guard<std::mutex> lk(mutex_);
        for (int i = 0; i < nfds; i++) {
            int fd = ev_[i].data.fd;
            std::string name = getName(fd);
            if (!name.empty()) {
                v.push_back(name);
                char buf[4096];
                ::lseek(fd, 0, SEEK_SET);
                UNUSED int s = ::read(fd, buf, 4096);
#ifdef DEBUG
                ::printf("read %d bytes\n", s);
#endif
            }
        }
        return v;
    }
    /**
     * If already exists, call del() and add().
     */
    bool addForce(const std::string &wdevName) {
        std::lock_guard<std::mutex> lk(mutex_);
        delNolock(wdevName);
        return addNolock(wdevName);
    }
    /**
     * Add a walb device to the polling targets.
     * @wdevName walb device name. This is unique in a host.
     */
    bool add(const std::string &wdevName) {
        std::lock_guard<std::mutex> lk(mutex_);
        return addNolock(wdevName);
    }
    /**
     * Del a walb device from the polling targets.
     */
    void del(const std::string &wdevName) {
        std::lock_guard<std::mutex> lk(mutex_);
        delNolock(wdevName);
    }
    /**
     * Get list of wdevName and polling fd.
     */
    std::vector<std::pair<std::string, int>> list() const {
        std::lock_guard<std::mutex> lk(mutex_);
        std::vector<std::pair<std::string, int>> v;
        for (const auto &p : fdMap_) {
            v.push_back(p);
        }
        return v;
    }
private:
    bool addName(const std::string &wdevName, int fd) {
        auto p0 = fdMap_.insert({wdevName, fd});
        if (!p0.second) {
            return false;
        }
        auto p1 = nameMap_.insert({fd, wdevName});
        if (!p1.second) {
            fdMap_.erase(p0.first);
            return false;
        }
        assert(fdMap_.size() == nameMap_.size());
        return true;
    }
    void delName(const std::string &wdevName, int fd) {
        auto it0 = fdMap_.find(wdevName);
        if (it0 != fdMap_.end()) {
            assert(it0->second == fd);
            fdMap_.erase(it0);
        }
        auto it1 = nameMap_.find(fd);
        if (it1 != nameMap_.end()) {
            assert(it1->second == wdevName);
            nameMap_.erase(it1);
        }
        assert(fdMap_.size() == nameMap_.size());
    }
    /**
     * RETURN:
     *   -1 if not found.
     */
    int getFd(const std::string &wdevName) const {
        auto it = fdMap_.find(wdevName);
        if (it == fdMap_.end()) return -1;
        return it->second;
    }
    /**
     * RETURN:
     *   "" if not found.
     */
    std::string getName(int fd) const {
        auto it = nameMap_.find(fd);
        if (it == nameMap_.end()) return "";
        return it->second;
    }
    /**
     * Add an walb device.
     * RETURN:
     *   false when some error occurs.
     */
    bool addNolock(const std::string &wdevName) {
        std::string path = device::getPollingPath(wdevName);
        Fd fd(::open(path.c_str(), O_RDONLY), std::string("addNolock:can't open ") + path);

        if (!addName(wdevName, fd())) return false;

        struct epoll_event ev;
        ::memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = fd();
        if (::epoll_ctl(efd_(), EPOLL_CTL_ADD, fd(), &ev) < 0) {
            delName(wdevName, fd());
            return false;
        }
        fd.dontClose();
        return true;
    }
    /**
     * Delete an walb device.
     * If not found, do nothing.
     */
    void delNolock(const std::string &wdevName) {
        Fd fd(getFd(wdevName));
        if (fd() < 0) return;
        delName(wdevName, fd());
        if (::epoll_ctl(efd_(), EPOLL_CTL_DEL, fd(), nullptr) < 0) {
            throw std::runtime_error("epoll_ctl failed.");
        }
        fd.close();
    }
};

} //namespace walb
