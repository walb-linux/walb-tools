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
#include "walb_logger.hpp"
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
    explicit LogDevMonitor(uint32_t maxEvents = 1)
        : mutex_()
        , efd_(::epoll_create(maxEvents), "epoll_create failed.")
        , ev_(maxEvents)
        , fdMap_()
        , nameMap_() {
        assert(0 < maxEvents);
    }

    ~LogDevMonitor() noexcept;

    /**
     * DO NOT call this from multiple threads.
     * This will throw an exception if caught SIGINT.
     */
    StrVec poll(int timeoutMs = -1);

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
    std::vector<std::pair<std::string, int>> list() const;

    bool exists(const std::string &wdevName) const {
        std::lock_guard<std::mutex> lk(mutex_);
        return fdMap_.find(wdevName) != fdMap_.cend();
    }
private:
    bool addName(const std::string &wdevName, int fd);
    void delName(const std::string &wdevName, int fd);

    /**
     * RETURN:
     *   -1 if not found.
     */
    int getFd(const std::string &wdevName) const;

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
    bool addNolock(const std::string &wdevName);

    /**
     * Delete an walb device.
     * If not found, do nothing.
     */
    void delNolock(const std::string &wdevName);

    void resetTrigger(int fd);

    /* debug */
    UNUSED
    void printEvent(const struct epoll_event &ev) const;
};

} //namespace walb
