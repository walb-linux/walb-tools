/**
 * @file
 * @brief File watcher header.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef FILE_WATCH_HPP
#define FILE_WATCH_HPP

#include <stdexcept>
#include <vector>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/inotify.h>
#include <sys/epoll.h>
#include <errno.h>

namespace cybozu {
namespace util {

/**
 * Event data.
 */
struct FileEvent
{
    std::string path;
    std::string file;
    uint32_t mask;
    uint32_t cookie;
};

/**
 * Watch files/directories using inotify and epoll.
 */
class FileWatcher /* final */
{
private:
    typedef std::map<std::string, int> WdMap;
    typedef std::map<int, std::string> PathMap;

    int ifd_;
    int efd_;
    struct epoll_event ev_;
    WdMap wdMap_;
    PathMap pathMap_;

public:
    FileWatcher()
        : ifd_(-1)
        , efd_(-1)
        , ev_()
        , wdMap_()
        , pathMap_() {

        ifd_ = ::inotify_init();
        if (ifd_ < 0) { throw std::runtime_error("inotify_create failed."); }
        efd_ = ::epoll_create(1);
        if (efd_ < 0) { throw std::runtime_error("epoll_create failed."); }

        ctlAdd();
    }

    ~FileWatcher() {
        close();
    }

    void close() {
        if (0 < efd_) { ::close(efd_); efd_ = -1; }
        if (0 < ifd_) { ::close(ifd_); ifd_ = -1; }
    }

    void watch(const std::string &filePath, uint32_t mask = IN_ALL_EVENTS) {
        int wd = ::inotify_add_watch(ifd_, filePath.c_str(), mask);
        if (wd < 0) {
            throw std::runtime_error("inotify_add_watch failed.");
        }
        {
            std::pair<WdMap::iterator, bool> ret =
                wdMap_.insert(std::make_pair(filePath, wd));
            if (!ret.second) {
                throw std::runtime_error("The file has been registered already.");
            }
        }
        {
            std::pair<PathMap::iterator, bool> ret =
                pathMap_.insert(std::make_pair(wd, filePath));
            if (!ret.second) {
                throw std::runtime_error("The file has been registered already.");
            }
        }
    }

    void unwatch(const std::string &filePath) {
        WdMap::iterator it0 = wdMap_.find(filePath);
        if (it0 == wdMap_.end()) {
            throw std::runtime_error("The file is not registered.");
        }
        int wd = it0->second;
        wdMap_.erase(it0);
        PathMap::iterator it1 = pathMap_.find(wd);
        if (it1 == pathMap_.end()) {
            throw std::runtime_error("Not found in pathMap.");
        }
        pathMap_.erase(it1);

        if (::inotify_rm_watch(ifd_, wd) < 0) {
            throw std::runtime_error("inotify_rm_watch failed.");
        }
    }

    bool poll(std::vector<FileEvent> &v, int timeoutMs = -1) {
        const size_t BUFFER_SIZE = 2U << 16;
        char buf[BUFFER_SIZE];

        struct epoll_event ev;
        ::memset(&ev, 0, sizeof(ev));
        int nfds = epoll_wait(efd_, &ev, 1, timeoutMs);
        if (nfds < 0) {
            throw std::runtime_error("epoll_wait failed.");
        }
        if (nfds == 0) { return false; }
        assert(nfds == 1);
        assert(ev.data.fd == ifd_);

        ssize_t r = 0;
        // ::printf("ifd %d\n", ifd_); /* debug */
        r = ::read(ifd_, buf, BUFFER_SIZE);
        if (r < 0) {
            throw std::runtime_error("read inotify_event failed.");
        }
        if (r < static_cast<ssize_t>(sizeof(struct inotify_event))) {
            throw std::runtime_error("inotiry_event read size too small.");
        }
        ssize_t off = 0;
        while (off < r) {
            struct inotify_event *iev =
                reinterpret_cast<struct inotify_event *>(&buf[off]);

            /* Read more if the event has been read partially. */
            size_t remaining = r - off;
            if (remaining < sizeof(*iev) || remaining < sizeof(*iev) + iev->len) {
                ::memmove(&buf[0], &buf[off], remaining);
                r = remaining + ::read(ifd_, &buf[remaining], BUFFER_SIZE - remaining);
                if (r < 0) {
                    throw std::runtime_error("read failed.");
                }
                off = 0;
            }

            FileEvent fev;
            fev.mask = iev->mask;
            fev.cookie = iev->cookie;
            PathMap::iterator it = pathMap_.find(iev->wd);
            if (it == pathMap_.end()) {
                throw std::runtime_error("Not found the path.");
            }
            fev.path = it->second;
            if (0 < iev->len) {
                fev.file = iev->name;
            }
            v.push_back(fev);

            off += sizeof(*iev) + iev->len;
        }
        return true;
    }

private:
    FileWatcher(const FileWatcher &);
    FileWatcher &operator=(const FileWatcher &);

    void ctlAdd() {
        ::memset(&ev_, 0, sizeof(ev_));
        ev_.events = EPOLLIN;
        ev_.data.fd = ifd_;
        if (::epoll_ctl(efd_, EPOLL_CTL_ADD, ifd_, &ev_) < 0) {
            throw std::runtime_error("epoll_ctl failed.");
        }
    }

    void ctlDel() {
        if (::epoll_ctl(efd_, EPOLL_CTL_DEL, ifd_, NULL) < 0) {
            throw std::runtime_error("epoll_ctl failed.");
        }
    }
};

}} //namesapce cybozu::util

#endif /* FILE_WATCH_HPP */
