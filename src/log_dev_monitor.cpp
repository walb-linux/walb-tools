#include "log_dev_monitor.hpp"

namespace walb {

LogDevMonitor::~LogDevMonitor() noexcept
{
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

StrVec LogDevMonitor::poll(int timeoutMs)
{
    const char *const FUNC = __func__;
    StrVec v;
    int nfds = ::epoll_wait(efd_(), &ev_[0], ev_.size(), timeoutMs);
    if (nfds < 0) {
        LOGs.error() << FUNC << "epoll_wait failed" << cybozu::ErrorNo();
        return v;
    }
    std::lock_guard<std::mutex> lk(mutex_);
    for (int i = 0; i < nfds; i++) {
        int fd = ev_[i].data.fd;
        std::string name = getName(fd);
        if (!name.empty()) {
            v.push_back(name);
#if 0
            printEvent(ev_[i]);
#endif
            resetTrigger(fd);
        }
    }
    return v;
}

std::vector<std::pair<std::string, int>> LogDevMonitor::list() const
{
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<std::pair<std::string, int>> v;
    for (const auto &p : fdMap_) {
        v.push_back(p);
    }
    return v;
}

bool LogDevMonitor::addName(const std::string &wdevName, int fd)
{
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

void LogDevMonitor::delName(const std::string &wdevName, int fd)
{
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

int LogDevMonitor::getFd(const std::string &wdevName) const
{
    auto it = fdMap_.find(wdevName);
    if (it == fdMap_.end()) return -1;
    return it->second;
}

bool LogDevMonitor::addNolock(const std::string &wdevName)
{
    std::string path = device::getPollingPath(wdevName);
    Fd fd(::open(path.c_str(), O_RDONLY), std::string("addNolock:can't open ") + path);

    if (!addName(wdevName, fd())) return false;

    resetTrigger(fd());

    struct epoll_event ev;
    ::memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLPRI;
    ev.data.fd = fd();
    if (::epoll_ctl(efd_(), EPOLL_CTL_ADD, fd(), &ev) < 0) {
        delName(wdevName, fd());
        return false;
    }
    fd.dontClose();
    return true;
}

void LogDevMonitor::delNolock(const std::string &wdevName)
{
    Fd fd(getFd(wdevName));
    if (fd() < 0) return;
    delName(wdevName, fd());
    if (::epoll_ctl(efd_(), EPOLL_CTL_DEL, fd(), nullptr) < 0) {
        throw std::runtime_error("epoll_ctl failed.");
    }
    fd.close();
}

void LogDevMonitor::resetTrigger(int fd)
{
    const char *const FUNC = __func__;
    if (::lseek(fd, 0, SEEK_SET) < 0) {
        // ignore.
        return;
    }
    char buf[4096];
    size_t size = 0;
    for (;;) {
        const int s = ::read(fd, buf, 4096);
        if (s <= 0) break;
        size += s;
    }
    LOGs.debug() << FUNC << "read bytes" << size;
}

void LogDevMonitor::printEvent(const struct epoll_event &ev) const
{
    ::printf("%u %u %u %u\n"
             , (ev.events & EPOLLIN) != 0
             , (ev.events & EPOLLERR) != 0
             , (ev.events & EPOLLPRI) != 0
             , (ev.events & EPOLLHUP) != 0
        );
}

} //namespace walb
