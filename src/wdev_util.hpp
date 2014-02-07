#pragma once

#include "cybozu/exception.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "fileio.hpp"
#include "walb/ioctl.h"
#include "walb/block_size.h"

namespace walb {

namespace wdev_impl {

inline void invokeWdevIoctl(const std::string& wdevPath, struct walb_ctl *ctl, int openFlag)
{
    cybozu::util::FileOpener f(wdevPath, openFlag);
    int ret = ::ioctl(f.fd(), WALB_IOCTL_WDEV, ctl);
    if (ret < 0) {
        throw cybozu::Exception("invokeWdevIoctl:ioctl") << cybozu::ErrorNo();
    }
}

} // walb::wdev_impl

inline void resetWal(const std::string& wdevPath)
{
    struct walb_ctl ctl;
    ::memset(&ctl, 0, sizeof(ctl));
    ctl.command = WALB_IOCTL_CLEAR_LOG;

    wdev_impl::invokeWdevIoctl(wdevPath, &ctl, O_RDWR);
}

inline uint64_t getSizeLb(const std::string& wdevPath)
{
    cybozu::util::FileOpener f(wdevPath, O_RDONLY);
    uint64_t size;
    if (::ioctl(f.fd(), BLKGETSIZE64, &size) < 0) {
        throw cybozu::Exception("getSizeLb:bad ioctl") << cybozu::ErrorNo();
    }
    return size / LOGICAL_BLOCK_SIZE;
}

} // walb

