#pragma once
#include <cassert>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include "util.hpp"

namespace cybozu {
namespace util {

inline void fstat(int fd, struct stat &s)
{
    if (fd < 0) throw RT_ERR("fstat: fd < 0");
    if (::fstat(fd, &s) < 0) {
        throw LibcError(errno, "fstat failed: ");
    }
}

inline bool isBlockDevice(int fd)
{
    if (fd < 0) throw RT_ERR("isBlockDevice: fd < 0");
    struct stat s;
    fstat(fd, s);
    return (s.st_mode & S_IFMT) == S_IFBLK;
}

inline uint32_t getLogicalBlockSize(int fd)
{
    if (fd < 0) throw RT_ERR("getLogicalBlockSize: fd < 0");
    if (!isBlockDevice(fd)) return 512;

    uint32_t lbs;
    if (::ioctl(fd, BLKSSZGET, &lbs) < 0) {
        throw LibcError(errno, "Geting logical block size failed: ");
    }
    assert(lbs > 0);
    return lbs;
}

inline uint32_t getPhysicalBlockSize(int fd)
{
    if (fd < 0) throw RT_ERR("getPhysicalBlockSize: fd < 0");
    if (!isBlockDevice(fd)) return 512;
    uint32_t pbs;
    if (::ioctl(fd, BLKPBSZGET, &pbs) < 0) {
        throw LibcError(errno, "Getting physical block size failed: ");
    }
    assert(pbs > 0);
    return pbs;
}

/**
 * Get device size in bytes.
 *
 * RETURN:
 *   device size [bytes].
 * EXCEPTION:
 *   std::runtime_error.
 */
inline uint64_t getBlockDeviceSize(int fd)
{
    if (isBlockDevice(fd)) {
        uint64_t size;
        if (::ioctl(fd, BLKGETSIZE64, &size) < 0) {
            throw LibcError(errno, "ioctl failed: ");
        }
        return size;
    } else {
        struct stat s;
        fstat(fd, s);
        return uint64_t(s.st_size);
    }
}

/**
 * CAUSION:
 *   This tries to discard the first physical block.
 */
inline bool isDiscardSupported(int fd)
{
    const uint32_t pbs = getPhysicalBlockSize(fd);
    uint64_t range[2] = {0, pbs};
    return ::ioctl(fd, BLKDISCARD, &range) == 0;
}

/**
 * @fd file descriptor.
 * @bgnLb begin offset [logical block].
 * @endLb end offset [logical block].
 */
inline void issueDiscard(int fd, uint64_t bgnLb, uint64_t endLb)
{
    assert(fd > 0);
    if (bgnLb >= endLb) {
        throw RT_ERR("bgnLb must be < endLb %" PRIu64 " %" PRIu64 ".", bgnLb, endLb);
    }
    uint64_t range[2] = {bgnLb << 9, endLb << 9};
    if (::ioctl(fd, BLKDISCARD, &range)) {
        throw LibcError(errno, __func__);
    }
}

}} // namespace cybozu::util
