#pragma once

#include <type_traits>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "util.hpp"
#include "fileio.hpp"
#include "file_path.hpp"
#include "process.hpp"
#include "fdstream.hpp"
#include "walb/ioctl.h"
#include "walb/block_size.h"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"
#include "walb_types.hpp"
#include "bdev_util.hpp"

namespace walb {
namespace device {

static const std::string WDEV_PATH_PREFIX = "/dev/walb/";

inline void invokeWdevIoctl(const std::string& wdevPath, struct walb_ctl *ctl,
                            const char *msg = "")
{
    if (!msg || !*msg) msg = __func__;
    cybozu::util::File file(wdevPath, O_RDWR);
    if (::ioctl(file.fd(), WALB_IOCTL_WDEV, ctl) < 0) {
        throw cybozu::Exception(msg) << "ioctl error" << cybozu::ErrorNo();
    }
    file.close();
}

/**
 * IntType: int, uint32_t, uint64_t.
 */
template <typename IntType>
IntType getValueByIoctl(const std::string& wdevPath, int command)
{
    struct walb_ctl ctl;
    ::memset(&ctl, 0, sizeof(ctl));
    ctl.command = command;

    invokeWdevIoctl(wdevPath, &ctl, __func__);

    if (std::is_same<IntType, int>::value) {
        return ctl.val_int;
    }
    if (std::is_same<IntType, uint32_t>::value) {
        return ctl.val_u32;
    }
    if (std::is_same<IntType, uint64_t>::value) {
        return ctl.val_u64;
    }
    throw cybozu::Exception(__func__) << "not supported type.";
}

/**
 * IntType: int, uint32_t, uint64_t.
 */
template <typename IntType>
void setValueByIoctl(const std::string& wdevPath, int command, IntType value)
{
    struct walb_ctl ctl;
    ::memset(&ctl, 0, sizeof(ctl));
    ctl.command = command;

    if (std::is_same<IntType, int>::value) {
        ctl.val_int = value;
    } else if (std::is_same<IntType, uint32_t>::value) {
        ctl.val_u32 = value;
    } else if (std::is_same<IntType, uint64_t>::value) {
        ctl.val_u64 = value;
    } else {
        throw cybozu::Exception(__func__) << "not supported type.";
    }

    invokeWdevIoctl(wdevPath, &ctl, __func__);
}

/**
 * Get a lsid of the volume using ioctl.
 *
 * @command
 *   WALB_IOCTL_GET_XXX_LSID defined walb/ioctl.h.
 *   XXX: OLDEST, PERMANENT, WRITTEN, PERMANENT, COMPLETED.
 */
inline uint64_t getLsid(const std::string& wdevPath, int command)
{
    uint64_t lsid = getValueByIoctl<uint64_t>(wdevPath, command);
    if (lsid == uint64_t(-1)) {
        throw cybozu::Exception("getLsid:invalid lsid");
    }
    return lsid;
}

inline void setOldestLsid(const std::string& wdevPath, uint64_t lsid)
{
    setValueByIoctl<uint64_t>(wdevPath, WALB_IOCTL_SET_OLDEST_LSID, lsid);
}

namespace local {

/**
 * Parse "XXX:YYY" string where XXX is major id and YYY is minor id.
 */
inline std::pair<uint32_t, uint32_t> parseDeviceIdStr(const std::string& devIdStr)
{
    const char *const FUNC = __func__;
    StrVec v = cybozu::Split(devIdStr, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception(FUNC) << "parse error" << devIdStr;
    }
    const uint32_t major = cybozu::atoi(v[0]);
    const uint32_t minor = cybozu::atoi(v[1]);
    return std::make_pair(major, minor);
}

/**
 * Replace charactor x to y in a string.
 */
inline void replaceChar(std::string &s, const char x, const char y)
{
    for (;;) {
        size_t n = s.find(x);
        if (n == std::string::npos) break;
        s[n] = y;
    }
}

/**
 * Get block device path from major and minor id using lsblk command.
 */
inline std::string getDevPathFromId(uint32_t major, uint32_t minor)
{
    const char *const FUNC = __func__;
    const std::string res = cybozu::process::call(
        "/bin/lsblk", { "-l", "-n", "-r", "-o", "KNAME,MAJ:MIN" });
    for (const std::string& line : cybozu::Split(res, '\n')) {
        const StrVec v = cybozu::Split(line, ' ');
        if (v.size() != 2) {
            throw cybozu::Exception(FUNC) << "lsblk output parse error" << line;
        }
        std::string name = v[0];
        uint32_t majorX, minorX;
        std::tie(majorX, minorX) = local::parseDeviceIdStr(v[1]);
        if (major != majorX || minor != minorX) continue;
        replaceChar(name, '!', '/');
        cybozu::FilePath path("/dev");
        path += name;
        if (!path.stat().exists()) {
            throw cybozu::Exception(FUNC) << "not exists" << path.str();
        }
        return path.str();
    }
    throw cybozu::Exception(FUNC) << "not found" << major << minor;
}

inline cybozu::FilePath getSysfsPath(const std::string& wdevName)
{
    return cybozu::FilePath(cybozu::util::formatString("/sys/block/walb!%s", wdevName.c_str()));
}

inline std::string readOneLine(const std::string& path)
{
    cybozu::util::File file(path, O_RDONLY);
    cybozu::ifdstream is(file.fd());
    std::string line;
    is >> line;
    return line;
}

inline std::string getUnderlyingDevPath(const std::string& wdevName, bool isLog)
{
    cybozu::FilePath path =
        local::getSysfsPath(wdevName) + "walb" + (isLog ? "ldev" : "ddev");
    uint32_t major, minor;
    std::tie(major, minor) = local::parseDeviceIdStr(local::readOneLine(path.str()));
    return local::getDevPathFromId(major, minor);
}

} // namespace local

inline void resetWal(const std::string& wdevPath)
{
    const int dummy = 0;
    setValueByIoctl<int>(wdevPath, WALB_IOCTL_CLEAR_LOG, dummy);
}

inline void takeCheckpoint(const std::string& wdevPath)
{
    const int dummy = 0;
    setValueByIoctl<int>(wdevPath, WALB_IOCTL_TAKE_CHECKPOINT, dummy);
}

inline uint64_t getPermanentLsid(const std::string& wdevPath)
{
    return getLsid(wdevPath, WALB_IOCTL_GET_PERMANENT_LSID);
}

inline uint64_t getOldestLsid(const std::string& wdevPath)
{
    return getLsid(wdevPath, WALB_IOCTL_GET_OLDEST_LSID);
}

inline bool isOverflow(const std::string& wdevPath)
{
    return getValueByIoctl<int>(wdevPath, WALB_IOCTL_IS_LOG_OVERFLOW) != 0;
}

inline uint64_t getLogCapacityPb(const std::string& wdevPath)
{
    return getValueByIoctl<uint64_t>(wdevPath, WALB_IOCTL_GET_LOG_CAPACITY);
}

inline uint64_t getLogUsagePb(const std::string& wdevPath)
{
    return getValueByIoctl<uint64_t>(wdevPath, WALB_IOCTL_GET_LOG_USAGE);
}

inline bool isFlushCapable(const std::string& wdevPath)
{
    return getValueByIoctl<int>(wdevPath, WALB_IOCTL_IS_FLUSH_CAPABLE) != 0;
}

/**
 * @lsid this must satisfy oldestLsid < lsid <= permanentLsid,
 *   or INVALID_LSID to erase all existing wlogs.
 *
 * RETURN:
 *   remaining amount of wlogs after deletion [physical block]
 */
inline uint64_t eraseWal(const std::string& wdevPath, uint64_t lsid = INVALID_LSID)
{
    const char *const FUNC = __func__;
    if (isOverflow(wdevPath)) {
        throw cybozu::Exception(FUNC) << "overflow" << wdevPath;
    }
    const uint64_t permanentLsid = getPermanentLsid(wdevPath);
    const uint64_t oldestLsid = getOldestLsid(wdevPath);
    if (lsid == INVALID_LSID) lsid = permanentLsid;
    if (oldestLsid == lsid) {
        /* There is no wlogs. */
        return 0;
    }
    if (!(oldestLsid < lsid && lsid <= permanentLsid)) {
        throw cybozu::Exception(FUNC)
            << "invalid lsid" << oldestLsid << lsid << permanentLsid;
    }
    setOldestLsid(wdevPath, lsid);
    return permanentLsid - lsid;
}

/**
 * @sizeLb
 *   0 can be specified (auto-detect).
 */
inline void resize(const std::string& wdevPath, uint64_t sizeLb = 0)
{
    setValueByIoctl<uint64_t>(wdevPath, WALB_IOCTL_RESIZE, sizeLb);
}

inline uint64_t getSizeLb(const std::string& bdevPath)
{
    cybozu::util::File file(bdevPath, O_RDONLY);
    const uint64_t sizeB = cybozu::util::getBlockDeviceSize(file.fd());
    file.close();
    return sizeB / LOGICAL_BLOCK_SIZE;
}

inline void flushBufferCache(const std::string& bdevPath)
{
    cybozu::util::File file(bdevPath, O_RDONLY);
    cybozu::util::flushBufferCache(file.fd());
    file.close();
}

/**
 * Get polling path.
 *
 * @wdevName walb device name.
 * RETURN:
 *   full path of polling target.
 */
inline std::string getPollingPath(const std::string &wdevName)
{
    return (local::getSysfsPath(wdevName) + "walb" + "lsids").str();
}

inline std::string getUnderlyingLogDevPath(const std::string& wdevName)
{
    return local::getUnderlyingDevPath(wdevName, true);
}

inline std::string getUnderlyingDataDevPath(const std::string& wdevName)
{
    return local::getUnderlyingDevPath(wdevName, false);
}

inline std::string getWdevPathFromWdevName(const std::string& wdevName)
{
    return WDEV_PATH_PREFIX + wdevName;
}

inline std::string getWldevPathFromWdevName(const std::string& wdevName)
{
    return WDEV_PATH_PREFIX + "L" + wdevName;
}

inline std::string getWdevNameFromWdevPath(const std::string& wdevPath)
{
    const char *const FUNC = __func__;
    if (wdevPath.find(WDEV_PATH_PREFIX) != 0) {
        throw cybozu::Exception(FUNC) << "bad name" << wdevPath;
    }
    return wdevPath.substr(WDEV_PATH_PREFIX.size());
}

inline cybozu::util::File getWldevFile(const std::string& wdevName, bool isRead = true)
{
    return cybozu::util::File(
        getWldevPathFromWdevName(wdevName),
        (isRead ? O_RDONLY : O_RDWR) | O_DIRECT);
}

}} // namespace walb::device
