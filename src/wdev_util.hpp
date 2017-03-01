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
#include "linux/walb/ioctl.h"
#include "linux/walb/block_size.h"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"
#include "walb_types.hpp"
#include "bdev_util.hpp"

namespace walb {
namespace device {

static const std::string WDEV_PATH_PREFIX = "/dev/walb/";

void invokeWdevIoctl(
    const std::string& wdevPath, struct walb_ctl *ctl, const char *msg = "");

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
uint64_t getLsid(const std::string& wdevPath, int command);

inline void setOldestLsid(const std::string& wdevPath, uint64_t lsid)
{
    setValueByIoctl<uint64_t>(wdevPath, WALB_IOCTL_SET_OLDEST_LSID, lsid);
}

namespace local {

/**
 * Parse "XXX:YYY" string where XXX is major id and YYY is minor id.
 */
std::pair<uint32_t, uint32_t> parseDeviceIdStr(const std::string& devIdStr);

/**
 * Replace charactor x to y in a string.
 */
void replaceChar(std::string &s, const char x, const char y);

/**
 * Get block device path from major and minor id using lsblk command.
 */
std::string getDevPathFromId(uint32_t major, uint32_t minor);

inline cybozu::FilePath getSysfsPath(const std::string& wdevName)
{
    return cybozu::FilePath(cybozu::util::formatString("/sys/block/walb!%s", wdevName.c_str()));
}

std::string readOneLine(const std::string& path);
std::string getUnderlyingDevPath(const std::string& wdevName, bool isLog);

} // namespace local

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

std::string getWdevNameFromWdevPath(const std::string& wdevPath);
cybozu::util::File getWldevFile(const std::string& wdevName, bool isRead = true);

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

struct LsidSet
{
    static constexpr const uint64_t invalid = uint64_t(-1);
    union {
        uint64_t array[8];
        struct {
            uint64_t latest;
            uint64_t submitted;
            uint64_t flush;
            uint64_t completed;
            uint64_t permanent;
            uint64_t written;
            uint64_t prevWritten;
            uint64_t oldest;
        };
    };
    LsidSet() {
        init();
    }
    void init() {
        for (uint64_t &v : array) v = invalid;
    }
    bool isValid() const {
        const size_t n = sizeof(array) / sizeof(uint64_t);
        for (size_t i = 0; i < n; i++) {
            if (i == 1) continue; // submitted may be missing.
            if (array[i] == invalid) return false;
        }
        return true;
    }
};

void getLsidSet(const std::string &wdevName, LsidSet &lsidSet);
uint64_t getLatestLsid(const std::string& wdevPath);

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

inline uint64_t getWrittenLsid(const std::string& wdevPath)
{
    return getLsid(wdevPath, WALB_IOCTL_GET_WRITTEN_LSID);
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
 * @lsid this must satisfy lsid <= prevWrittenLsid,
 *
 * RETURN:
 *   remaining amount of wlogs after deletion [physical block]
 */
void eraseWal(const std::string& wdevName, uint64_t lsid);

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

}} // namespace walb::device
