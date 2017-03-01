#include "wdev_util.hpp"
#include "cybozu/atoi.hpp"
#include "walb_logger.hpp"

namespace walb {
namespace device {

void invokeWdevIoctl(const std::string& wdevPath, struct walb_ctl *ctl,
                     const char *msg)
{
    if (!msg || !*msg) msg = __func__;
    cybozu::util::File file(wdevPath, O_RDWR);
    if (::ioctl(file.fd(), WALB_IOCTL_WDEV, ctl) < 0) {
        throw cybozu::Exception(msg) << "ioctl error" << cybozu::ErrorNo();
    }
    file.close();
}

uint64_t getLsid(const std::string& wdevPath, int command)
{
    uint64_t lsid = getValueByIoctl<uint64_t>(wdevPath, command);
    if (lsid == uint64_t(-1)) {
        throw cybozu::Exception("getLsid:invalid lsid");
    }
    return lsid;
}

namespace local {

std::pair<uint32_t, uint32_t> parseDeviceIdStr(const std::string& devIdStr)
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

void replaceChar(std::string &s, const char x, const char y)
{
    for (;;) {
        size_t n = s.find(x);
        if (n == std::string::npos) break;
        s[n] = y;
    }
}

std::string getDevPathFromId(uint32_t major, uint32_t minor)
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

std::string readOneLine(const std::string& path)
{
    cybozu::util::File file(path, O_RDONLY);
    cybozu::ifdstream is(file.fd());
    std::string line;
    is >> line;
    return line;
}

std::string getUnderlyingDevPath(const std::string& wdevName, bool isLog)
{
    cybozu::FilePath path =
        local::getSysfsPath(wdevName) + "walb" + (isLog ? "ldev" : "ddev");
    uint32_t major, minor;
    std::tie(major, minor) = local::parseDeviceIdStr(local::readOneLine(path.str()));
    return local::getDevPathFromId(major, minor);
}

} // namespace local

std::string getWdevNameFromWdevPath(const std::string& wdevPath)
{
    const char *const FUNC = __func__;
    if (wdevPath.compare(0, WDEV_PATH_PREFIX.size(), WDEV_PATH_PREFIX) != 0) {
        throw cybozu::Exception(FUNC) << "bad name" << wdevPath;
    }
    return wdevPath.substr(WDEV_PATH_PREFIX.size());
}

cybozu::util::File getWldevFile(const std::string& wdevName, bool isRead)
{
    return cybozu::util::File(
        getWldevPathFromWdevName(wdevName),
        (isRead ? O_RDONLY : O_RDWR) | O_DIRECT);
}

void getLsidSet(const std::string &wdevName, LsidSet &lsidSet)
{
    const char *const FUNC = __func__;

    struct Pair {
        const char *name;
        std::function<void(uint64_t)> set;
    } tbl[] = {
        {"latest", [&](uint64_t lsid) { lsidSet.latest = lsid; } },
        {"submitted", [&](uint64_t lsid) { lsidSet.submitted = lsid; } },
        {"flush", [&](uint64_t lsid) { lsidSet.flush = lsid; } },
        {"completed", [&](uint64_t lsid) { lsidSet.completed = lsid; } },
        {"permanent", [&](uint64_t lsid) { lsidSet.permanent = lsid; } },
        {"written", [&](uint64_t lsid) { lsidSet.written = lsid; } },
        {"prev_written", [&](uint64_t lsid) { lsidSet.prevWritten = lsid; } },
        {"oldest", [&](uint64_t lsid) { lsidSet.oldest = lsid; } },
    };

    lsidSet.init();
    const std::string lsidPath = getPollingPath(wdevName);
    std::string readStr;
    cybozu::util::readAllFromFile(lsidPath, readStr);
    for (const std::string &line : cybozu::util::splitString(readStr, "\r\n")) {
        if (line.empty()) continue;
        StrVec v = cybozu::util::splitString(line, " \t");
        cybozu::util::removeEmptyItemFromVec(v);
        if (v.size() != 2) {
            throw cybozu::Exception(FUNC) << "bad data" << v.size() << line;
        }
        bool found = false;
        for (Pair &pair : tbl) {
            if (v[0] == pair.name) {
                pair.set(cybozu::atoi(v[1]));
                found = true;
                break;
            }
        }
#if 0
        if (!found) throw cybozu::Exception(FUNC) << "bad data" << line;
#else
        if (!found) LOGs.warn() << FUNC << "could not parse line" << line;
#endif
    }
    if (!lsidSet.isValid()) {
        throw cybozu::Exception(FUNC) << "invalid data" << readStr;
    }
}

uint64_t getLatestLsid(const std::string& wdevPath)
{
    const std::string wdevName = getWdevNameFromWdevPath(wdevPath);
    LsidSet lsidSet;
    getLsidSet(wdevName, lsidSet);
    return lsidSet.latest;
}

void eraseWal(const std::string& wdevName, uint64_t lsid)
{
    const char *const FUNC = __func__;
    const std::string wdevPath = getWdevPathFromWdevName(wdevName);
    if (isOverflow(wdevPath)) {
        throw cybozu::Exception(FUNC) << "overflow" << wdevPath;
    }
    LsidSet lsidSet;
    getLsidSet(wdevName, lsidSet);
    if (lsid <= lsidSet.oldest) {
        /* There is no wlogs. */
        return;
    }
    if (lsid > lsidSet.prevWritten) {
        throw cybozu::Exception(FUNC)
            << "invalid lsid" << lsid << lsidSet.prevWritten;
    }
    setOldestLsid(wdevPath, lsid);
}

} // namespace device
} // namespace walb
