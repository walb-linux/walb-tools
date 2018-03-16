#include "walb_util.hpp"
#include <sstream>

namespace walb {
namespace util {

void saveMap(const std::string& file)
{
    const int pid = getpid();
    char name[256];
    snprintf(name, sizeof(name), "/proc/%d/maps", pid);
    std::ifstream ifs(name, std::ios::binary);
    std::ofstream ofs(file.c_str(), std::ios::binary);
    std::string line;
    while (std::getline(ifs, line)) {
        ofs << line << std::endl;
    }
}

void makeDir(const std::string &dirStr, const char *msg,
             bool ensureNotExistance)
{
    cybozu::FilePath dir(dirStr);
    if (dir.stat().exists()) {
        if (ensureNotExistance) {
            throw cybozu::Exception(msg) << "already exists" << dirStr;
        } else {
            if (dir.stat().isDirectory()) {
                return;
            } else {
                throw cybozu::Exception(msg) << "not directory" << dirStr;
            }
        }
    }
    if (!dir.mkdir()) {
        throw cybozu::Exception(msg) << "mkdir failed" << dirStr;
    }
}

namespace walb_util_local {

/**
 * isDir: if true, get directories only.
 *        othwerwise, get files only.
 */
StrVec getDirEntNameList(const std::string &dirStr, bool isDir, const char *ext)
{
    StrVec ret;
    std::vector<cybozu::FileInfo> list = cybozu::GetFileList(dirStr, ext);
    for (const cybozu::FileInfo &info : list) {
        bool isDir2, isFile2;
        if (info.isUnknown()) {
            cybozu::FilePath fpath(dirStr);
            fpath += info.name;
            cybozu::FileStat stat = fpath.stat();
            if (!stat.exists()) continue;
            isDir2 = stat.isDirectory();
            isFile2 = stat.isFile();
        } else {
            isDir2 = info.isDirectory();
            isFile2 = info.isFile();
        }
        if ((isDir && isDir2) || (!isDir && isFile2)) {
            ret.push_back(info.name);
        }
    }
    return ret;
}

} // namesapce walb_util_local

void setLogSetting(const std::string &pathStr, bool isDebug)
{
    cybozu::SetLogUseMsec(true);
    if (pathStr == "-") {
        cybozu::SetLogFILE(::stderr);
    } else {
        cybozu::OpenLogFile(pathStr);
    }
    if (isDebug) {
        cybozu::SetLogPriority(cybozu::LogDebug);
    } else {
        cybozu::SetLogPriority(cybozu::LogInfo);
    }
}

std::string getNowStr()
{
    struct timespec ts;
    if (::clock_gettime(CLOCK_REALTIME, &ts) < 0) {
        throw cybozu::Exception("getNowStr: clock_gettime failed.");
    }
    return cybozu::getHighResolutionTimeStr(ts);
}

std::string binaryToStr(const void *data, size_t size)
{
    std::string s;
    const uint8_t *p = static_cast<const uint8_t *>(data);
    s.resize(size * 2);
    for (size_t i = 0; i < size; i++) {
        cybozu::itohex(&s[i * 2], 2, p[i], false);
    }
    return s;
}

void setSocketParams(cybozu::Socket& sock, const KeepAliveParams& params, size_t timeoutS)
{
    if (params.enabled) {
        sock.setSendTimeout(0);
        sock.setReceiveTimeout(0);
        util::enableKeepAlive(sock, params.idle, params.intvl, params.cnt);
    } else {
        sock.setSendTimeout(timeoutS * 1000);
        sock.setReceiveTimeout(timeoutS * 1000);
    }
}

uint64_t parseSizeLb(const std::string &str, const char *msg, uint64_t minB, uint64_t maxB)
{
    const uint64_t sizeLb = cybozu::util::fromUnitIntString(str) / LOGICAL_BLOCK_SIZE;
    const uint64_t minLb = minB / LOGICAL_BLOCK_SIZE;
    const uint64_t maxLb = maxB / LOGICAL_BLOCK_SIZE;
    if (sizeLb < minLb) throw cybozu::Exception(msg) << "too small size" << minB << sizeLb;
    if (maxLb < sizeLb) throw cybozu::Exception(msg) << "too large size" << maxB << sizeLb;
    return sizeLb;
}

} // namespace util
} // namespace walb

int errorSafeMain(int (*doMain)(int, char *[]), int argc, char *argv[], const char *msg)
{
    try {
        walb::util::setLogSetting("-", false);
        return doMain(argc, argv);
    } catch (std::exception &e) {
        LOGs.error() << msg << e.what();
    } catch (...) {
        LOGs.error() << msg << "unknown error";
    }
    return 1;
}
