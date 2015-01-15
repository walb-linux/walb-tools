#pragma once
/**
 * @file
 * @brief walb utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include <atomic>
#include <chrono>
#include <thread>
#include <fstream>
#include "util.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "tmp_file_serializer.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"
#include "constant.hpp"
#include "task_queue.hpp"
#include "action_counter.hpp"
#include "thread_util.hpp"
#include "time.hpp"
#include "walb/block_size.h"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"
#include "cybozu/atoi.hpp"
#include "cybozu/itoa.hpp"
#include "cybozu/log.hpp"
#include "cybozu/file.hpp"
#include "cybozu/serializer.hpp"
#include "cybozu/array.hpp"
#include "walb_logger.hpp"
#include "walb_types.hpp"

namespace walb {

class ProcessStatus
{
    std::atomic<int> status_;
    enum {
        RUNNING, GRACEFUL_SHUTDOWN, FORCE_SHUTDOWN
    };
public:
    ProcessStatus() : status_(RUNNING) {}
    bool isRunning() const noexcept { return status_ == RUNNING; }
    bool isGracefulShutdown() const noexcept { return status_ == GRACEFUL_SHUTDOWN; }
    bool isForceShutdown() const noexcept { return status_ == FORCE_SHUTDOWN; }
    void setGracefulShutdown() noexcept { status_ = GRACEFUL_SHUTDOWN; }
    void setForceShutdown() noexcept { status_ = FORCE_SHUTDOWN; }
};

namespace util {

inline void saveMap(const std::string& file)
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

/**
 * Make a directory.
 *
 * If not exists, make a specified directory.
   If exists,
 *   ensureNotExistance == false
 *     check the directory existance.
 *   ensureNotExistance == true
 *     throw an error.
 */
void makeDir(const std::string &dirStr, const char *msg,
             bool ensureNotExistance = false)
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

inline StrVec getDirEntNameList(const std::string &dirStr, bool isDir, const char *ext = "")
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

} // walb_util_local

inline StrVec getDirNameList(const std::string &dirStr)
{
    return walb_util_local::getDirEntNameList(dirStr, true);
}

inline StrVec getFileNameList(const std::string &dirStr, const char *ext)
{
    return walb_util_local::getDirEntNameList(dirStr, false, ext);
}

template <typename T>
void saveFile(const cybozu::FilePath &dir, const std::string &fname, const T &t)
{
    cybozu::TmpFile tmp(dir.str());
    cybozu::save(tmp, t);
    tmp.save((dir + fname).str());
}

template <typename T>
void loadFile(const cybozu::FilePath &dir, const std::string &fname, T &t)
{
    cybozu::util::File r((dir + fname).str(), O_RDONLY);
    cybozu::load(t, r);
}

inline void setLogSetting(const std::string &pathStr, bool isDebug)
{
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

inline void sleepMs(size_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/**
 * Convert binary data to hex string.
 */
inline std::string binaryToStr(const void *data, size_t size)
{
    std::string s;
    const uint8_t *p = static_cast<const uint8_t *>(data);
    s.resize(size * 2);
    for (size_t i = 0; i < size; i++) {
        cybozu::itohex(&s[i * 2], 2, p[i], false);
    }
    return s;
}

/**
 * Convert a hex string to binary data.
 */
template <typename CharT>
inline void strToBinary(const std::string &s, CharT *p, size_t size)
{
    if (size * 2 != s.size()) {
        throw cybozu::Exception(__func__) << "bad size" << s << size * 2;
    }
    for (size_t i = 0; i < size; i++) {
        p[i] = cybozu::hextoi(&s[i * 2], 2);
    }
}

inline std::string timeToPrintable(uint64_t ts)
{
    if (ts == 0) {
        return "---";
    } else {
        return cybozu::unixTimeToPrettyStr(ts);
    }
}

/**
 * @sock socket to connect.
 * @sockAddr socket address.
 * @timeout connection/read/write timeout [sec].
 */
inline void connectWithTimeout(cybozu::Socket &sock, const cybozu::SocketAddr &sockAddr, size_t timeout)
{
    const size_t timeoutMs = timeout * 1000;
    sock.connect(sockAddr, timeoutMs);
    sock.setSendTimeout(timeoutMs);
    sock.setReceiveTimeout(timeoutMs);
}

/**
 * Parse integer string with suffix character k/m/g/t/p which means kibi/mebi/gibi/tebi/pebi.
 * and convert from [byte] to [logical block size].
 */
inline uint64_t parseSizeLb(const std::string &str, const char *msg, uint64_t minB = 0, uint64_t maxB = -1)
{
    const uint64_t sizeLb = cybozu::util::fromUnitIntString(str) / LOGICAL_BLOCK_SIZE;
    const uint64_t minLb = minB / LOGICAL_BLOCK_SIZE;
    const uint64_t maxLb = maxB / LOGICAL_BLOCK_SIZE;
    if (sizeLb < minLb) throw cybozu::Exception(msg) << "too small size" << minB << sizeLb;
    if (maxLb < sizeLb) throw cybozu::Exception(msg) << "too large size" << maxB << sizeLb;
    return sizeLb;
}

inline uint64_t parseBulkLb(const std::string &str, const char *msg)
{
    return parseSizeLb(str, msg, LOGICAL_BLOCK_SIZE, MAX_BULK_SIZE);
}

class TemporalExistingFile
{
    const cybozu::FilePath  path_;
    static constexpr const char *NAME() { return "TemporalExistingFile"; }
public:
    explicit TemporalExistingFile(const cybozu::FilePath &path)
        : path_(path) {
        if (path.stat().exists()) {
            throw cybozu::Exception(NAME()) << "file exists" << path.str();
        }
        ::FILE *fp = ::fopen(path.str().c_str(), "w");
        if (fp) {
            ::fclose(fp);
        } else {
            throw cybozu::Exception(NAME()) << "fopen failed" << path.str();
        }
    }
    ~TemporalExistingFile() noexcept {
        if (!path_.unlink()) {
            LOGs.error() << NAME() << "unlink error" << path_.str();
        }
    }
};

template <typename IntType>
inline void verifyNotZero(const IntType &t, const char *msg)
{
    if (t == 0) {
        throw cybozu::Exception(msg) << "must not be 0.";
    }
}

inline std::string getElapsedTimeStr(double elapsedSec)
{
    return cybozu::util::formatString("elapsed_time %.3f sec", elapsedSec);
}

}} // walb::util

inline int errorSafeMain(int (*doMain)(int, char *[]), int argc, char *argv[], const char *msg)
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

#define DEFINE_ERROR_SAFE_MAIN(msg)                    \
    int main(int argc, char *argv[]) {                 \
        return errorSafeMain(doMain, argc, argv, msg); \
    }
