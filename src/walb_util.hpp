#pragma once
/**
 * @file
 * @brief File utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include <atomic>
#include <chrono>
#include <thread>
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
#include "walb_logger.hpp"
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

namespace walb {

typedef std::vector<std::string> StrVec;
typedef std::vector<char> Buffer;
typedef std::unique_lock<std::recursive_mutex> UniqueLock;

namespace util {

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

inline std::vector<std::string> getDirEntNameList(const std::string &dirStr, bool isDir, const char *ext = "")
{
    std::vector<std::string> ret;
    std::vector<cybozu::FileInfo> list = cybozu::GetFileList(dirStr, ext);
    for (const cybozu::FileInfo &info : list) {
        if ((isDir && info.isDirectory()) ||
            (!isDir && info.isFile())) {
            ret.push_back(info.name);
        }
    }
    return ret;
}

} // walb_util_local

inline std::vector<std::string> getDirNameList(const std::string &dirStr)
{
    return walb_util_local::getDirEntNameList(dirStr, true);
}

inline std::vector<std::string> getFileNameList(const std::string &dirStr, const char *ext)
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
 * Hex string.
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

inline std::string timeToPrintable(uint64_t ts)
{
    if (ts == 0) {
        return "---";
    } else {
        return cybozu::unixTimeToStr(ts);
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

}} // walb::util
