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
#include "uuid.hpp"
#include "constant.hpp"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"
#include "cybozu/socket.hpp"
#include "cybozu/option.hpp"

namespace walb {

typedef std::vector<std::string> StrVec;
typedef std::unique_lock<std::recursive_mutex> UniqueLock;

enum StopState {
    NotStopping,
    Stopping,
    ForceStopping
};

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
    cybozu::util::FileReader r((dir + fname).str(), O_RDONLY);
    cybozu::load(t, r);
}

inline cybozu::SocketAddr parseSocketAddr(const std::string &addrPort)
{
    const StrVec v = cybozu::Split(addrPort, ':', 2);
    if (v.size() != 2) {
        throw cybozu::Exception("parse error") << addrPort;
    }
    return cybozu::SocketAddr(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}

inline std::vector<cybozu::SocketAddr> parseMultiSocketAddr(const std::string &multiAddrPort)
{
    std::vector<cybozu::SocketAddr> ret;
    const StrVec v = cybozu::Split(multiAddrPort, ',');
    for (const std::string &addrPort : v) {
        ret.emplace_back(parseSocketAddr(addrPort));
    }
    return ret;
}

void setLogSetting(const std::string &pathStr, bool isDebug)
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

class Stopper
{
private:
    std::atomic<int> &stopState;
    bool success;
public:
    Stopper(std::atomic<int> &stopState, bool isForce)
        : stopState(stopState), success(false) {
        int before = NotStopping;
        const int after = isForce ? ForceStopping : Stopping;
        success = stopState.compare_exchange_strong(before, after);
    }
    ~Stopper() noexcept {
        if (success) {
            assert(stopState != NotStopping);
            stopState = NotStopping;
        }
    }
    bool isSuccess() const {
        return success;
    }
};

inline void sleepMs(size_t ms)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

/**
 * return when pred() is true.
 */
template <typename Mutex, typename Pred>
void waitUntil(Mutex &mu, Pred pred, const char *msg, size_t timeout = DEFAULT_TIMEOUT)
{
    for (size_t c = 0; c < timeout; c++) {
        if (pred()) return;
        mu.unlock();
        sleepMs(1000);
        mu.lock();
    }
    throw cybozu::Exception(msg) << "timeout" << timeout;
}

}} // walb::util
