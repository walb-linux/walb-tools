#pragma once
/**
 * @file
 * @brief File utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2014 Cybozu Labs, Inc.
 */
#include "util.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "uuid.hpp"
#include "cybozu/exception.hpp"
#include "cybozu/string_operation.hpp"
#include "cybozu/socket.hpp"

namespace walb {

typedef std::vector<std::string> StrVec;

namespace util {

/**
 * Make a directory.
 *
 * ensureNotExistance == true
 *   If not exists, make a specified directory.
 *   Otherwise, check the directory existance.
 * ensureNotExistance == false
 *   If not exists, make a specified directory.
 *   Otherwise, throw an error.
 */
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
    StrVec v;
    if (cybozu::Split(v, addrPort, ':', 2) != 2) {
        throw cybozu::Exception("parse error") << addrPort;
    }
    return cybozu::SocketAddr(v[0], static_cast<uint16_t>(cybozu::atoi(v[1])));
}

inline std::vector<cybozu::SocketAddr> parseMultiSocketAddr(const std::string &multiAddrPort)
{
    std::vector<cybozu::SocketAddr> ret;
    StrVec v;
    cybozu::Split(v, multiAddrPort, ',');
    for (const std::string &addrPort : v) {
        ret.emplace_back(parseSocketAddr(addrPort));
    }
    return ret;
}

}} // namespace walb::util
