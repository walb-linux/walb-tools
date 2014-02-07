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

namespace walb {
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

}} // namespace walb::util
