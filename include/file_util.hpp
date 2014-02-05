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

namespace cybozu {
namespace util {

/**
 * If not exists, make a specified directory.
 * Otherwise, check the directory existance.
 */
void checkOrMakeDir(const std::string &dirStr)
{
    FilePath dir(dirStr);
    if (!dir.stat().exists()) {
        if (!dir.mkdir()) {
            std::string m = formatString("mkdir directory %s failed.", dirStr.c_str());
            throw std::runtime_error(m);
        }
    }
    if (!dir.stat(true).isDirectory()) {
        std::string m = formatString("The path %s is not directory.", dirStr.c_str());
        throw std::runtime_error(m);
    }
}

}} // namespace cybozu::util
