#pragma once
/**
 * @file
 * @brief Temporary file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <cstdlib>
#include <string>
#include <stdexcept>
#include <unistd.h>
#include <cstdlib>
#include <sys/stat.h>
#include "file_path.hpp"
#include "fileio.hpp"

namespace cybozu {

const std::string DEFAULT_TMP_FILE_PREFIX = "tmp";
const std::string TMP_FILE_SUFFIX = "XXXXXX";

class TmpFile
{
private:
    std::string path_;
    int fd_;
public:
    explicit TmpFile(const std::string &dirPath, int flags = 0, const std::string &prefix = DEFAULT_TMP_FILE_PREFIX)
        : path_(createTmpPathStatic(dirPath, prefix)), fd_(-1) {
        fd_ = ::mkostemp(&path_[0], flags);
        if (fd_ < 0) {
            throw std::runtime_error("mkstemp failed.");
        }
    }
    ~TmpFile() noexcept {
        if (0 < fd_) ::close(fd_);
        if (!path_.empty()) ::unlink(path_.c_str());
    }
    const std::string &path() const {
        return path_;
    }
    int fd() const { return fd_; }
    void save(const std::string &path, mode_t mode = 0644) {
        assert(0 <= fd_);
        if (::fsync(fd_) < 0) {
            throw std::runtime_error("fsync failed.");
        }
        if (::close(fd_) < 0) {
            throw std::runtime_error("close failed.");
        }
        fd_ = -1;
        FilePath oldPath(path_);
        FilePath newPath(path);
        if (!oldPath.rename(newPath)) {
            throw std::runtime_error("rename failed.");
        }
        path_.resize(0);
        if (!newPath.chmod(mode)) {
            throw std::runtime_error("chmod failed.");
        }
        /* directory sync. */
        util::File file(newPath.parent().str(), O_RDONLY | O_DIRECTORY);
        file.fdatasync();
    }
private:
    static std::string createTmpPathStatic(const std::string &dirPath, const std::string &prefix) {
        assert(!prefix.empty());
        if (!FilePath(dirPath).stat().isDirectory()) {
            throw std::runtime_error("dirPath not found.");
        }
        return (FilePath(dirPath) + FilePath(prefix)).str() + "XXXXXX";
    }
};

inline size_t removeAllTmpFiles(const std::string &dirPath, const std::string prefix = DEFAULT_TMP_FILE_PREFIX)
{
    FilePath dirFp(dirPath);
    if (!dirFp.stat().isDirectory()) {
        throw RT_ERR("removeAllTmpFiles:directory not found: %s.", dirFp.str().c_str());
    }

    size_t nr = 0;
    Directory dir(dirPath);
    while (!dir.isEnd()) {
        const std::string name = dir.next();
        if (util::hasPrefix(name, prefix) && name.size() == prefix.size() + TMP_FILE_SUFFIX.size()) {
            FilePath fileFp = dirFp + name;
            if (!fileFp.unlink()) {
                throw RT_ERR("removeAllTmpFiles:unlink failed: %s.", fileFp.str().c_str());
            }
            nr++;
        }
    }
    return nr;
}

} //namespace cybozu
