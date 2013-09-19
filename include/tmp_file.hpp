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

class TmpFile
{
private:
    std::string path_;
    int fd_;
public:
    explicit TmpFile(const std::string &dirPath, const std::string &prefix = "tmp")
        : path_(createTmpPathStatic(dirPath, prefix)), fd_(-1) {
        fd_ = ::mkstemp(&path_[0]);
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
        util::FileOpener fo(newPath.parent().str(), O_RDONLY | O_DIRECTORY);
        if (::fdatasync(fo.fd()) < 0) {
            throw std::runtime_error("fdatasync failed.");
        }
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

} //namespace cybozu
