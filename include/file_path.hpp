/**
 * @file
 * @brief File path utility
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cassert>
#include <cstring>
#include <memory>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef CYBOZU_FILE_PATH_HPP
#define CYBOZU_FILE_PATH_HPP

namespace cybozu {

/**
 * A wrapper of stat()/lstat().
 * TODO: implement other functionalities.
 */
class FileStat
{
private:
    struct stat st_;
    const bool isLstat_;
    bool isFailed_;
public:
    explicit FileStat(const std::string &path, bool isLstat = false)
        : st_(), isLstat_(isLstat), isFailed_(false) {
        ::memset(&st_, 0, sizeof(st_));
        if (isLstat) {
            isFailed_ = (::lstat(path.c_str(), &st_) != 0);
        } else {
            isFailed_ = (::stat(path.c_str(), &st_) != 0);
        }
    }
    bool exists() const {
        return !isFailed_;
    }
    bool isFile() const {
        if (isFailed_) return false;
        return S_ISREG(st_.st_mode);
    }
    bool isDirectory() const {
        if (isFailed_) return false;
        return S_ISDIR(st_.st_mode);
    }
    bool isCharacter() const {
        if (isFailed_) return false;
        return S_ISCHR(st_.st_mode);
    }
    bool isBlock() const {
        if (isFailed_) return false;
        return S_ISBLK(st_.st_mode);
    }
    bool isFifo() const {
        if (isFailed_) return false;
        return S_ISFIFO(st_.st_mode);
    }
    bool isSock() const {
        if (isFailed_) return false;
        return S_ISSOCK(st_.st_mode);
    }
    bool isSimlink() const {
        if (isFailed_) return false;
        return S_ISLNK(st_.st_mode);
    }
    uint64_t size() const {
        return uint64_t(st_.st_size);
    }
};

/**
 * A file path management.
 */
class FilePath
{
private:
    std::string path_;
    std::shared_ptr<FileStat> statP_;

public:
    const int SEPARATOR = '/';

    explicit FilePath(const std::string &path) : path_(path), statP_() {}
    explicit FilePath(std::string &&path) : path_(std::move(path)), statP_() {}
    FilePath operator+(const FilePath &rhs) const {
        if (path_.empty()) {
            return rhs;
        }
        if (path_.back() == SEPARATOR) {
            return FilePath(path_ + rhs.path_);
        }
        std::string path(path_);
        path += SEPARATOR;
        path += rhs.path_;
        return FilePath(std::move(path));
    }
    bool isFull() const {
        if (path_.empty()) return false;
        return path_[0] == SEPARATOR;
    }
    std::string str() const {
        return path_;
    }
    FileStat &stat(bool isForce = false) {
        if (!statP_ || isForce) {
            statP_ = std::make_shared<FileStat>(path_);
        }
        return *statP_;
    }
    bool remove() {
        return stat().isDirectory() ? rmdir() : unlink();
    }
    bool unlink() {
        return ::unlink(path_.c_str()) == 0;
    }
    bool mkdir(mode_t mode = 0755) {
        return ::mkdir(path_.c_str(), mode) == 0;
    }
    bool rmdir() {
        return ::rmdir(path_.c_str()) == 0;
    }
    bool rename(const FilePath &newPath) {
        return ::rename(path_.c_str(), newPath.str().c_str()) == 0;
    }
};

}; //namespace cybozu

#endif /* CYBOZU_FILE_PATH_HPP */
