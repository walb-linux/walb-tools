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
#include <list>
#include <queue>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

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
 * opendir()/closedir()/readdir() wrapper.
 */
class Directory
{
private:
    DIR *dirP_;
    mutable std::queue<std::string> q_;
public:
    explicit Directory(const std::string &path) : dirP_(nullptr), q_() {
        dirP_ = ::opendir(path.c_str());
        if (!dirP_) {
            throw std::runtime_error("opendir failed.");
        }
    }
    ~Directory() noexcept {
        if (dirP_) {
            ::closedir(dirP_);
            dirP_ = nullptr;
        }
    }
    bool isEnd() const {
        return q_.empty() && !readAhead();
    }
    std::string next() const {
        if (isEnd()) {
            throw std::runtime_error("no more data.");
        }
        assert(!q_.empty());
        std::string s = std::move(q_.front());
        q_.pop();
        return std::move(s);
    }
private:
    /**
     * RETURN:
     *   false readdir() returns nullptr.
     */
    bool readAhead() const {
        assert(dirP_);
        struct dirent *ent = ::readdir(dirP_);
        if (!ent) return false;
        q_.push(std::string(ent->d_name));
        return true;
    }
};

/**
 * A file path management.
 * Escaped separator character "\\/" is not supported.
 */
class FilePath
{
private:
    std::string path_;
    mutable std::shared_ptr<FileStat> statP_;

public:
    static const int SEPARATOR = '/';

    explicit FilePath(const std::string &path) : path_(path), statP_() {}
    explicit FilePath(std::string &&path) : path_(std::move(path)), statP_() {}
    FilePath operator+(const FilePath &rhs) const {
        if (rhs.isRoot()) {
            std::runtime_error("full path can not be added.");
        }
        if (path_.empty()) {
            return rhs;
        }
        if (isRoot()) {
            return FilePath(path_ + rhs.path_);
        }
        std::string path(path_);
        path.push_back(SEPARATOR);
        path.append(rhs.path_);
        return FilePath(std::move(path)).removeRedundancy();
    }
    bool operator==(const FilePath &rhs) const {
        std::string s0 = removeRedundancy().str();
        std::string s1 = rhs.removeRedundancy().str();
        return s0 == s1;
    }
    bool operator!=(const FilePath &rhs) const {
        return !(*this == rhs);
    }
    bool isFull() const {
        if (path_.empty()) return false;
        return path_[0] == SEPARATOR;
    }
    bool isRoot() const {
        if (!isFull()) return false;
        return removeRedundancy().str().size() == 1;
    }
    std::string baseName() const {
        std::list<std::string> li = split();
        eliminateRelativeParents(li);
        if (li.empty()) {
            return join(li, isFull());
        } else {
            return li.back();
        }
    }
    std::string dirName() const {
        std::list<std::string> li = split();
        eliminateRelativeParents(li);
        if (!li.empty()) {
            li.pop_back();
        }
        return join(li, isFull());
    }
    size_t depth() const {
        if (!isFull()) {
            throw std::runtime_error("depth can not be defined for relative path.");
        }
        std::list<std::string> li = split();
        eliminateRelativeParents(li);
        return li.size();
    }
    FilePath removeRedundancy() const {
        std::list<std::string> li;
        const bool isFull0 = isFull();
        li = split();
        eliminateRelativeParents(li);
        return FilePath(join(li, isFull0));
    }
    FilePath parent() const {
        if (isRoot()) return *this;
        return (*this + FilePath("..")).removeRedundancy();
    }
    const std::string &str() const {
        return path_;
    }
    const char *cStr() const {
        return path_.c_str();
    }
    FileStat &stat(bool isForce = false) const {
        if (!statP_ || isForce) {
            statP_ = std::make_shared<FileStat>(path_);
        }
        return *statP_;
    }
    FilePath toFullPath() const {
        if (isFull()) return *this;
        char buf[PATH_MAX + 1];
        if (::getcwd(buf, PATH_MAX + 1) == nullptr) {
            throw std::runtime_error("getcwd failed.");
        }
        return FilePath(buf) + *this;
    }
    bool remove() const {
        return stat().isDirectory() ? rmdir() : unlink();
    }
    bool unlink() const {
        bool ret = ::unlink(path_.c_str()) == 0;
        statP_.reset();
        return ret;
    }
    bool mkdir(mode_t mode = 0755) const {
        bool ret = ::mkdir(path_.c_str(), mode) == 0;
        statP_.reset();
        return ret;
    }
    bool rmdir() const {
        bool ret = ::rmdir(path_.c_str()) == 0;
        statP_.reset();
        return ret;
    }
    bool rename(const FilePath &newPath) const {
        bool ret = ::rename(path_.c_str(), newPath.cStr()) == 0;
        statP_.reset();
        return ret;
    }
    bool chmod(mode_t mode) const {
        bool ret = ::chmod(path_.c_str(), mode) == 0;
        statP_.reset();
        return ret;
    }
    bool rmdirRecursive() const {
        if (!stat().isDirectory()) return false;
        Directory dir(str());
        while (!dir.isEnd()) {
            std::string s = dir.next();
            if (s == "." || s == "..") continue;
            FilePath child = *this + FilePath(s);
            if (child.stat().isDirectory()) {
                if (!child.rmdirRecursive()) return false;
            } else {
                if (!child.unlink()) return false;
            }
        }
        return rmdir();
    }
private:
    /**
     * Split by SEPARATOR and eliminate "" and ".".
     */
    std::list<std::string> split() const {
        std::list<std::string> li;
        /* Split by SEPARATOR. */
        std::string sep;
        sep.push_back(SEPARATOR);
        std::string s = str();
        while (!s.empty()) {
            size_t n = s.find(sep);
            if (n == std::string::npos) {
                li.push_back(s);
                s = "";
            } else {
                if (0 < n) {
                    li.push_back(s.substr(0, n));
                }
                s = s.substr(n + 1);
            }
        }
        /* eliminate "" and "." */
        auto it = li.begin();
        while (it != li.end()) {
            std::string &s0 = *it;
            if (s0.empty() || s0 == ".") {
                it = li.erase(it);
            } else {
                ++it;
            }
        }
        return std::move(li);
    }
    /**
     * eliminate "xxx/.." pattern.
     */
    static void eliminateRelativeParents(std::list<std::string> &li0) {
        if (li0.empty()) return;
        std::list<std::string> li1;
        li1.push_back(std::move(li0.front()));
        li0.pop_front();
        while (!li0.empty()) {
            li1.push_back(std::move(li0.front()));
            li0.pop_front();
            assert(2 <= li1.size());
            auto it = li1.rbegin();
            std::string &s1 = *it;
            ++it;
            std::string &s0 = *it;
            if (s0 != ".." && s1 == "..") {
                li1.pop_back();
                li1.pop_back();
                if (li1.empty()) {
                    if (li0.empty()) {
                        break;
                    } else {
                        li1.push_back(std::move(li0.front()));
                        li0.pop_front();
                    }
                }
            }
        }
        li0 = std::move(li1);
    }
    /**
     * Convert separated path names to its path string.
     * RETURN:
     *   If li is not empty like ["a", "b", "c"],
     *     "a/b/c" (isFull0 is false).
     *     "/a/b/c" (isFull0 is true).
     *   If li is empty,
     *     "." (isFull0 is false).
     *     "/" (isFull0 is true).
     */
    static std::string join(std::list<std::string> &li, bool isFull0) {
        std::string s1;
        if (isFull0) s1.push_back(SEPARATOR);
        for (std::string &s0 : li) {
            s1.append(s0);
            s1.push_back(SEPARATOR);
        }
        if (!li.empty()) s1.resize(s1.size() - 1);
        if (s1.empty()) s1.push_back('.');
        return std::move(s1);
    }
};

}; //namespace cybozu

#endif /* CYBOZU_FILE_PATH_HPP */
