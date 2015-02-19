#pragma once
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
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <cstdlib>
#include <system_error>

namespace cybozu {

/**
 * A wrapper of stat()/lstat()/fstat().
 */
class FileStat
{
private:
    struct stat st_;
    bool isLstat_;
    bool isFailed_;
public:
    FileStat() : st_(), isLstat_(), isFailed_(true) {}
    explicit FileStat(const std::string &path, bool isLstat = false)
        : st_(), isLstat_(isLstat), isFailed_(false) {
        ::memset(&st_, 0, sizeof(st_));
        if (isLstat) {
            isFailed_ = (::lstat(path.c_str(), &st_) != 0);
        } else {
            isFailed_ = (::stat(path.c_str(), &st_) != 0);
        }
    }
    explicit FileStat(int fd)
        : st_(), isLstat_(false), isFailed_(false) {
        ::memset(&st_, 0, sizeof(st_));
        isFailed_ = (::fstat(fd, &st_) != 0);
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
    bool isExecutable() const {
        if (isFailed_) return false;
        return (st_.st_mode & S_IXUSR) != 0;
    }
    uint64_t size() const {
        if (isFailed_) return 0;
        return uint64_t(st_.st_size);
    }
    dev_t deviceId() const {
        if (isFailed_) throw std::runtime_error("stat failed.");
        return st_.st_rdev;
    }
    int majorId() const { return ::major(deviceId()); }
    int minorId() const { return ::minor(deviceId()); }
    ino_t getInode() const { return st_.st_ino; }
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
        return s;
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
    bool normalized_;

public:
    static const char SEPARATOR = '/';
    FilePath() : path_("."), normalized_(true) {} /* current directly */
    explicit FilePath(const std::string &path)
        : path_(path), normalized_(false) {}
    explicit FilePath(std::string &&path)
        : path_(std::move(path)), normalized_(false) {}
    /* Copy constructor */
    FilePath(const FilePath &rhs)
        : path_(rhs.path_), normalized_(rhs.normalized_) {}
    /* Move constructor */
    FilePath(FilePath &&rhs)
        : FilePath() {
        swap(rhs);
    }
    /* Copy */
    FilePath &operator=(const FilePath &rhs) {
        FilePath tmp(rhs);
        swap(tmp);
        return *this;
    }
    /* Copy */
    FilePath &operator=(const std::string &rhs) {
        return *this = cybozu::FilePath(rhs);
    }
    /* Move */
    FilePath &operator=(FilePath &&rhs) {
        swap(rhs);
        return *this;
    }
    FilePath operator+(const FilePath &rhs) const {
        if (rhs.isRoot()) std::runtime_error("full path can not be added.");
        if (path_.empty()) return rhs;
        return FilePath(path_ + SEPARATOR + rhs.path_).removeRedundancy();
    }
    FilePath operator+(const std::string &rhs) const {
        return *this + cybozu::FilePath(rhs);
    }
    FilePath& operator+=(const std::string &rhs) {
        *this = *this + rhs;
        return *this;
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
        if (normalized_) return *this;
        std::list<std::string> li;
        const bool isFull0 = isFull();
        li = split();
        eliminateRelativeParents(li);
        std::string path = join(li, isFull0);
        FilePath fp(std::move(path));
        fp.normalized_ = true;
        return fp;
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
    FileStat stat() const {
        return FileStat(path_, false);
    }
    FileStat lstat() const {
        return FileStat(path_, true);
    }
    FilePath toFullPath() const {
        if (isFull()) return *this;
        char buf[PATH_MAX + 1];
        if (::getcwd(buf, PATH_MAX + 1) == nullptr) {
            throw std::runtime_error("getcwd failed.");
        }
        return FilePath(buf) + *this;
    }
    bool remove(int *err = nullptr) const {
        return stat().isDirectory() ? rmdir(err) : unlink(err);
    }
    bool unlink(int *err = nullptr) const {
        bool ret = ::unlink(path_.c_str()) == 0;
        setError(ret, err);
        return ret;
    }
    bool mkdir(mode_t mode = 0755, int *err = nullptr) const {
        bool ret = ::mkdir(path_.c_str(), mode) == 0;
        setError(ret, err);
        return ret;
    }
    bool rmdir(int *err = nullptr) const {
        bool ret = ::rmdir(path_.c_str()) == 0;
        setError(ret, err);
        return ret;
    }
    bool rename(const FilePath &newPath, int *err = nullptr) const {
        bool ret = ::rename(path_.c_str(), newPath.cStr()) == 0;
        setError(ret, err);
        return ret;
    }
    bool link(const FilePath &newPath, int *err = nullptr) const {
        bool ret = ::link(path_.c_str(), newPath.cStr()) == 0;
        setError(ret, err);
        return ret;
    }
    bool symlink(const FilePath &newPath, int *err = nullptr) const {
        bool ret = ::symlink(path_.c_str(), newPath.cStr()) == 0;
        setError(ret, err);
        return ret;
    }
    bool chmod(mode_t mode, int *err = nullptr) const {
        bool ret = ::chmod(path_.c_str(), mode) == 0;
        setError(ret, err);
        return ret;
    }
    void printRecursive() const {
        if (!stat().isDirectory()) return;
        ::printf("%s\n", cStr());
        Directory dir(str());
        while (!dir.isEnd()) {
            std::string s = dir.next();
            if (s == "." || s == "..") continue;
            FilePath child = *this + FilePath(s);
            if (child.stat().isDirectory()) {
                child.printRecursive();
            } else {
                ::printf("%s\n", child.cStr());
            }
        }
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
    friend inline std::ostream &operator<<(std::ostream &os, const FilePath &fp) {
        os << fp.removeRedundancy().str();
        return os;
    }
private:
    void swap(FilePath &rhs) noexcept {
        std::swap(path_, rhs.path_);
        std::swap(normalized_, rhs.normalized_);
    }
    static void setError(bool ret, int *err) {
        if (!ret && err != nullptr) {
            *err = errno;
        }
    }
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
        return li;
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
        return s1;
    }
};

inline FilePath getCurrentDir()
{
    char *p = ::getcwd(nullptr, 0);
    if (!p) throw std::system_error(errno, std::system_category(), "getcwd failed.");
    try {
        FilePath fp(p);
        ::free(p);
        return fp;
    } catch (...) {
        ::free(p);
        throw;
    }
}

inline bool isSameFile(const std::string& path1, const std::string& path2)
{
    const FileStat s1(path1);
    const FileStat s2(path2);
    return s1.getInode() == s2.getInode();
}

}; //namespace cybozu
