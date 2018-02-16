#pragma once
/**
 * @file
 * @brief File IO utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <mutex>
#include <stdexcept>
#include <atomic>

#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "util.hpp"

namespace cybozu {
namespace util {

/**
 * Eof error for IO.
 */
class EofError : public std::exception {
public:
    virtual const char *what() const noexcept {
        return "eof error";
    }
};

/**
 * A simple file/fd operators.
 * close() will be called in the destructor when you forget to call it.
 */
class File
{
private:
    int fd_;
    bool autoClose_;

    void throwOpenError(const std::string& filePath) const {
        std::string s("open failed: ");
        s += filePath;
        throwLibcError(s);
    }
public:
    File()
        : fd_(-1), autoClose_(false) {
    }
    File(const std::string& filePath, int flags)
        : File() {
        if (!open(filePath, flags)) throwOpenError(filePath);
    }
    File(const std::string& filePath, int flags, mode_t mode)
        : File() {
        if (!open(filePath, flags, mode)) throwOpenError(filePath);
    }
    explicit File(int fd, bool autoClose = false)
        : fd_(fd), autoClose_(autoClose) {
    }
    File(File&& rhs)
        : File() {
        swap(rhs);
    }
    File& operator=(File&& rhs) {
        close();
        swap(rhs);
        return *this;
    }
    void swap(File& rhs) noexcept {
        std::swap(fd_, rhs.fd_);
        std::swap(autoClose_, rhs.autoClose_);
    }
    ~File() noexcept try {
        close();
    } catch (...) {
    }
    int fd() const {
        if (fd_ < 0) throw RT_ERR("fd < 0.");
        return fd_;
    }
    bool open(const std::string& filePath, int flags) {
        fd_ = ::open(filePath.c_str(), flags);
        autoClose_ = true;
        return fd_ >= 0;
    }
    bool open(const std::string& filePath, int flags, mode_t mode) {
        fd_ = ::open(filePath.c_str(), flags, mode);
        autoClose_ = true;
        return fd_ >= 0;
    }
    void setFd(int fd, bool autoClose = false) {
        fd_ = fd;
        autoClose_ = autoClose;
    }
    void close() {
        if (!autoClose_ || fd_ < 0) return;
        if (::close(fd_) < 0) {
            throwLibcError("close failed.");
        }
        fd_ = -1;
    }
    bool seekable() {
        return ::lseek(fd(), 0, SEEK_CUR) != -1;
    }
    off_t lseek(off_t oft, int whence = SEEK_SET) {
        off_t ret = ::lseek(fd(), oft, whence);
        if (ret == off_t(-1)) {
            throwLibcError("lseek failed.");
        }
        return ret;
    }
    size_t readsome(void *data, size_t size) {
        ssize_t r = ::read(fd(), data, size);
        if (r < 0) throwLibcError("read failed.");
        return r;
    }
    void read(void *data, size_t size) {
        char *buf = reinterpret_cast<char *>(data);
        size_t s = 0;
        while (s < size) {
            size_t r = readsome(&buf[s], size - s);
            if (r == 0) throw EofError();
            s += r;
        }
    }
    void skip(size_t size) {
        if (seekable()) {
            lseek(size, SEEK_CUR);
            return;
        }
        char buf[4096];
        size_t s = 0;
        while (s < size) {
            size_t r = readsome(&buf[0], std::min<size_t>(size - s, 4096));
            if (r == 0) throw EofError();
            s += r;
        }
    }
    void pread(void *data, size_t size, off_t off) {
        lseek(off);
        read(data, size);
    }
    void write(const void *data, size_t size) {
        const char *buf = reinterpret_cast<const char *>(data);
        size_t s = 0;
        while (s < size) {
            ssize_t r = ::write(fd(), &buf[s], size - s);
            if (r < 0) throwLibcError("write failed.");
            if (r == 0) throw EofError();
            s += r;
        }
    }
    void pwrite(const void *data, size_t size, off_t off) {
        lseek(off);
        write(data, size);
    }
    void fdatasync() {
        if (::fdatasync(fd()) < 0) {
            throwLibcError("fdsync failed.");
        }
    }
    void fsync() {
        if (::fsync(fd()) < 0) {
            throwLibcError("fsync failed.");
        }
    }
    void ftruncate(off_t length) {
        if (::ftruncate(fd(), length) < 0) {
            throwLibcError("ftruncate failed.");
        }
    }
    void fadvise(off_t off, off_t len, int advise) {
        int ret = ::posix_fadvise(fd(), off, len, advise);
        if (ret != 0) {
            throwLibcErrorWithNo("posix_fadvise failed.", ret);
        }
    }
};

/**
 * Create a file if it does not exist.
 */
inline void createEmptyFile(const std::string &path, mode_t mode = 0644)
{
    struct stat st;
    if (::stat(path.c_str(), &st) == 0) return;
    File writer(path, O_CREAT | O_TRUNC | O_RDWR, mode);
    writer.close();
}

/**
 * Read all contents from a file.
 * Do not specify streams.
 * Do not use this for large files.
 *
 * String: it must have size(), resize(), and operator[].
 *   such as std::string and std::vector<char>.
 */
template <typename String>
inline void readAllFromFile(File &file, String &buf)
{
    constexpr const size_t usize = 4096; // unit size.
    size_t rsize = buf.size(); // read data will be appended to buf.

    for (;;) {
        if (buf.size() < rsize + usize) buf.resize(rsize + usize);
        const size_t r = file.readsome(&buf[rsize], usize);
        if (r == 0) break;
        rsize += r;
    }
    buf.resize(rsize);
}

template <typename String>
inline void readAllFromFile(const std::string &path, String &buf)
{
    File file(path, O_RDONLY);
    readAllFromFile(file, buf);
    file.close();
}

}} //namespace cybozu::util
