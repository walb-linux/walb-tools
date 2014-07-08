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
#include "bdev_util.hpp"

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
public:
    File()
        : fd_(-1), autoClose_(false) {
    }
    File(const std::string& filePath, int flags)
        : File() {
        if (!open(filePath, flags)) throw LibcError(errno, "open failed: ");
    }
    File(const std::string& filePath, int flags, mode_t mode)
        : File() {
        if (!open(filePath, flags, mode)) throw LibcError(errno, "open failed: ");
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
            throw LibcError(errno, "close failed: ");
        }
        fd_ = -1;
    }
    bool seekable() {
        return ::lseek(fd(), 0, SEEK_CUR) != -1;
    }
    off_t lseek(off_t oft, int whence = SEEK_SET) {
        off_t ret = ::lseek(fd(), oft, whence);
        if (ret == off_t(-1)) {
            throw LibcError(errno, "lseek failed: ");
        }
        return ret;
    }
    size_t readsome(void *data, size_t size) {
        ssize_t r = ::read(fd(), data, size);
        if (r < 0) throw LibcError(errno, "read failed: ");
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
    void pread(void *data, size_t size, off_t off) {
        lseek(off);
        read(data, size);
    }
    void write(const void *data, size_t size) {
        const char *buf = reinterpret_cast<const char *>(data);
        size_t s = 0;
        while (s < size) {
            ssize_t r = ::write(fd(), &buf[s], size - s);
            if (r < 0) throw LibcError(errno, "write failed: ");
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
            throw LibcError(errno, "fdsync failed: ");
        }
    }
    void fsync() {
        if (::fsync(fd()) < 0) {
            throw LibcError(errno, "fsync failed: ");
        }
    }
    void ftruncate(off_t length) {
        if (::ftruncate(fd(), length) < 0) {
            throw LibcError(errno, "ftruncate failed: ");
        }
    }
};

/**
 * Block device manager.
 *
 * You can do write/read or other operations on the device.
 * You can deal with normal file as a block device also.
 */
class BlockDevice /* final */
{
private:
    std::string name_;
    int openFlags_;
    int fd_;
    bool isBlockDevice_;
    uint64_t deviceSize_; // [bytes].
    unsigned int lbs_; // logical block size [bytes].
    unsigned int pbs_; // physical block size [bytes].
public:
    BlockDevice()
        : name_()
        , openFlags_()
        , fd_(-1)
        , isBlockDevice_(false)
        , deviceSize_(0)
        , lbs_(0)
        , pbs_(0) {
    }
    BlockDevice(const std::string& name, int flags)
        : name_(name)
        , openFlags_(flags)
        , fd_(openDevice(name, flags))
        , isBlockDevice_(cybozu::util::isBlockDevice(fd_))
        , deviceSize_(cybozu::util::getBlockDeviceSize(fd_))
        , lbs_(cybozu::util::getLogicalBlockSize(fd_))
        , pbs_(cybozu::util::getPhysicalBlockSize(fd_)) {
#if 0
        ::printf("device %s size %zu isWrite %d isDirect %d isBlockDevice %d "
                 "lbs %u pbs %u\n",
                 name_.c_str(), deviceSize_,
                 (openFlags_ & O_RDWR) != 0, (openFlags_ & O_DIRECT) != 0,
                 isBlockDevice_, lbs_, pbs_);
#endif
    }
    DISABLE_COPY_AND_ASSIGN(BlockDevice);

    void swap(BlockDevice& rhs) noexcept {
        name_.swap(rhs.name_);
        std::swap(openFlags_, rhs.openFlags_);
        std::swap(fd_, rhs.fd_);
        std::swap(isBlockDevice_, rhs.isBlockDevice_);
        std::swap(deviceSize_, rhs.deviceSize_);
        std::swap(lbs_, rhs.lbs_);
        std::swap(pbs_, rhs.pbs_);
    }
    BlockDevice(BlockDevice&& rhs)
        : BlockDevice() {
        swap(rhs);
    }

    BlockDevice& operator=(BlockDevice&& rhs) {
        close();
        swap(rhs);
        return *this;
    }

    ~BlockDevice() noexcept {
        try {
            close();
        } catch (...) {
        }
    }

    void close() {
        if (fd_ < 0) return;
        if (::close(fd_) < 0) {
            throw LibcError(errno, "close failed: ");
        }
        fd_ = -1;
    }

    /**
     * Read data.
     */
    void read(void *data, size_t size) {
        char *p = (char *)data;
        while (0 < size) {
            ssize_t s = ::read(fd_, p, size);
            if (s < 0) throw LibcError(errno, "read failed: ");
            if (s == 0) throw EofError();
            p += s;
            size -= s;
        }
    }

    /**
     * Write data.
     */
    void write(const void *data, size_t size) {
        const char *p = (const char *)data;
        while (0 < size) {
            ssize_t s = ::write(fd_, p, size);
            if (s < 0) throw LibcError(errno, "write failed: ");
            if (s == 0) throw EofError();
            p += s;
            size -= s;
        }
    }

    /**
     * Seek and read.
     */
    void read(off_t oft, size_t size, void *data) {
        if (deviceSize_ < oft + size) throw EofError();
        seek(oft);
        read(data, size);
    }

    /**
     * Seek and write.
     */
    void write(off_t oft, size_t size, const void* data) {
        if (deviceSize_ < oft + size) throw EofError();
        seek(oft);
        write(data, size);
    }

    /**
     * lseek.
     */
    void seek(off_t oft) {
        if (::lseek(fd_, oft, SEEK_SET) < 0) {
            throw LibcError(errno, "lsddk failed: ");
        }
    }

    /**
     * fdatasync.
     */
    void fdatasync() {
        int ret = ::fdatasync(fd_);
        if (ret) {
            throw LibcError(errno, "fdatasync failed: ");
        }
    }

    /**
     * fsync.
     */
    void fsync() {
        int ret = ::fsync(fd_);
        if (ret) {
            throw LibcError(errno, "fsync failed: ");
        }
    }

    /**
     * Get device size [byte].
     */
    uint64_t getDeviceSize() const { return deviceSize_; }

    /**
     * Open flags.
     */
    int getFlags() const { return openFlags_; }

    /**
     * File descriptor.
     */
    int getFd() const { return fd_; }

    /**
     * RETURN:
     *   True if the descriptor is of a block device file,
     *   or false.
     */
    bool isBlockDevice() const { return isBlockDevice_; }
    uint32_t getPhysicalBlockSize() const { return pbs_; }
    uint32_t getLogicalBlockSize() const { return lbs_; }

private:
    /**
     * Helper function for constructor.
     */
    static int openDevice(const std::string& name, int flags) {
        int fd = ::open(name.c_str(), flags);
        if (fd < 0) {
            throw LibcError(
                errno, formatString("open %s failed: ", name.c_str()).c_str());
        }
        return fd;
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

}} //namespace cybozu::util
