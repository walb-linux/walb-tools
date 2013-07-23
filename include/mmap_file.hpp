/**
 * @file
 * @brief Mmaped file wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <exception>
#include <algorithm>
#include <cstdio>
#include "fileio.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>

#ifndef MMAP_FILE_HPP
#define MMAP_FILE_HPP

namespace cybozu {
namespace util {

/**
 * Mmaped file wrapper.
 * You can inherit this class and add your favorite header data manaement.
 */
class MmappedFile
{
private:
    const unsigned int pageSize_;
    void *mapped_;
    uint64_t mappedSize_;
    uint64_t size_;
    FileOpener fo_;

public:
    /**
     * @size file size in bytes. 0 means not changing the file size.
     * @filePath File path.
     * @flags open flags.
     */
    MmappedFile(uint64_t size, const std::string& filePath, int flags)
        : pageSize_(getpagesize())
        , mapped_(nullptr)
        , mappedSize_(0)
        , size_(size)
        , fo_(filePath, flags) {
        init();
    }

    /**
     * @size file size in bytes.
     * @filePath Bitmap file path.
     * @flags open flags.
     * @mode open mode.
     */
    MmappedFile(uint64_t size, const std::string& filePath, int flags, int mode)
        : pageSize_(getpagesize())
        , mapped_(nullptr)
        , mappedSize_(0)
        , size_(size)
        , fo_(filePath, flags, mode) {
        init();
    }

    MmappedFile(const std::string& filePath, int flags)
        : MmappedFile(0, filePath, flags) {
    }

    virtual ~MmappedFile() noexcept {
        try {
            munmap();
        } catch (...) {
        }
    }

    /**
     * Current size. This is not file size.
     */
    uint64_t size() const { return size_; }

    /**
     * Pointer to access the mmapped file contents.
     */
    template <typename T> const T* ptr() const {
        return reinterpret_cast<const T*>(mapped_);
    }
    template <typename T> T* ptr() {
        return reinterpret_cast<T*>(mapped_);
    }

    /**
     * Sync data down to the file.
     */
    void sync() {
        if (::msync(mapped_, mappedSize_, MS_SYNC)) {
            throw std::runtime_error("msync failed.");
        }
        if (::fdatasync(fo_.fd())) {
            throw std::runtime_error("fdatasync failed.");
        }
    }

    /**
     * Resize the file and mapping.
     */
    void resize(uint64_t size) {
        size_ = size;
        uint64_t fileSize = adjustFileSize();
        mremap(fileSize);
    }

private:
    void init() {
        uint64_t fileSize = 0;
        if (0 < size_) {
            fileSize = adjustFileSize();
        } else {
            fileSize = lseek(0, SEEK_END);
            size_ = fileSize;
        }
        mmap(fileSize);
    }

    uint64_t calcFileSize() const {
        return ((size_ - 1) / pageSize_ + 1) * pageSize_;
    }

    uint64_t lseek(off_t offset, int whence) {
        off_t off = ::lseek(fo_.fd(), offset, whence);
        if (off < 0) {
            throw std::runtime_error("lseek failed.");
        }
        return off;
    }

    void appendZeroData(uint64_t len) {
        FdWriter fdw(fo_.fd());
        fdw.lseek(0, SEEK_END);
        char buf[4096];
        ::memset(buf, 0, 4096);
        while (0 < len) {
            size_t l = std::min(len, uint64_t(4096));
            fdw.write(buf, l);
            len -= l;
        }
    }

    uint64_t adjustFileSize() {
        uint64_t fileSize = calcFileSize();
        uint64_t currFileSize = lseek(0, SEEK_END);
        if (currFileSize < fileSize) {
            appendZeroData(fileSize - currFileSize);
        }
        if (fileSize < currFileSize) {
            if (::ftruncate(fo_.fd(), fileSize)) {
                throw std::runtime_error("ftruncate failed.");
            }
        }
        return fileSize;
    }

    void mmap(uint64_t fileSize) {
        lseek(0, SEEK_SET);
        void *p = ::mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fo_.fd(), 0);
        if (p == MAP_FAILED) {
            throw std::runtime_error("mmap failed.");
        }
        mapped_ = p;
        mappedSize_ = fileSize;
    }

    void munmap() {
        if (::munmap(mapped_, mappedSize_)) {
            throw std::runtime_error("munmap failed.");
        }
        mapped_ = nullptr;
        mappedSize_ = 0;
    }

    void mremap(uint64_t fileSize) {
        void *p = ::mremap(mapped_, mappedSize_, fileSize, MREMAP_MAYMOVE);
        if (p == MAP_FAILED) {
            throw std::runtime_error("mremap failed.");
        }
        mapped_ = p;
        mappedSize_ = fileSize;
    }
};

}} //namespace cybozu::util

#endif /* MMAP_FILE_HPP */
