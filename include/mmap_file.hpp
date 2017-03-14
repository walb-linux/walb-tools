#pragma once
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

namespace cybozu {
namespace util {

/**
 * Mmaped file wrapper.
 */
class MmappedFile final
{
private:
    void *mapped_;
    uint64_t mappedSize_;
    uint64_t size_;
    mutable File file_;

public:
    MmappedFile()
        : mapped_(nullptr)
        , mappedSize_(0)
        , size_(0)
        , file_() {
    }
    /**
     * @size file size in bytes. 0 means not changing the file size.
     * @filePath File path.
     * @flags open flags.
     */
    MmappedFile(uint64_t size, const std::string& filePath, int flags)
        : MmappedFile() {
        reset(File(filePath, flags), size);
    }
    /**
     * @size file size in bytes.
     * @filePath Bitmap file path.
     * @flags open flags.
     * @mode open mode.
     */
    MmappedFile(uint64_t size, const std::string& filePath, int flags, int mode)
        : MmappedFile() {
        reset(File(filePath, flags, mode), size);
    }
    MmappedFile(const std::string& filePath, int flags)
        : MmappedFile() {
        reset(File(filePath, flags));
    }
    MmappedFile(MmappedFile &&rhs)
        : MmappedFile() {
        swap(std::move(rhs));
    }
    ~MmappedFile() noexcept try {
        reset();
    } catch (...) {
    }
    MmappedFile& operator=(MmappedFile &&rhs) {
        reset();
        swap(std::move(rhs));
        return *this;
    }
    void reset(File &&file, size_t size = 0) {
        munmap();
        file_.close();
        file_ = std::move(file);
        size_ = size;
        init();
    }
    void reset() {
        munmap();
        file_.close();
        size_ = 0;
    }
    void swap(MmappedFile &&rhs) {
        std::swap(mapped_, rhs.mapped_);
        std::swap(mappedSize_, rhs.mappedSize_);
        std::swap(size_, rhs.size_);
        std::swap(file_, rhs.file_);
    }
    /**
     * Current size. This is not file size.
     */
    uint64_t size() const { return size_; }
    uint64_t getFileSize() const { return lseek(0, SEEK_END); }
    /**
     * Pointer to access the mmapped file.
     */
    template <typename T> const T* ptr() const {
        return reinterpret_cast<const T*>(mapped_);
    }
    template <typename T> T* ptr() {
        return reinterpret_cast<T*>(mapped_);
    }
    /**
     * Char reference to access the mmapped file.
     */
    char &operator[](size_t i) {
        assert(mapped_);
        return *((char *)mapped_ + i);
    }
    const char &operator[](size_t i) const {
        assert(mapped_);
        return *((const char *)mapped_ + i);
    }
    /**
     * Sync data down to the file.
     */
    void sync() {
        if (::msync(mapped_, mappedSize_, MS_SYNC)) {
            throw std::runtime_error("msync failed.");
        }
        file_.fdatasync();
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
            fileSize = getFileSize();
            size_ = fileSize;
        }
        mmap(fileSize);
    }
    uint64_t calcFileSize() const {
        return ((size_ - 1) / pageSize() + 1) * pageSize();
    }
    uint64_t lseek(off_t offset, int whence) const {
        return file_.lseek(offset, whence);
    }
    void appendZeroData(uint64_t len) {
        lseek(0, SEEK_END);
        char buf[4096];
        ::memset(buf, 0, 4096);
        while (0 < len) {
            size_t l = std::min(len, uint64_t(4096));
            file_.write(buf, l);
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
            file_.ftruncate(fileSize);
        }
        return fileSize;
    }
    void mmap(uint64_t fileSize) {
        if (mapped_) return;
        lseek(0, SEEK_SET);
        void *p = ::mmap(NULL, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, file_.fd(), 0);
        if (p == MAP_FAILED) {
            throw std::runtime_error("mmap failed.");
        }
        mapped_ = p;
        mappedSize_ = fileSize;
    }
    void munmap() {
        if (!mapped_) return;
        if (::munmap(mapped_, mappedSize_)) {
            throw std::runtime_error("munmap failed.");
        }
        mapped_ = nullptr;
        mappedSize_ = 0;
    }
    void mremap(uint64_t fileSize) {
        if (!mapped_) return;
        void *p = ::mremap(mapped_, mappedSize_, fileSize, MREMAP_MAYMOVE);
        if (p == MAP_FAILED) {
            throw std::runtime_error("mremap failed.");
        }
        mapped_ = p;
        mappedSize_ = fileSize;
    }
    static size_t pageSize() {
        static const size_t s = ::getpagesize();
        return s;
    }
};

}} //namespace cybozu::util
