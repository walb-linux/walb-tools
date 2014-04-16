#pragma once
/**
 * @file
 * @brief File or fd utility.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <memory>
#include <string>
#include "fileio.hpp"

class FileOrFd
{
private:
    int fd_;
    std::unique_ptr<cybozu::util::FileOpener> fo_;
public:
    FileOrFd() : fd_(-1), fo_() {}
    void setFd(int fd) { fd_ = fd; }
    void open(const std::string &path, int flags) {
        fo_.reset(new cybozu::util::FileOpener(path, flags));
    }
    void open(const std::string &path, int flags, mode_t mode) {
        fo_.reset(new cybozu::util::FileOpener(path, flags, mode));
    }
    void close() { if (fo_) fo_.reset(); }
    int fd() const { return fo_ ? fo_->fd() : fd_; }
};
