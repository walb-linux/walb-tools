#pragma once
/**
 * @file
 * @brief process wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <wait.h>

#include "fileio.hpp"

namespace cybozu {
namespace process {

/**
 * Wrapper of pipe().
 */
class Pipe
{
private:
    int fd_[2];

public:
    Pipe() {
        if (::pipe(fd_) < 0) {
            throw std::runtime_error("pipe() failed.");
        }
    }
    ~Pipe() noexcept {
        try { closeR(); } catch (...) {}
        try { closeW(); } catch (...) {}
    }
    int fdR() { return fd_[0]; }
    int fdW() { return fd_[1]; }
    void closeR() { closeDetail(0); }
    void closeW() { closeDetail(1); }
    void dupR(int fd) { dupDetail(fd, 0); }
    void dupW(int fd) { dupDetail(fd, 1); }
private:
    void closeDetail(size_t i) {
        if (fd_[i] < 0) return;
        if (::close(fd_[i]) < 0)
            throw std::runtime_error("close() failed.");
        fd_[i] = -1;
    }
    void dupDetail(int fd, size_t i) {
        if (fd_[i] < 0)
            throw std::runtime_error("fd is invalid.");
        if (::dup2(fd_[i], fd) < 0)
            throw std::runtime_error("dup2() failed.");
        closeDetail(i);
    }
};

/**
 * @fd 0, 1, or 2.
 */
void redirectToNullDev(int fd)
{
    int flags = (fd == 0 ? O_RDONLY : O_WRONLY);
    int tmpFd = ::open("/dev/null", flags);
    if (tmpFd < 0)
        throw std::runtime_error("open() failed.");
    if (::dup2(tmpFd, fd) < 0)
        throw std::runtime_error("dup2() failed.");
    if (::close(tmpFd) < 0)
        throw std::runtime_error("close() failed.");
}

/**
 * Close all file descriptors except for 0, 1, and 2.
 */
void closeAllFileDescriptors()
{
    int maxFd = ::sysconf(_SC_OPEN_MAX);
    maxFd = maxFd < 0 ? 1024 : maxFd;
    for (int i = 3; i < maxFd; i++) {
        ::close(i);
    }
}

/**
 * Create command line arguments for execv().
 */
std::vector<char *> prepareArgv(const std::string &cmd, const std::vector<std::string> &args)
{
    std::vector<char *> argv;
    argv.push_back(const_cast<char *>(cmd.c_str()));
    for (const std::string &arg : args) {
        argv.push_back(const_cast<char *>(arg.c_str()));
    }
    argv.push_back(nullptr);
    return argv;
}

/**
 * Call another command and get stdout as a result.
 */
std::string call(const std::string& cmd, const std::vector<std::string> &args)
{
    Pipe pipe0;
    pid_t cpid;
    cpid = ::fork();
    if (cpid < 0) {
        throw std::runtime_error("fork() failed.");
    }
    if (cpid == 0) { /* child process */
        /* bind pipe and stdout. */
        pipe0.closeR();
        pipe0.dupW(1);
        try {
            redirectToNullDev(0);
            redirectToNullDev(2);
        } catch (...) {
            ::exit(1);
        }
        closeAllFileDescriptors();
        std::vector<char *> argv = prepareArgv(cmd, args);
        ::execv(cmd.c_str(), &argv[0]);
    }
    /* parent process. */

    /* Read the stdout of the child process. */
    pipe0.closeW();
    cybozu::util::FdReader reader(pipe0.fdR());
    char buf[4096];
    std::stringstream ss;
    size_t r;
    while ((r = reader.readsome(buf, 4096)) > 0) {
        ss.write(buf, r);
    }

    /* wait for done */
    int status;
    ::waitpid(cpid, &status, 0);
    if (status != 0) {
        throw std::runtime_error("child process has returned non-zero.");
    }

    return ss.str();
}

}} //namespace cybozu::process
