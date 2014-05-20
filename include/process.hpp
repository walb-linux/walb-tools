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
#include <thread>

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
inline void redirectToNullDev(int fd)
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
inline void closeAllFileDescriptors()
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
inline std::vector<char *> prepareArgv(const std::string &cmd, const std::vector<std::string> &args)
{
    std::vector<char *> argv;
    argv.push_back(const_cast<char *>(cmd.c_str()));
    for (const std::string &arg : args) {
        argv.push_back(const_cast<char *>(arg.c_str()));
    }
    argv.push_back(nullptr);
    return argv;
}

inline void streamToStr(int fdr, std::string &outStr, std::exception_ptr& ep) noexcept
{
    try {
        std::stringstream ss;
        cybozu::util::File reader(fdr);
        char buf[4096];
        size_t r;
        while ((r = reader.readsome(buf, 4096)) > 0) {
            ss.write(buf, r);
        }
        outStr = ss.str();
    } catch (...) {
        ep = std::current_exception();
    }
}

/**
 * Call another command and get stdout as a result.
 */
inline std::string call(const std::string& cmd, const std::vector<std::string> &args)
{
    Pipe pipe0, pipe1;
    pid_t cpid;
    cpid = ::fork();
    if (cpid < 0) {
        throw std::runtime_error("fork() failed.");
    }
    if (cpid == 0) { /* child process */
        /* bind pipe and stdout. */
        pipe0.closeR();
        pipe0.dupW(1);
        pipe1.closeR();
        pipe1.dupW(2);
        try {
            redirectToNullDev(0);
        } catch (...) {
            ::exit(1);
        }
        closeAllFileDescriptors();
        std::vector<char *> argv = prepareArgv(cmd, args);
        ::execv(cmd.c_str(), &argv[0]);
    }
    /* parent process. */

    /* Read the stdout/stderr of the child process. */
    pipe0.closeW();
    pipe1.closeW();
    std::string stdOutStr, stdErrStr;
    std::exception_ptr epOut, epErr;
    std::thread th0(streamToStr, pipe0.fdR(), std::ref(stdOutStr), std::ref(epOut));
    std::thread th1(streamToStr, pipe1.fdR(), std::ref(stdErrStr), std::ref(epErr));
    th0.join();
    th1.join();

    /* wait for done */
    int status;
    ::waitpid(cpid, &status, 0);

    /* handle errors */
    if (status != 0) {
        std::string msg("child process has returned non-zero:");
        msg += cybozu::util::formatString("%d\n", status);
        msg += "cmd:" + cmd + "\n";
        msg += "args:";
        for (const std::string &arg : args) msg += arg + " ";
        msg += "\nstderr:" + stdErrStr;
        throw std::runtime_error(msg);
    }
    if (epOut) std::rethrow_exception(epOut);
    if (epErr) std::rethrow_exception(epErr);

    return stdOutStr;
}

}} //namespace cybozu::process
