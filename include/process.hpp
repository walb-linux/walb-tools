/**
 * @file
 * @brief process wrapper.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef CYBOZU_PROCESS_HPP
#define CYBOZU_PROCESS_HPP

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
 * Call another command and get stdout as a result.
 */
std::string call(const std::string& cmd, const std::vector<std::string> &args)
{
    int pipefd[2];
    pid_t cpid;
    if (::pipe(pipefd) < 0) {
        throw std::runtime_error("pipe() failed.");
    }
    cpid = ::fork();
    if (cpid < 0) {
        throw std::runtime_error("fork() failed.");
    }
    if (cpid == 0) { /* child process */
        /* bind pipe and stdout. */
        ::close(pipefd[0]);
        ::dup2(pipefd[1], 1);
        ::close(pipefd[1]);
        int fd0 = ::open("/dev/null", O_RDONLY);
        int fd2 = ::open("/dev/null", O_RDWR);
        if (fd0 < 0 || fd2 < 0) ::exit(1);
        ::dup2(fd0, 0);
        ::dup2(fd2, 2);
        ::close(fd0);
        ::close(fd2);

        /* close all file descriptor. */
        int maxFd = ::sysconf(_SC_OPEN_MAX);
        maxFd = maxFd < 0 ? 1024 : maxFd;
        for (int i = 3; i < maxFd; i++) {
            ::close(i);
        }

        /* prepare argv */
        std::vector<char *> argv;
        argv.push_back(const_cast<char *>(cmd.c_str()));
        for (const std::string &arg : args) {
            argv.push_back(const_cast<char *>(arg.c_str()));
        }
        argv.push_back(nullptr);

        ::execv(cmd.c_str(), &argv[0]);
    }
    /* parent process. */

    /* Read the stdout of the child process. */
    ::close(pipefd[1]);
    cybozu::util::FdReader reader(pipefd[0]);
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
        ::close(pipefd[0]);
        throw std::runtime_error("child process has returned non-zero.");
    }

    ::close(pipefd[0]);
    return ss.str();
}

}} //namespace cybozu::process

#endif /* CYBOZU_PROCESS */
