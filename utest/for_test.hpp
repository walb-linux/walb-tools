#include <string>
#include <stdexcept>
#include "file_path.hpp"

#ifndef WALB_FOR_TEST_HPP
#define WALB_FOR_TEST_HPP

class TestDirectory
{
private:
    cybozu::FilePath fp_;
public:
    explicit TestDirectory(const std::string &path)
        : fp_(path) {
        if (fp_.stat().exists()) {
            throw std::runtime_error("directory already exists.");
        }
        if (!fp_.mkdir()) {
            throw std::runtime_error("mkdir() failed.");
        }
    }
    ~TestDirectory() noexcept {
        try {
            fp_.rmdirRecursive();
        } catch (...) {
        }
    }
};

#endif /* WALB_FOR_TEST_HPP */
