#include <string>
#include <stdexcept>
#include "file_path.hpp"
#include "meta.hpp"
#include "wdiff_data.hpp"

#ifndef WALB_FOR_TEST_HPP
#define WALB_FOR_TEST_HPP

class TestDirectory
{
private:
    cybozu::FilePath fp_;
    bool isTmp_;

public:
    explicit TestDirectory(const std::string &path, bool isTmp = true)
        : fp_(path), isTmp_(isTmp) {
        if (fp_.stat().exists()) {
            throw std::runtime_error("directory already exists.");
        }
        if (!fp_.mkdir()) {
            throw std::runtime_error("mkdir() failed.");
        }
    }
    ~TestDirectory() noexcept {
        try {
            fp_.printRecursive();
            if (isTmp_) {
                fp_.rmdirRecursive();
            }
        } catch (...) {
        }
    }
};

void setDiff(walb::MetaDiff &diff, uint64_t gid0, uint64_t gid1, bool canMerge)
{
    diff.init();
    diff.raw().gid0 = gid0;
    diff.raw().gid1 = gid1;
    diff.raw().gid2 = gid1;
    diff.raw().can_merge = canMerge;
};

void createDiffFile(const walb::WalbDiffFiles &diffFiles, walb::MetaDiff &diff)
{
    cybozu::FilePath fp = diffFiles.dirPath()
        + cybozu::FilePath(walb::createDiffFileName(diff));
    cybozu::util::createEmptyFile(fp.str());
}

#endif /* WALB_FOR_TEST_HPP */
