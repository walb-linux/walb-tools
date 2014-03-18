#pragma once

#include <string>
#include <stdexcept>
#include "file_path.hpp"
#include "meta.hpp"
#include "wdiff_data.hpp"
#include "tmp_file_serializer.hpp"

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

void setDiff(walb::MetaDiff &diff, uint64_t gid0, uint64_t gid1, bool isMergeable)
{
    diff.snapB.gidB = gid0;
    diff.snapB.gidE = gid0;
    diff.snapE.gidB = gid1;
    diff.snapE.gidE = gid1;
    diff.isMergeable = isMergeable;
};

void createDiffFile(const walb::WalbDiffFiles &diffFiles, walb::MetaDiff &diff)
{
    cybozu::FilePath fp = diffFiles.dirPath()
        + cybozu::FilePath(walb::createDiffFileName(diff));
    cybozu::util::createEmptyFile(fp.str());
}
