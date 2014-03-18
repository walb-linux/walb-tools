/**
 * @file
 * @brief Get information of a log device.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <queue>
#include <memory>
#include <deque>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "walb_log_dev.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    std::string ldevPath_;
    bool isVerbose_;
public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , isVerbose_(false) {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    bool isVerbose() const { return isVerbose_; }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlinfo: show superblock information of a log device.");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&ldevPath_, "LOG_DEVICE_PATH");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

/**
 * To get log device suprblock.
 */
class WalbLogInfo
{
private:
    const Config& config_;
    cybozu::util::BlockDevice bd_;
    walb::device::SuperBlock super_;
    const size_t blockSize_;

public:
    WalbLogInfo(const Config& config)
        : config_(config)
        , bd_(config.ldevPath().c_str(), O_RDONLY | O_DIRECT)
        , super_(bd_)
        , blockSize_(bd_.getPhysicalBlockSize()) {
    }

    void show() {
        super_.print();
    }
};

int main(int argc, char* argv[]) try
{
    cybozu::SetLogFILE(::stderr);
    Config config(argc, argv);
    WalbLogInfo wlInfo(config);
    wlInfo.show();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
