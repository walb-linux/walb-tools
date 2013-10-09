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
#include <getopt.h>

#include "stdout_logger.hpp"

#include "util.hpp"
#include "walb_log_file.hpp"
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
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("ldevPath: %s\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 ldevPath().c_str(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (ldevPath_.empty()) {
            throw RT_ERR("Specify log device path.");
        }
    }
private:
    /* Option ids. */
    enum Opt {
        VERBOSE = 1,
        HELP,
    };

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::VERBOSE:
            case 'v':
                isVerbose_ = true;
                break;
            case Opt::HELP:
            case 'h':
                isHelp_ = true;
                break;
            default:
                throw RT_ERR("Unknown option.");
            }
        }

        while(optind < argc) {
            args_.push_back(std::string(argv[optind++]));
        }
        if (!args_.empty()) {
            ldevPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlinfo: show superblock information of a log device.\n"
            "Usage: wlinfo [options] LOG_DEVICE_PATH\n"
            "Options:\n"
            "  -v, --verbose:        verbose messages to stderr.\n"
            "  -h, --help:           show this message.\n");
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
    walb::log::SuperBlock super_;
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

int main(int argc, char* argv[])
{
    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WalbLogInfo wlInfo(config);
        wlInfo.show();
        return 0;
    } catch (std::exception& e) {
        LOGe("Exception: %s\n", e.what());
    } catch (...) {
        LOGe("Caught other error.\n");
    }
    return 1;
}

/* end of file. */
