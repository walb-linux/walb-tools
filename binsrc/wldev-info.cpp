/**
 * @file
 * @brief Get information of a log device.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

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

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    cybozu::util::File file(config.ldevPath(), O_RDONLY | O_DIRECT);
    walb::device::SuperBlock super;
    super.read(file.fd());
    super.print();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wldev-info")
