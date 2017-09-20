/**
 * @file
 * @brief This command waits for a logical volume available after lvcreate called.
 * @author HOSHINO Takashi

 * (C) 2017 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "lvm.hpp"
#include "file_path.hpp"
#include "walb_util.hpp"


using namespace walb;


struct Option
{
    std::string lvPath;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("This command waits for a logical block "
                           "device available after lvcreate called.");
        opt.appendParam(&lvPath, "LOGICAL_BLOCK_DEVICE_PATH");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};


int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::FilePath path(opt.lvPath);
    if (!path.stat().exists()) {
        throw cybozu::Exception("logical device path not found") << opt.lvPath;
    }
    cybozu::lvm::waitForDeviceAvailable(path);

    return 0;
}


DEFINE_ERROR_SAFE_MAIN("wait-for-lv")
