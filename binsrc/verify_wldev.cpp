/**
 * @file
 * @brief Verify logs on a walb log device by comparing with an IO recipe.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "io_recipe.hpp"
#include "linux/walb/common.h"
#include "linux/walb/block_size.h"
#include "walb_util.hpp"
#include "walb_log_verify.hpp"

using namespace walb;

struct Option
{
    uint64_t bgnLsid;
    uint64_t endLsid;
    std::string recipePath; /* recipe path or "-" for stdin. */
    std::string wldevPath; /* walb log device path. */
    bool dontUseAio;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("verify_wldev: verify logs on a walb log device with an IO recipe.");
        opt.appendOpt(&bgnLsid, 0, "b", "LSID: begin lsid. (default: oldest lsid)");
        opt.appendOpt(&endLsid, -1, "e", "LSID: end lsid. (default: written lsid)");
        opt.appendOpt(&recipePath, "-", "r", "PATH: recipe file path. '-' for stdin. (default: '-')");
        opt.appendParam(&wldevPath, "WALB_LOG_DEVICE");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

template <typename Reader>
void verifyWldev(const Option &opt)
{
    cybozu::util::File wldevFile(opt.wldevPath, O_RDONLY | O_DIRECT);
    Reader wldevReader(std::move(wldevFile));
    device::SuperBlock &super = wldevReader.super();
    const uint64_t bgnLsid = std::max(opt.bgnLsid, super.getOldestLsid());
    wldevReader.reset(bgnLsid);

    cybozu::util::File recipeFile;
    if (opt.recipePath != "-") {
        recipeFile.open(opt.recipePath, O_RDONLY);
    } else {
        recipeFile.setFd(0);
    }
    util::IoRecipeParser recipeParser(recipeFile.fd());

    verifyLogStream(wldevReader, recipeParser, bgnLsid, opt.endLsid, super.pbs(), super.salt());
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    if (opt.dontUseAio) {
        verifyWldev<device::SimpleWldevReader>(opt);
    } else {
        verifyWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("verify_wldev")
