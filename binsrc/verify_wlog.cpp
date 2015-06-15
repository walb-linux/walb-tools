/**
 * @file
 * @brief Verify a walb log by comparing with an IO recipe.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "walb_log_file.hpp"
#include "io_recipe.hpp"
#include "linux/walb/common.h"
#include "linux/walb/block_size.h"
#include "walb_util.hpp"
#include "walb_log_verify.hpp"

using namespace walb;

struct Option
{
    std::string wlogPath; /* walb log path or "-" for stdin. */
    std::string recipePath; /* recipe path or "-" for stdin. */
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("verify_wlog: verify a walb log with an IO recipe.");
        opt.appendOpt(&recipePath, "-", "r", "PATH: recipe file path. '-' for stdin. (default: '-')");
        opt.appendOpt(&wlogPath, "-", "w", "PATH: wlog file path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (recipePath == "-" && wlogPath == "-") {
            throw RT_ERR("Specify -r or -w option.");
        }
    }
};

cybozu::util::File getFile(const std::string &path) {
    cybozu::util::File f;
    if (path == "-") {
        f.setFd(0);
    } else {
        f.open(path, O_RDONLY);
    }
    return f;
}

void verifyWlog(const Option &opt)
{
    cybozu::util::File recipeFile = getFile(opt.recipePath);
    util::IoRecipeParser recipeParser(recipeFile.fd());

    cybozu::util::File wlogFile = getFile(opt.wlogPath);
    WlogFileHeader wh;
    wh.readFrom(wlogFile);

    verifyLogStream(wlogFile, recipeParser, wh.beginLsid(), -1, wh.pbs(), wh.salt());
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    verifyWlog(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("verify_wlog")
