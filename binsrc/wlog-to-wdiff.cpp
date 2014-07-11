/**
 * @file
 * @brief Convert walb logs to a walb diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_diff_converter.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    uint32_t maxIoSize;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setUsage("Usage: wlog-to-wdiff < [wlogs] > [wdiff]", true);
        opt.appendOpt(&maxIoSize, 64 * KIBI, "x", "max IO size in the output wdiff [byte].");
        opt.appendHelp("h");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    diff::Converter c;
    c.convert(0, 1, opt.maxIoSize / LBS);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-to-wdiff")
