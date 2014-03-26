/**
 * @file
 * @brief Convert walb logs to a walb diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <random>
#include <stdexcept>
#include <cstdint>
#include <cinttypes>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <type_traits>

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "walb_diff_converter.hpp"

struct Option : public cybozu::Option
{
    uint32_t maxIoSize;
    Option() {
        setUsage("Usage: wlog-to-wdiff < [wlogs] > [wdiff]", true);
        appendOpt(&maxIoSize, uint16_t(-1), "x", "max IO size in the output wdiff [byte].");
        appendHelp("h");
    }
};

int main(int argc, UNUSED char *argv[]) try
{
    walb::util::setLogSetting("-", false);
    Option opt;
    if (!opt.parse(argc, argv)) {
        opt.usage();
        return 1;
    }
    walb::diff::Converter c;
    c.convert(0, 1, opt.maxIoSize);
    return 0;
} catch (std::exception &e) {
    LOGe("exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("caught other error.\n");
    return 1;
}

/* end of file. */
