/**
 * @file
 * @brief Merge several walb diff files to a wdiff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <deque>
#include <queue>
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

#include "util.hpp"
#include "walb_diff_merge.hpp"

#include <sys/types.h>

struct Option : public cybozu::Option
{
    uint32_t maxIoSize;
    std::vector<std::string> inputWdiffs;
    std::string outputWdiff;
    Option() {
        setUsage("Usage: wdiff-merge (options) -i [input wdiffs] -o [merged wdiff]");
        appendOpt(&maxIoSize, 0, "x", "max IO size [byte]. 0 means no limitation.");
        appendVec(&inputWdiffs, "i", "Input wdiff paths.");
        appendMust(&outputWdiff, "o", "Output wdiff path.");
        appendHelp("h");
    }

    uint16_t maxIoBlocks() const {
        if (uint16_t(-1) < maxIoSize) {
            throw RT_ERR("Max IO size must be less than 32M.");
        }
        return maxIoSize / LOGICAL_BLOCK_SIZE;
    }

    bool parse(int argc, char *argv[]) {
        if (!cybozu::Option::parse(argc, argv)) {
            goto error;
        }
        if (inputWdiffs.empty()) {
            ::printf("You must specify one or more input wdiff files.\n");
            goto error;
        }
        return true;
      error:
        usage();
        return false;
    }
};

int main(int argc, char *argv[])
{
    try {
        Option opt;
        if (!opt.parse(argc, argv)) {
            return 1;
        }
        walb::util::setLogSetting("-", false);
        walb::diff::Merger merger;
        for (std::string &path : opt.inputWdiffs) {
            merger.addWdiff(path);
        }
        cybozu::util::File file(opt.outputWdiff, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        merger.setMaxIoBlocks(opt.maxIoBlocks());
        merger.setShouldValidateUuid(false);
        merger.mergeToFd(file.fd());
        file.close();
        return 0;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "exception: %s\n", e.what());
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
    }
    return 1;
}

/* end of file. */
