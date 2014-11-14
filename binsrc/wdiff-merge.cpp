/**
 * @file
 * @brief Merge several walb diff files to a wdiff file.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "util.hpp"
#include "walb_diff_merge.hpp"

struct Option : public cybozu::Option
{
    uint32_t maxIoSize;
    std::vector<std::string> inputWdiffs;
    std::string outputWdiff;
    bool doStat;

    Option() {
        setDescription("Merge wdiff files.");
        appendOpt(&maxIoSize, 0, "x", "SIZE: max IO size [byte]. 0 means no limitation.");
        appendVec(&inputWdiffs, "i", "WDIFF_PATH_LIST: input wdiff paths.");
        appendMust(&outputWdiff, "o", "WDIFF_PATH: output wdiff path.");
        appendBoolOpt(&doStat, "stat", ": put statistics.");
        appendHelp("h", ": put this message.");
    }
    uint16_t maxIoBlocks() const {
        if (0 < maxIoSize && maxIoSize < LOGICAL_BLOCK_SIZE) {
            throw RT_ERR("Too small max IO size specified.");
        }
        if (uint16_t(-1) < maxIoSize / LOGICAL_BLOCK_SIZE) {
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

int doMain(int argc, char *argv[])
{
    Option opt;
    if (!opt.parse(argc, argv)) return 1;
    walb::diff::Merger merger;
    for (std::string &path : opt.inputWdiffs) {
        merger.addWdiff(path);
    }
    cybozu::util::File file(opt.outputWdiff, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    merger.setMaxIoBlocks(opt.maxIoBlocks());
    merger.setShouldValidateUuid(false);
    merger.mergeToFd(file.fd());
    file.close();
    if (opt.doStat) {
        merger.statIn().printOneline(::stdout, "input:  ");
        merger.statOut().printOneline(::stdout, "output: ");
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-merge")
