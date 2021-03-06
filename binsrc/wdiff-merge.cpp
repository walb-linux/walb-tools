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
#include "host_info.hpp"

using namespace walb;

struct Option : public cybozu::Option
{
    uint32_t maxIoSize;
    std::vector<std::string> inputWdiffs;
    std::string outputWdiff, cmprStr;
    bool doStat;
    CompressOpt cmpr;

    Option() {
        setDescription("Merge wdiff files.");
        appendOpt(&maxIoSize, 0, "x", "SIZE: max IO size [byte]. 0 means no limitation.");
        appendVec(&inputWdiffs, "i", "WDIFF_PATH_LIST: input wdiff paths.");
        appendOpt(&outputWdiff, "-", "o", "WDIFF_PATH: output wdiff path (default: stdout).");
        appendBoolOpt(&doStat, "stat", ": put statistics.");
        appendOpt(&cmprStr, "snappy:0:1", "cmpr", "type:level:concurrency : compression for output (default: snappy:0:1)");
        appendHelp("h", ": put this message.");
    }
    uint32_t maxIoBlocks() const {
        if (0 < maxIoSize && maxIoSize < LOGICAL_BLOCK_SIZE) {
            throw RT_ERR("Too small max IO size specified.");
        }
        return maxIoSize / LOGICAL_BLOCK_SIZE;
    }
    bool parse(int argc, char *argv[]) {
        if (!cybozu::Option::parse(argc, argv)) {
            goto error;
        }
        if (inputWdiffs.empty()) {
            ::fprintf(::stderr, "You must specify one or more input wdiff files.\n");
            goto error;
        }
        cmpr.parse(cmprStr);
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
    DiffMerger merger;
    for (std::string &path : opt.inputWdiffs) {
        merger.addWdiff(path);
    }
    cybozu::util::File file;
    if (opt.outputWdiff == "-") {
        file.setFd(1);
    } else {
        file.open(opt.outputWdiff, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
    merger.setMaxIoBlocks(opt.maxIoBlocks());
    merger.setShouldValidateUuid(false);
#if 0
    merger.mergeToFd(file.fd());
#else
    merger.mergeToFdInParallel(file.fd(), opt.cmpr);
#endif
#if 0
    /*
     * currently we prefer prompt quit of the command
     * rather than persistence of output file.
     */
    file.fdatasync();
#endif
    file.close();
    if (opt.doStat) {
        std::cerr << "mergeIn  " << merger.statIn() << std::endl
                  << "mergeOut " << merger.statOut() << std::endl
                  << "mergeMemUsage " << merger.memUsageStr() << std::endl;
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-merge")
