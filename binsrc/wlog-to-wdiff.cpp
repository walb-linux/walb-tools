/**
 * @file
 * @brief Convert walb logs to a walb diff.
 */
#include "cybozu/option.hpp"
#include "walb_diff_converter.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    uint32_t maxIoSize;
    bool isDebug;
    std::string input, output;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setUsage("Usage: wlog-to-wdiff < [wlog] > [wdiff]", true);
        opt.appendOpt(&input, "-", "i", ": input wlog file (default: stdin)");
        opt.appendOpt(&output, "-", "o", ": output wdiff file (default: stdout)");
        opt.appendOpt(&maxIoSize, DEFAULT_MAX_WDIFF_IO_BLOCKS * LBS
                      , "x", ": max IO size in the output wdiff [byte].");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendHelp("h");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

void setupFile(cybozu::util::File &file, const std::string &path, bool isRead)
{
    if (path == "-") {
        file.setFd(isRead ? 0 : 1);
    } else if (isRead) {
        file.open(path, O_RDONLY);
    } else {
        file.open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
}

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    DiffConverter c;
    cybozu::util::File inputFile, outputFile;
    setupFile(inputFile, opt.input, true);
    setupFile(outputFile, opt.output, false);
    c.convert(inputFile.fd(), outputFile.fd(), opt.maxIoSize / LBS);
    outputFile.fdatasync();
    outputFile.close();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-to-wdiff")
