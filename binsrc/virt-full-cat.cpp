/**
 * @file
 * @brief Virtual full image scanner.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_diff_virt.hpp"
#include "fileio.hpp"

using namespace walb;

struct Option : public cybozu::Option
{
    std::string inputPath;
    std::string outputPath;
    std::vector<std::string> inputWdiffs;
    uint32_t bufferSize;
    bool doStat;
    Option() {
        setUsage("virt-full-cat:\n"
                 "  Full scan of virtul full image that consists\n"
                 "  a base full image and additional wdiff files.\n"
                 "Usage: virt-full-cat (options) -i [input image] -d [input wdiffs] -o [output image]\n"
                 "Options:\n"
                 "  -i arg:  Input full image path. This must be seekable.\n"
                 "  -o arg:  Output full image path. '-' means stdout. (default '-')\n"
                 "  -w args: Input wdiff paths\n"
                 "  -b arg:  Buffer size [byte]. default: '64K'\n"
                 "  -stat:   Put merging statistics.\n"
                 "  -h:      Show this help message.\n");
        appendMust(&inputPath, "i", "Input full image path. This must be seekable.");
        appendOpt(&outputPath, "-", "o", "Output full image path. '-' means stdout. (default '-')");
        appendVec(&inputWdiffs, "d", "Input wdiff paths");
        appendOpt(&bufferSize, 2 << 16, "b", "Buffer size [byte].");
        appendBoolOpt(&doStat, "stat");
        appendHelp("h");
    }
    bool parse(int argc, char *argv[]) {
        if (!cybozu::Option::parse(argc, argv)) {
            goto error;
        }
        return true;

        /* check options. */
      error:
        usage();
        return false;
    }
};


void setupFiles(FileReader &inFile, cybozu::util::File &outFile, Option &opt)
{
    inFile.open(opt.inputPath);
    inFile.readAhead(SIZE_MAX);
    if (opt.outputPath != "-") {
        outFile.open(opt.outputPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    } else {
        outFile.setFd(1);
    }
}


int doMain(int argc, char *argv[])
{
    Option opt;
    if (!opt.parse(argc, argv)) return 1;
    FileReader inFile;
    cybozu::util::File outFile;
    setupFiles(inFile, outFile, opt);
    VirtualFullScanner virt;
    virt.init(std::move(inFile), opt.inputWdiffs);
    virt.readAndWriteTo(outFile.fd(), opt.bufferSize);
    if (opt.doStat) {
        std::cerr << "mergeIn  " << virt.statIn()  << std::endl
                  << "mergeOut " << virt.statOut() << std::endl
                  << "mergeMemUsage " << virt.memUsageStr() << std::endl;
    }
    outFile.close();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("virt-full-cat")
