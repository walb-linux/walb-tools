/**
 * @file
 * @brief Virtual full image scanner.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <vector>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include "cybozu/option.hpp"
#include "walb_diff_virt.hpp"
#include "fileio.hpp"

struct Option : public cybozu::Option
{
    std::string inputPath;
    std::string outputPath;
    std::vector<std::string> inputWdiffs;
    uint32_t bufferSize;
    Option() {
        setUsage("virt-full-cat:\n"
                 "  Full scan of virtul full image that consists\n"
                 "  a base full image and additional wdiff files.\n"
                 "Usage: virt-full-cat (options) -i [input image] -d [input wdiffs] -o [output image]\n"
                 "Options:\n"
                 "  -i arg:  Input full image path. '-' means stdin. (default '-')\n"
                 "  -o arg:  Output full image path. '-' means stdout. (default '-')\n"
                 "  -w args: Input wdiff paths\n"
                 "  -b arg:  Buffer size [byte]. default: '64K'\n"
                 "  -h:      Show this help message.\n");
        appendOpt(&inputPath, "-", "i", "Input full image path. '-' means stdin. (default '-')");
        appendOpt(&outputPath, "-", "o", "Output full image path. '-' means stdout. (default '-')");
        appendVec(&inputWdiffs, "d", "Input wdiff paths");
        appendOpt(&bufferSize, 2 << 16, "b", "Buffer size [byte].");
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

int main(int argc, char *argv[])
{
    try {
        Option opt;
        if (!opt.parse(argc, argv)) return 1;
        walb::util::setLogSetting("-", false);

        /* File descriptors for input/output. */
        int inFd = 0, outFd = 1;
        std::shared_ptr<cybozu::util::FileOpener> inFo, outFo;
        if (opt.inputPath != "-") {
            inFo = std::make_shared<cybozu::util::FileOpener>(
                opt.inputPath, O_RDONLY);
            inFd = inFo->fd();
        }
        if (opt.outputPath != "-") {
            outFo = std::make_shared<cybozu::util::FileOpener>(
                opt.outputPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            outFd = outFo->fd();
        }
        walb::diff::VirtualFullScanner virt;
        virt.init(inFd, opt.inputWdiffs);
        virt.readAndWriteTo(outFd, opt.bufferSize);
        if (outFo) outFo->close();
        return 0;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "exception: %s\n", e.what());
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
    }
    return 1;
}

/* end of file. */
