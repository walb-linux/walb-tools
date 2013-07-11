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
        setDescription("\nvirt-full-cat:\n"
                       "  Full scan of virtul full image that consists\n"
                       "  a base full image and additional wdiff files.\n");
        setUsage("Usage: virt-full-cat (options) -i [input image] -d [input wdiffs] -o [output image]");
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
#if 0
        for (int i = 0; i < argc; i++) {
            ::printf("%s\n", argv[i]);
        }
#endif
        Option opt;
        if (!opt.parse(argc, argv)) return 1;

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
        walb::diff::VirtualFullScanner scanner(inFd, opt.inputWdiffs);
        scanner.readAndWriteTo(outFd, opt.bufferSize);
        if (outFo) outFo->close();

#if 0
        opt.put();

        for (std::string &s : opt.inputWdiffs) {
            ::printf("%s\n", s.c_str());
        }
        ::printf("%u %s\n%s\n"
                 , opt.bufferSize
                 , opt.inputPath.c_str()
                 , opt.outputPath.c_str());
#endif

        return 0;
    } catch (std::runtime_error &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (std::exception &e) {
        ::fprintf(::stderr, "%s\n", e.what());
        return 1;
    } catch (...) {
        ::fprintf(::stderr, "caught other error.\n");
        return 1;
    }
    return 0;
}

/* end of file. */
