/**
 * @file
 * @brief Simple binary diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "walb_util.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    uint32_t blockSize_;
    bool isVerbose_;
    std::string file1_;
    std::string file2_;
public:
    Config(int argc, char* argv[])
        : blockSize_(512)
        , isVerbose_(false)
        , file1_()
        , file2_() {
        cybozu::Option opt;
        opt.setDescription("bdiff: Show block diff.");
        opt.appendOpt(&blockSize_, 512, "b", "SIZE: block size in bytes (default: 512)");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&file1_, "FILE1");
        opt.appendParam(&file2_, "FILE2");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
        check();
    }

    const std::string& filePath1() const { return file1_; }
    const std::string& filePath2() const { return file2_; }
    uint32_t blockSize() const { return blockSize_; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (blockSize_ == 0) {
            throw cybozu::Exception("blockSize_ must be positive integer.");
        }
    }

private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.appendOpt(&blockSize_, 512, "b", "blockSize");
        opt.appendOpt(&isVerbose_, false, "v", "verbose");
        opt.appendHelp("h");
        opt.appendParam(&file1_, "FILE1");
        opt.appendParam(&file2_, "FILE2");
        opt.parse(argc, argv, true);
    }
    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "bdiff: Show block diff.\n"
            "Usage: bdiff [options] FILE1 FILE2\n"
            "Options:\n"
            "  -b, --blockSize \n"
            "  -v, --verbose:         \n"
            "  -h, --help:            \n");
    }
};

/**
 * RETURN:
 *   Number of different blocks.
 */
uint64_t checkBlockDiff(Config& config)
{
    cybozu::util::File fileR1(config.filePath1(), O_RDONLY);
    cybozu::util::File fileR2(config.filePath2(), O_RDONLY);

    const uint32_t bs = config.blockSize();
    std::unique_ptr<char> p1(new char[bs]);
    std::unique_ptr<char> p2(new char[bs]);
#if 0
    ::printf("%d\n%d\n", f1.fd(), f2.fd());
#endif

    uint64_t nDiffer = 0;
    uint64_t nChecked = 0;
    try {
        while (true) {
            fileR1.read(p1.get(), bs);
            fileR2.read(p2.get(), bs);
            if (::memcmp(p1.get(), p2.get(), bs) != 0) {
                nDiffer++;
                if (config.isVerbose()) {
                    ::printf("block %" PRIu64 " differ\n", nChecked);
                    cybozu::util::printByteArray(p1.get(), bs);
                    cybozu::util::printByteArray(p2.get(), bs);
                }
            }
            nChecked++;
        }
    } catch (cybozu::util::EofError& e) {
    }

    fileR1.close();
    fileR2.close();
    ::printf("%" PRIu64 "/%" PRIu64 " differs\n",
             nDiffer, nChecked);

    return nDiffer;
}

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    return checkBlockDiff(config) != 0;
}

DEFINE_ERROR_SAFE_MAIN("bdiff")
