/**
 * @file
 * @brief Simple binary diff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <string>
#include <vector>
#include <stdexcept>
#include "cybozu/option.hpp"

#include "util.hpp"
#include "fileio.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    unsigned int blockSize_;
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
    unsigned int blockSize() const { return blockSize_; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (blockSize_ == 0) {
            throw RT_ERR("Block size must be positive integer.");
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
    cybozu::util::FileOpener f1(config.filePath1(), O_RDONLY);
    cybozu::util::FileOpener f2(config.filePath2(), O_RDONLY);
    cybozu::util::FdReader fdr1(f1.fd());
    cybozu::util::FdReader fdr2(f2.fd());

    const unsigned int bs = config.blockSize();
    std::unique_ptr<char> p1(new char[bs]);
    std::unique_ptr<char> p2(new char[bs]);
#if 0
    ::printf("%d\n%d\n", f1.fd(), f2.fd());
#endif

    uint64_t nDiffer = 0;
    uint64_t nChecked = 0;
    try {
        while (true) {
            fdr1.read(p1.get(), bs);
            fdr2.read(p2.get(), bs);
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

    f1.close();
    f2.close();
    ::printf("%" PRIu64 "/%" PRIu64 " differs\n",
             nDiffer, nChecked);

    return nDiffer;
}

int main(int argc, char* argv[])
    try
{
    Config config(argc, argv);

    return checkBlockDiff(config) != 0;
} catch (std::exception& e) {
    ::fprintf(::stderr, "Exception: %s\n", e.what());
    return 1;
} catch (...) {
    ::fprintf(::stderr, "Caught other error.\n");
    return 1;
}

