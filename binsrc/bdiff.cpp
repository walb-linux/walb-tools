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

using namespace walb;

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
    size_t lineSize_;
public:
    bool doShowDiff;
    Config(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("bdiff: Show block diff.");
        opt.appendOpt(&blockSize_, 512, "b", "SIZE: block size in bytes (default: 512)");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendOpt(&lineSize_, 32, "l", ": line size in printing different block contents.");
        opt.appendBoolOpt(&doShowDiff, "d", ": show diff for different contents.");
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
    size_t lineSize() const { return lineSize_; }

    void check() const {
        if (blockSize_ == 0) {
            throw cybozu::Exception("blockSize_ must be positive integer.");
        }
    }
};

void showDifference(const void *data0, const void *data1, size_t s, size_t lineSize)
{
    const char *p0 = (const char *)data0;
    const char *p1 = (const char *)data1;

    for (size_t i = 0; i < s; i++) {
        ::printf("%s", (*p0 == *p1 ? "_" : "X"));
        p0++;
        p1++;
        if (i % lineSize == lineSize - 1) {
            ::printf("\n");
        }
    }
    if (s % lineSize != 0) {
        ::printf("\n");
    }
}

/**
 * RETURN:
 *   Number of different blocks.
 */
uint64_t checkBlockDiff(Config& config)
{
    cybozu::util::File fileR1(config.filePath1(), O_RDONLY);
    cybozu::util::File fileR2(config.filePath2(), O_RDONLY);

    const uint32_t bs = config.blockSize();
    AlignedArray a1(bs), a2(bs);
#if 0
    ::printf("%d\n%d\n", f1.fd(), f2.fd());
#endif

    uint64_t nDiffer = 0;
    uint64_t nChecked = 0;
    try {
        while (true) {
            fileR1.read(a1.data(), bs);
            fileR2.read(a2.data(), bs);
            if (::memcmp(a1.data(), a2.data(), bs) != 0) {
                nDiffer++;
                if (config.isVerbose()) {
                    ::printf("block %" PRIu64 " differ\n", nChecked);
                    const size_t lineS = config.lineSize();
                    cybozu::util::printByteArray(a1.data(), bs, lineS);
                    cybozu::util::printByteArray(a2.data(), bs, lineS);
                    if (config.doShowDiff) {
                        showDifference(a1.data(), a2.data(), bs, lineS);
                    }
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
