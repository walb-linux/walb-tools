/**
 * @file
 * @brief verify data written by write_random_data.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "checksum.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "io_recipe.hpp"
#include "walb/common.h"
#include "walb/block_size.h"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    uint32_t bs; /* block size [byte] */
    std::string recipePath; /* recipe file path. */
    std::string targetPath; /* device or file path. */
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("verify_written_data: verify data written by write_random_data.");
        opt.appendOpt(&bs, LOGICAL_BLOCK_SIZE, "b",
                      cybozu::format("SIZE: block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&recipePath, "-", "r", "PATH: recipe file path. '-' for stdin. (default: '-')");
        opt.appendParam(&targetPath, "DEVICE|FILE");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (bs == 0) {
            throw cybozu::Exception(__func__) << "block size must be not zero.";
        }
    }
    bool isDirect() const {
        return bs % LOGICAL_BLOCK_SIZE == 0;
    }
};

void verifyData(const Option &opt)
{
    const size_t bufSize = 4 * MEBI;

    cybozu::util::File dataFile(opt.targetPath, O_RDONLY | (opt.isDirect() ? O_DIRECT : 0));
    AlignedArray buf(bufSize, false);

    cybozu::util::File recipeFile;
    if (opt.recipePath != "-") {
        recipeFile.open(opt.recipePath, O_RDONLY);
    } else {
        recipeFile.setFd(0);
    }
    util::IoRecipeParser recipeParser(recipeFile.fd());

    /* Read and verify for each IO recipe. */
    while (!recipeParser.isEnd()) {
        const util::IoRecipe r = recipeParser.get();
        buf.resize(r.size * opt.bs, false);
        dataFile.pread(buf.data(), buf.size(), r.offset * opt.bs);
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), 0);
        ::printf("%s %s %08x\n",
                 (csum == r.csum ? "OK" : "NG"), r.str().c_str(), csum);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    verifyData(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("verify_written_data")
