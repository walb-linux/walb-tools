/**
 * Calculate hash value of each blocks.
 */
#include "cybozu/option.hpp"
#include "fileio.hpp"
#include "walb_util.hpp"
#include "murmurhash3.hpp"
#include "bdev_util.hpp"

struct Option
{
    uint32_t bs;
    uint64_t size;
    uint32_t seed;
    bool isEach;
    std::string filePath;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.appendOpt(&bs, 512, "b", ": block size [byte].");
        opt.appendOpt(&size, 0, "s", ": size to calc [byte] (default: whole file size).");
        opt.appendOpt(&seed, 0, "seed", ": hash seed (default 0).");
        opt.appendBoolOpt(&isEach, "each", ": put hash for each block.");
        opt.appendParam(&filePath, "FILE_PATH", ": block device path");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (bs == 0 || bs % 512 != 0) {
            throw cybozu::Exception("bad block size") << bs;
        }
        if (size != 0 && size % bs != 0) {
            throw cybozu::Exception("bad size") << size;
        }
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    cybozu::util::File file(opt.filePath, O_RDONLY);
    const uint32_t bs = opt.bs;
    uint64_t size;
    if (opt.size == 0) {
        size = cybozu::util::getBlockDeviceSize(file.fd());
        size = size / bs * bs;
    } else {
        size = opt.size;
    }

    std::vector<char> buf(bs);
    cybozu::murmurhash3::Hasher hasher(opt.seed);
    uint64_t remaining = size;
    uint64_t blk = 0;
    cybozu::murmurhash3::Hash sum;
    sum.zeroClear();
    while (remaining > 0) {
        file.read(buf.data(), bs);
        cybozu::murmurhash3::Hash h = hasher(buf.data(), bs);
        if (opt.isEach) {
            ::printf("%10" PRIu64 " %s\n", blk, h.str().c_str());
        } else {
            sum.doXor(h);
        }
        blk++;
        remaining -= bs;
    }
    if (!opt.isEach) {
        ::printf("%s\n", sum.str().c_str());
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("bhash");
