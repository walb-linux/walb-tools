#include "util.hpp"
#include "walb_diff_file.hpp"
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "murmurhash3.hpp"

using namespace walb;

struct Option
{
    bool isDebug;
    uint32_t seed;
    size_t sleepMs;
    bool isNoDirect;
    uint64_t addr;
    uint32_t blks;
    std::string bdevPath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setDescription("Monitor update of contiguous blocks\n");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendOpt(&seed, 0, "seed", ": murmurhash3 seed.");
        opt.appendOpt(&sleepMs, 0, "sleep", ": interval sleep [ms] (default: 0).");
        opt.appendBoolOpt(&isNoDirect, "nodirect", ": do not use O_DIRECT.");
        opt.appendParam(&addr, "ADDR", ": start address to read [logical block].");
        opt.appendParam(&blks, "SIZE", ": number of blocks to read [logical block].");
        opt.appendParam(&bdevPath, "PATH", ": block device path.");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

inline std::string getNowStr()
{
    struct timespec ts;
    if (::clock_gettime(CLOCK_REALTIME, &ts) < 0) {
        throw cybozu::Exception("getNowStr: clock_gettime failed.");
    }
    return cybozu::getHighResolutionTimeStr(ts);
}

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    const int flags = O_RDONLY | (opt.isNoDirect ? 0 : O_DIRECT);
    cybozu::util::File f(opt.bdevPath, flags);
    AlignedArray buf(opt.blks * LBS);
    cybozu::murmurhash3::Hasher hasher(opt.seed);
    cybozu::murmurhash3::Hash prevHash;
    prevHash.clear();
    const uint64_t off = opt.addr * LBS;
    for (;;) {
        f.pread(buf.data(), buf.size(), off);
        cybozu::murmurhash3::Hash h = hasher(buf.data(), buf.size());
        if (h != prevHash) {
           ::printf("%s\t%s\n", getNowStr().c_str(), h.str().c_str());
        }
        prevHash = h;
        if (opt.sleepMs > 0) util::sleepMs(opt.sleepMs);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("mon-blk")
