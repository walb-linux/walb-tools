/**
 * @file
 * @brief walb log generator for test.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "walb_log_gen.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

using namespace walb;

/**
 * Command line configuration.
 */
struct Option
{
    uint32_t pbs; /* physical block size [byte] */
    uint64_t devSize; /* [byte]. */
    uint32_t minIoSize; /* [byte]. */
    uint32_t maxIoSize; /* [byte]. */
    uint32_t maxPackSize; /* [byte]. */
    uint64_t outLogSize; /* Approximately output log size [byte]. */
    uint64_t lsid; /* start lsid [physical block]. */
    bool isNotPadding;
    bool isNotDiscard;
    bool isNotAllZero;
    bool isRandom;
    bool isVerbose;
    std::string outPath;

    bool isOpened;
    cybozu::util::File file;

    Option(int argc, char* argv[])
        : pbs(LOGICAL_BLOCK_SIZE)
        , devSize(16 * 1024 * 1024) /* default 16MB. */
        , minIoSize(pbs)
        , maxIoSize(32 * 1024) /* default 32KB. */
        , maxPackSize(16 * 1024 * 1024) /* default 16MB. */
        , outLogSize(1024 * 1024) /* default 1MB. */
        , lsid(0)
        , isNotPadding(false)
        , isNotDiscard(false)
        , isNotAllZero(false)
        , isVerbose(false)
        , outPath()
        , isOpened(false)
        , file() {

        cybozu::Option opt;
        opt.setDescription("Wlog-gen: generate walb log randomly.");
        opt.appendOpt(&devSize, 16 * 1024 * 1024, "s", "SIZE: device size [byte]. (default: 16M)");
        opt.appendOpt(&minIoSize, LOGICAL_BLOCK_SIZE, "-minIoSize", cybozu::format("SIZE: minimum IO size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&maxIoSize, 32 * 1024, "-maxIoSize", "SIZE: maximum IO size [byte]. (default: 32K)");
        opt.appendOpt(&pbs, LOGICAL_BLOCK_SIZE, "b", cybozu::format("SIZE: physical block size [byte]. (default: %u)", LOGICAL_BLOCK_SIZE));
        opt.appendOpt(&maxPackSize, 16 * 1024 * 1024, "-maxPackSize", "SIZE: maximum logpack size [byte]. (default: 16M)");
        opt.appendOpt(&outLogSize, 1024 * 1024, "z", "SIZE: total log size to generate [byte]. (default: 1M)");
        opt.appendOpt(&lsid, 0, "-lsid", "LSID: lsid of the first log. (default: 0)");
        opt.appendBoolOpt(&isNotPadding, "-nopadding", "no padding. (default: randomly inserted)");
        opt.appendBoolOpt(&isNotDiscard, "-nodiscard", "no discard. (default: randomly inserted)");
        opt.appendBoolOpt(&isNotAllZero, "-noallzero", "no all-zero. (default: randomly inserted)");
        opt.appendBoolOpt(&isRandom, "-rand", "use random IO data instead of almost zero");
        opt.appendOpt(&outPath, "-", "o", "PATH: output file path or '-' for stdout.");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        genConfig().check();
    }

    uint64_t devLb() const { return devSize / LOGICAL_BLOCK_SIZE; }
    uint32_t minIoLb() const { return minIoSize / LOGICAL_BLOCK_SIZE; }
    uint32_t maxIoLb() const { return maxIoSize / LOGICAL_BLOCK_SIZE; }
    uint32_t maxPackPb() const { return maxPackSize / pbs; }
    uint64_t outLogPb() const { return outLogSize / pbs; }

    int getOutFd() {
        if (isOpened) return file.fd();
        if (outPath == "-") return 1;
        file.open(outPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        isOpened = true;
        return file.fd();
    }

    WlogGenerator::Config genConfig() const {
        WlogGenerator::Config cfg;
        cfg.devLb = devLb();
        cfg.minIoLb = minIoLb();
        cfg.maxIoLb = maxIoLb();
        cfg.pbs = pbs;
        cfg.maxPackPb = maxPackPb();
        cfg.outLogPb = outLogPb();
        cfg.lsid = lsid;
        cfg.isPadding = !isNotPadding;
        cfg.isDiscard = !isNotDiscard;
        cfg.isAllZero = !isNotAllZero;
        cfg.isRandom = isRandom;
        cfg.isVerbose = isVerbose;
        return cfg;
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    WlogGenerator::Config cfg = opt.genConfig();
    WlogGenerator wlGen(cfg);
    wlGen.generate(opt.getOutFd());
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-gen")
