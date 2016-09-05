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
#include "linux/walb/walb.h"
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
    uint64_t minDiscardSize; /* [bytes]. */
    uint64_t maxDiscardSize; /* [bytes]. */
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
        : isOpened(false)
        , file() {

        cybozu::Option opt;
        opt.setDescription("Wlog-gen: generate walb log randomly.");
        opt.appendOpt(&devSize, 16 * MEBI, "s", "SIZE: device size [byte]. (default: 16M)");
        opt.appendOpt(&minIoSize, LBS, "-minIoSize", cybozu::format("SIZE: minimum IO size [byte]. (default: %lu)", LBS));
        opt.appendOpt(&maxIoSize, 32 * KIBI, "-maxIoSize", "SIZE: maximum IO size [byte]. (default: 32K)");
        opt.appendOpt(&minDiscardSize, LBS
                      , "-minDiscardSize", cybozu::format("SIZE: minimum discard size [byte]. (default: %lu)", LBS));
        opt.appendOpt(&maxDiscardSize, 64 * MEBI
                      , "-maxDiscardSize", "SIZE: maximum discard size [byte]. (default: 64M)");
        opt.appendOpt(&pbs, LBS, "b", cybozu::format("SIZE: physical block size [byte]. (default: %lu)", LBS));
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

    uint64_t devLb() const { return devSize / LBS; }
    uint32_t minIoLb() const { return minIoSize / LBS; }
    uint32_t maxIoLb() const { return maxIoSize / LBS; }
    uint32_t minDiscardLb() const { return minDiscardSize / LBS; }
    uint32_t maxDiscardLb() const { return maxDiscardSize / LBS; }

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
        cfg.minDiscardLb = minDiscardLb();
        cfg.maxDiscardLb = maxDiscardLb();
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
