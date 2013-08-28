/**
 * @file
 * @brief walb log generator for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <cstdint>
#include <queue>
#include <memory>
#include <deque>
#include <algorithm>
#include <utility>
#include <set>
#include <limits>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <getopt.h>

#include "stdout_logger.hpp"

#include "util.hpp"
#include "memory_buffer.hpp"
#include "walb_log.hpp"
#include "walb_log_gen.hpp"

#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    unsigned int pbs_; /* physical block size [byte] */
    uint64_t devSize_; /* [byte]. */
    unsigned int minIoSize_; /* [byte]. */
    unsigned int maxIoSize_; /* [byte]. */
    unsigned int maxPackSize_; /* [byte]. */
    uint64_t outLogSize_; /* Approximately output log size [byte]. */
    uint64_t lsid_; /* start lsid [physical block]. */
    bool isPadding_;
    bool isDiscard_;
    bool isAllZero_;
    bool isVerbose_;
    bool isHelp_;
    std::string outPath_;
    std::vector<std::string> args_;

    std::shared_ptr<cybozu::util::FileOpener> foP_;

public:
    Config(int argc, char* argv[])
        : pbs_(LOGICAL_BLOCK_SIZE)
        , devSize_(16 * 1024 * 1024) /* default 16MB. */
        , minIoSize_(pbs_)
        , maxIoSize_(32 * 1024) /* default 32KB. */
        , maxPackSize_(16 * 1024 * 1024) /* default 16MB. */
        , outLogSize_(1024 * 1024) /* default 1MB. */
        , lsid_(0)
        , isPadding_(true)
        , isDiscard_(true)
        , isAllZero_(true)
        , isVerbose_(false)
        , isHelp_(false)
        , outPath_()
        , args_()
        , foP_() {
        parse(argc, argv);
    }

    uint64_t devLb() const { return devSize_ / LOGICAL_BLOCK_SIZE; }
    unsigned int minIoLb() const { return minIoSize_ / LOGICAL_BLOCK_SIZE; }
    unsigned int maxIoLb() const { return maxIoSize_ / LOGICAL_BLOCK_SIZE; }
    unsigned int pbs() const { return pbs_; }
    unsigned int maxPackPb() const { return maxPackSize_ / pbs(); }
    uint64_t outLogPb() const { return outLogSize_ / pbs(); }
    uint64_t lsid() const { return lsid_; }
    bool isPadding() const { return isPadding_; }
    bool isDiscard() const { return isDiscard_; }
    bool isAllZero() const { return isAllZero_; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }
    const std::string& outPath() const { return outPath_; }

    int getOutFd() {
        if (foP_) return foP_->fd();
        if (outPath() == "-") return 1;
        foP_ = std::make_shared<cybozu::util::FileOpener>(
            outPath(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        return foP_->fd();
    }

    walb::log::Generator::Config genConfig() const {
        walb::log::Generator::Config cfg;
        cfg.devLb = devLb();
        cfg.minIoLb = minIoLb();
        cfg.maxIoLb = maxIoLb();
        cfg.pbs = pbs();
        cfg.maxPackPb = maxPackPb();
        cfg.outLogPb = outLogPb();
        cfg.lsid = lsid();
        cfg.isPadding = isPadding();
        cfg.isDiscard = isDiscard();
        cfg.isAllZero = isAllZero();
        cfg.isVerbose = isVerbose();
        return std::move(cfg);
    }

    void print() const {
        ::printf("devLb: %" PRIu64 "\n"
                 "minIoLb: %u\n"
                 "maxIoLb: %u\n"
                 "pbs: %u\n"
                 "maxPackPb: %u\n"
                 "outLogPb: %" PRIu64 "\n"
                 "lsid: %" PRIu64 "\n"
                 "outPath: %s\n"
                 "isPadding: %d\n"
                 "isDiscard: %d\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 devLb(), minIoLb(), maxIoLb(),
                 pbs(), maxPackPb(), outLogPb(),
                 lsid(), outPath().c_str(),
                 isPadding(), isDiscard(), isVerbose(), isHelp());
        int i = 0;
        for (const auto& s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        genConfig().check();
        if (outPath().size() == 0) {
            throw RT_ERR("specify outPath.");
        }
    }

private:
    /* Option ids. */
    enum Opt {
        DEVSIZE = 1,
        MINIOSIZE,
        MAXIOSIZE,
        PBS,
        MAXPACKSIZE,
        OUTLOGSIZE,
        LSID,
        NOPADDING,
        NODISCARD,
        NOALLZERO,
        OUTPATH,
        VERBOSE,
        HELP,
    };

    template <typename IntType>
    IntType str2int(const char *str) const {
        return static_cast<IntType>(cybozu::util::fromUnitIntString(str));
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"devSize", 1, 0, Opt::DEVSIZE},
                {"minIoSize", 1, 0, Opt::MINIOSIZE},
                {"maxIoSize", 1, 0, Opt::MAXIOSIZE},
                {"pbs", 1, 0, Opt::PBS},
                {"maxPackSize", 1, 0, Opt::MAXPACKSIZE},
                {"outLogSize", 1, 0, Opt::OUTLOGSIZE},
                {"lsid", 1, 0, Opt::LSID},
                {"nopadding", 0, 0, Opt::NOPADDING},
                {"nodiscard", 0, 0, Opt::NODISCARD},
                {"noallzero", 0, 0, Opt::NOALLZERO},
                {"outPath", 1, 0, Opt::OUTPATH},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "s:b:o:z:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::DEVSIZE:
            case 's':
                devSize_ = cybozu::util::fromUnitIntString(optarg);
                break;
            case Opt::MINIOSIZE:
                minIoSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::MAXIOSIZE:
                maxIoSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::PBS:
            case 'b':
                pbs_ = str2int<unsigned int>(optarg);
                break;
            case Opt::MAXPACKSIZE:
                maxPackSize_ = str2int<unsigned int>(optarg);
                break;
            case Opt::OUTLOGSIZE:
            case 'z':
                outLogSize_ = str2int<uint64_t>(optarg);
                break;
            case Opt::LSID:
                lsid_ = str2int<uint64_t>(optarg);
                break;
            case Opt::NOPADDING:
                isPadding_ = false;
                break;
            case Opt::NODISCARD:
                isDiscard_ = false;
                break;
            case Opt::NOALLZERO:
                isAllZero_ = false;
                break;
            case Opt::OUTPATH:
            case 'o':
                outPath_ = std::string(optarg);
                break;
            case Opt::VERBOSE:
            case 'v':
                isVerbose_ = true;
                break;
            case Opt::HELP:
            case 'h':
                isHelp_ = true;
                break;
            default:
                throw RT_ERR("Unknown option.");
            }
        }

        while(optind < argc) {
            args_.push_back(std::string(argv[optind++]));
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlog-gen: generate walb log randomly.\n"
            "Usage: wlog-gen [options]\n"
            "Options:\n"
            "  -o, --outPath PATH:    output file path or '-' for stdout.\n"
            "  -b, --pbs SIZE:        physical block size [byte]. (default: %u)\n"
            "  -s, --devSize SIZE:    device size [byte]. (default: 16M)\n"
            "  -z, --outLogSize SIZE: total log size to generate [byte]. (default: 1M)\n"
            "  --minIoSize SIZE:      minimum IO size [byte]. (default: pbs)\n"
            "  --maxIoSize SIZE:      maximum IO size [byte]. (default: 32K)\n"
            "  --maxPackSize SIZE:    maximum logpack size [byte]. (default: 16M)\n"
            "  --lsid LSID:           lsid of the first log. (default: 0)\n"
            "  --nopadding:           no padding. (default: randomly inserted)\n"
            "  --nodiscard:           no discard. (default: randomly inserted)\n"
            "  --noallzero:           no all-zero. (default: randomly inserted)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n",
            LOGICAL_BLOCK_SIZE);
    }
};

int main(int argc, char* argv[])
{
    try {
        Config config(argc, argv);
        /* config.print(); */
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();
        walb::log::Generator wlGen(config.genConfig());
        wlGen.generate(config.getOutFd());
        return 0;
    } catch (std::runtime_error& e) {
        LOGe("Error: %s\n", e.what());
        return 1;
    } catch (std::exception& e) {
        LOGe("Exception: %s\n", e.what());
        return 1;
    } catch (...) {
        LOGe("Caught other error.\n");
        return 1;
    }
}

/* end of file. */
