/**
 * @file
 * @brief Read walb log device and archive it.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    std::string ldevPath;
    std::string outPath;
    uint64_t beginLsid;
    uint64_t endLsid;
    bool isVerbose;
    bool isDebug;
    std::vector<std::string> args;

    Option(int argc, char* argv[])
        : ldevPath()
        , outPath("-")
        , beginLsid(0)
        , endLsid(-1)
        , isVerbose(false)
        , isDebug(false)
        , args() {

        cybozu::Option opt;
        opt.setDescription("Wlcat: extract wlog from a log device.");
        opt.appendOpt(&outPath, "-", "o", "PATH: output wlog path. '-' for stdout. (default: '-')");
        opt.appendOpt(&beginLsid, 0, "b", "LSID: begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");
        opt.appendParam(&ldevPath, "LOG_DEVICE_PATH");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (beginLsid >= endLsid) {
            throw RT_ERR("beginLsid must be < endLsid.");
        }
    }
    bool isOutStdout() const { return outPath == "-"; }
};

class WlogExtractor
{
private:
    const uint64_t beginLsid_, endLsid_;
    device::AsyncWldevReader ldevReader_;
    device::SuperBlock &super_;
    const uint32_t pbs_;
    bool isVerbose_;

public:
    WlogExtractor(const std::string &ldevPath,
                  uint64_t beginLsid, uint64_t endLsid,
                  bool isVerbose = false)
        : beginLsid_(beginLsid), endLsid_(endLsid)
        , ldevReader_(ldevPath)
        , super_(ldevReader_.super())
        , pbs_(super_.pbs())
        , isVerbose_(isVerbose) {
    }
    DISABLE_COPY_AND_ASSIGN(WlogExtractor);
    DISABLE_MOVE(WlogExtractor);
    /**
     * Read walb log from the device and write to outFd with wl header.
     */
    void run(int outFd) {
        if (outFd <= 0) throw RT_ERR("outFd is not valid.");
        log::Writer writer(outFd);

        /* Set lsids. */
        uint64_t beginLsid = std::max(beginLsid_, super_.getOldestLsid());

        const uint32_t salt = super_.getLogChecksumSalt();
        const uint32_t pbs = super_.getPhysicalBlockSize();

        /* Create and write walblog header. */
        log::FileHeader wh;
        wh.init(pbs, salt, super_.getUuid(), beginLsid, endLsid_);
        writer.writeHeader(wh);

        /* Read and write each logpack. */
        if (isVerbose_) {
            ::fprintf(::stderr, "beginLsid: %" PRIu64 "\n", beginLsid);
        }
        ldevReader_.reset(beginLsid);
        uint64_t lsid = beginLsid;
        uint64_t totalPaddingPb = 0;
        uint64_t nPacks = 0;
        LogPackHeader packH(pbs, salt);
        bool isNotShrinked = true;
        while (lsid < endLsid_ && isNotShrinked) {
            if (!readLogPackHeader(ldevReader_, packH, lsid)) break;
            std::queue<LogBlockShared> ioQ;
            isNotShrinked = readAllLogIos(ldevReader_, packH, ioQ);
            if (!isNotShrinked && packH.nRecords() == 0) break;
            writer.writePack(packH, std::move(ioQ));
            assert(ioQ.empty());

            totalPaddingPb += packH.totalPaddingPb();
            nPacks++;
            lsid = packH.nextLogpackLsid();
        }
        writer.close();

        if (isVerbose_) {
            ::fprintf(::stderr, "endLsid: %" PRIu64 "\n"
                      "lackOfLogPb: %" PRIu64 "\n"
                      "totalPaddingPb: %" PRIu64 "\n"
                      "nPacks: %" PRIu64 "\n",
                      lsid, endLsid_ - lsid, totalPaddingPb, nPacks);
        }
    }
};

void setupOutputFile(cybozu::util::File &fileW, const Option &opt)
{
    if (opt.isOutStdout()) {
        fileW.setFd(1);
    } else {
        fileW.open(opt.outPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    walb::util::setLogSetting("-", opt.isDebug);
    WlogExtractor extractor(opt.ldevPath, opt.beginLsid, opt.endLsid, opt.isVerbose);
    cybozu::util::File fileW;
    setupOutputFile(fileW, opt);
    extractor.run(fileW.fd());
    fileW.close();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-cat")
