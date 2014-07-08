/**
 * @file
 * @brief Verify logs on a walb log device by comparing with an IO recipe.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "io_recipe.hpp"
#include "walb/common.h"
#include "walb/block_size.h"
#include "walb_util.hpp"

/**
 * Command line configuration.
 */
class Config
{
private:
    uint64_t beginLsid_;
    uint64_t endLsid_;
    bool isVerbose_;
    std::string recipePath_; /* recipe path or "-" for stdin. */
    std::string wldevPath_; /* walb log devcie path. */

public:
    Config(int argc, char* argv[])
        : beginLsid_(-1)
        , endLsid_(-1)
        , isVerbose_(false)
        , recipePath_("-")
        , wldevPath_() {
        parse(argc, argv);
    }

    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    bool isVerbose() const { return isVerbose_; }
    const std::string& recipePath() const { return recipePath_; }
    const std::string& wldevPath() const { return wldevPath_; }

private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("verify_wldev: verify logs on a walb log device with an IO recipe.");
        opt.appendOpt(&beginLsid_, -1, "b", "LSID: begin lsid. (default: oldest lsid)");
        opt.appendOpt(&endLsid_, -1, "e", "LSID: end lsid. (default: written lsid)");
        opt.appendOpt(&recipePath_, "-", "r", "PATH: recipe file path. '-' for stdin. (default: '-')");
        opt.appendParam(&wldevPath_, "WALB_LOG_DEVICE");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

class WldevVerifier
{
private:
    using LogPackHeader = walb::LogPackHeader;
    using LogPackIo = walb::LogPackIo;
    using AlignedArray = walb::AlignedArray;

    const Config &config_;
    cybozu::util::File file_;
    const uint32_t pbs_;
    walb::device::SuperBlock super_;
    const uint32_t salt_;

public:
    WldevVerifier(const Config &config)
        : config_(config)
        , file_(config.wldevPath(), O_RDONLY | O_DIRECT)
        , pbs_(cybozu::util::getPhysicalBlockSize(file_.fd()))
        , super_(pbs_)
        , salt_(super_.getLogChecksumSalt()) {
        super_.read(file_.fd());
    }
    void run() {
        /* Get IO recipe parser. */
        cybozu::util::File recipeFile;
        if (config_.recipePath() != "-") {
            recipeFile.open(config_.recipePath(), O_RDONLY);
        } else {
            recipeFile.setFd(0);
        }
        walb::util::IoRecipeParser recipeParser(recipeFile.fd());

        /* Decide lsid range to verify. */
        uint64_t beginLsid = config_.beginLsid();
        if (beginLsid == uint64_t(-1)) { beginLsid = super_.getOldestLsid(); }
        uint64_t endLsid = config_.endLsid();
        if (endLsid == uint64_t(-1)) { endLsid = super_.getWrittenLsid(); }
        if (endLsid <= beginLsid) {
            throw RT_ERR("Invalid lsid range.");
        }

        /* Read walb logs and verify them with IO recipes. */
        uint64_t lsid = beginLsid;
        while (lsid < endLsid) {
            LogPackHeader logh;
            readPackHeader(logh, lsid);
            if (lsid != logh.logpackLsid()) { throw RT_ERR("wrong lsid"); }
            std::queue<LogPackIo> q;
            readPackIo(logh, q);

            while (!q.empty()) {
                LogPackIo packIo = std::move(q.front());
                q.pop();
                const walb::LogRecord &rec = packIo.rec;
                if (recipeParser.isEnd()) {
                    throw RT_ERR("Recipe not found.");
                }
                walb::util::IoRecipe recipe = recipeParser.get();
                if (recipe.offsetB() != rec.offset) {
                    RT_ERR("offset mismatch.");
                }
                if (recipe.ioSizeB() != rec.ioSizeLb()) {
                    RT_ERR("io_size mismatch.");
                }
                /* Validate the log and confirm checksum equality. */
                const uint32_t csum0 = packIo.calcIoChecksumWithZeroSalt();
                const uint32_t csum1 = rec.checksum;
                const uint32_t csum2 = packIo.calcIoChecksum();
                const bool isValid = packIo.isValid(false) &&
                    recipe.csum() == csum0 && csum1 == csum2;

                /* Print result. */
                ::printf("%s\t%s\t%08x\t%08x\t%08x\n", isValid ? "OK" : "NG",
                         recipe.toString().c_str(), csum0, csum1, csum2);
            }
            lsid = logh.nextLogpackLsid();
        }

        if (!recipeParser.isEnd()) {
            throw RT_ERR("There are still remaining recipes.");
        }
    }

private:
    AlignedArray readBlock(uint64_t lsid) {
        AlignedArray b(pbs_);
        const uint64_t offset = super_.getOffsetFromLsid(lsid);
        file_.pread(b.data(), pbs_, offset * pbs_);
        return b;
    }

    void readPackHeader(LogPackHeader& logh, uint64_t lsid) {
        logh.setBlock(readBlock(lsid));
        logh.setPbs(pbs_);
        logh.setSalt(salt_);
    }

    void readPackIo(LogPackHeader &logh, std::queue<LogPackIo> &queue) {
        for (size_t i = 0; i < logh.nRecords(); i++) {
            LogPackIo packIo;
            packIo.set(logh, i);
            const walb::LogRecord &rec = packIo.rec;
            if (!rec.hasData()) continue;
            const uint64_t endLsid = rec.lsid + rec.ioSizePb(pbs_);
            for (uint64_t lsid = rec.lsid; lsid < endLsid; lsid++) {
                packIo.blockS.addBlock(readBlock(lsid));
            }
            if (!rec.hasDataForChecksum()) continue;
            /* Only normal IOs will be inserted. */
            queue.push(std::move(packIo));
        }
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    WldevVerifier v(config);
    v.run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("verify_wldev")
