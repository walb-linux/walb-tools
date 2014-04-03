/**
 * @file
 * @brief Verify logs on a walb log device by comparing with an IO recipe.
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
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "memory_buffer.hpp"
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
    using PackIoRaw = walb::log::PackIoRaw<walb::log::BlockDataShared>;

    const Config &config_;
    cybozu::util::BlockDevice wlDev_;
    walb::device::SuperBlock super_;
    const unsigned int pbs_;
    const uint32_t salt_;
    const unsigned int BUFFER_SIZE_;
    cybozu::util::BlockAllocator<uint8_t> ba_;

public:
    WldevVerifier(const Config &config)
        : config_(config)
        , wlDev_(config.wldevPath(), O_RDONLY | O_DIRECT)
        , super_(wlDev_)
        , pbs_(super_.getPhysicalBlockSize())
        , salt_(super_.getLogChecksumSalt())
        , BUFFER_SIZE_(16 << 20) /* 16MB */
        , ba_(BUFFER_SIZE_ / pbs_, pbs_, pbs_) {}

    void run() {
        /* Get IO recipe parser. */
        std::shared_ptr<cybozu::util::FileOpener> rFop;
        if (config_.recipePath() != "-") {
            rFop.reset(new cybozu::util::FileOpener(config_.recipePath(), O_RDONLY));
        }
        int rFd = 0;
        if (rFop) { rFd = rFop->fd(); }
        walb::util::IoRecipeParser recipeParser(rFd);

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
            std::queue<PackIoRaw> q;
            readPackIo(logh, q);

            while (!q.empty()) {
                PackIoRaw packIo = std::move(q.front());
                q.pop();
                if (recipeParser.isEnd()) {
                    throw RT_ERR("Recipe not found.");
                }
                walb::util::IoRecipe recipe = recipeParser.get();
                if (recipe.offsetB() != packIo.record().offset()) {
                    RT_ERR("offset mismatch.");
                }
                if (recipe.ioSizeB() != packIo.record().ioSizeLb()) {
                    RT_ERR("io_size mismatch.");
                }
                /* Validate the log and confirm checksum equality. */
                const uint32_t csum0 = packIo.calcIoChecksum(0);
                const uint32_t csum1 = packIo.record().checksum();
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
    using Block = std::shared_ptr<uint8_t>;

    Block readBlock(uint64_t lsid) {
        Block b = ba_.alloc();
        uint64_t offset = super_.getOffsetFromLsid(lsid);
        wlDev_.read(offset * pbs_, pbs_, reinterpret_cast<char *>(b.get()));
        return b;
    }

    void readPackHeader(LogPackHeader& logh, uint64_t lsid) {
        logh.setBlock(readBlock(lsid));
        logh.setPbs(pbs_);
        logh.setSalt(salt_);
    }

    void readPackIo(LogPackHeader &logh, std::queue<PackIoRaw> &queue) {
        for (size_t i = 0; i < logh.nRecords(); i++) {
            PackIoRaw packIo(logh, i);
            const walb::log::Record &rec = packIo.record();
            if (!rec.hasData()) { continue; }
            const uint64_t endLsid = rec.lsid() + rec.ioSizePb();
            for (uint64_t lsid = rec.lsid(); lsid < endLsid; lsid++) {
                packIo.blockData().addBlock(readBlock(lsid));
            }
            if (!rec.hasDataForChecksum()) continue;
            /* Only normal IOs will be inserted. */
            queue.push(std::move(packIo));
        }
    }
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);
    Config config(argc, argv);
    WldevVerifier v(config);
    v.run();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end file. */
