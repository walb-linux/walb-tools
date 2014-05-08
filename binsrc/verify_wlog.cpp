/**
 * @file
 * @brief Verify a walb log by comparing with an IO recipe.
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
#include "fileio.hpp"
#include "walb_log_file.hpp"
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
    bool isVerbose_;
    std::string wlogPath_; /* walb log path or "-" for stdin. */
    std::string recipePath_; /* recipe path or "-" for stdin. */

public:
    Config(int argc, char* argv[])
        : isVerbose_(false)
        , wlogPath_("-")
        , recipePath_("-") {
        parse(argc, argv);
    }

    bool isVerbose() const { return isVerbose_; }
    const std::string& recipePath() const { return recipePath_; }
    const std::string& wlogPath() const { return wlogPath_; }

    void check() const {
        if (recipePath_ == "-" && wlogPath_ == "-") {
            throw RT_ERR("Specify --recipe or --wlog.");
        }
    }

private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("verify_wlog: verify a walb log with an IO recipe.");
        opt.appendOpt(&recipePath_, "-", "r", "PATH: recipe file path. '-' for stdin. (default: '-')");
        opt.appendOpt(&wlogPath_, "-", "w", "PATH: wlog file path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
        check();
    }
};

class WlogVerifier
{
private:
    using LogPackHeader = walb::LogPackHeader;
    using LogPackIo = walb::LogPackIo;

    const Config &config_;
    uint32_t pbs_;

public:
    WlogVerifier(const Config &config)
        : config_(config), pbs_(0) {}

    void run() {
        /* Get IO recipe parser. */
        cybozu::util::File recipeFile;
        if (config_.recipePath() != "-") {
            recipeFile.open(config_.recipePath(), O_RDONLY);
        } else {
            recipeFile.setFd(0);
        }
        walb::util::IoRecipeParser recipeParser(recipeFile.fd());

        /* Get wlog file descriptor. */
        cybozu::util::File wlFileR;
        if (config_.wlogPath() != "-") {
            wlFileR.open(config_.wlogPath(), O_RDONLY);
        } else {
            wlFileR.setFd(0);
        }

        /* Read wlog header. */
        walb::log::FileHeader wh;
        wh.readFrom(wlFileR);
        if (!wh.isValid(true)) {
            throw RT_ERR("invalid wlog header.");
        }

        pbs_ = wh.pbs();
        const uint32_t salt = wh.salt();

        uint64_t beginLsid = wh.beginLsid();
        uint64_t endLsid = wh.endLsid();
        uint64_t lsid = beginLsid;

        /* Read walb logs and verify them with IO recipes. */
        while (lsid < endLsid) {
            LogPackHeader logh;
            readPackHeader(logh, wlFileR, salt);
            if (lsid != logh.logpackLsid()) { throw RT_ERR("wrong lsid"); }
            std::queue<LogPackIo> q;
            readPackIo(logh, wlFileR, q);

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
    using AlignedArray = walb::AlignedArray;

    AlignedArray readBlock(
        cybozu::util::File &fileR) {
        AlignedArray b(pbs_);
        fileR.read(b.data(), b.size());
        return b;
    }

    void readPackHeader(LogPackHeader& logh, cybozu::util::File &fileR, uint32_t salt) {
        logh.setBlock(readBlock(fileR));
        logh.setPbs(pbs_);
        logh.setSalt(salt);
    }

    void readPackIo(
        LogPackHeader &logh, cybozu::util::File &fileR,
        std::queue<LogPackIo> &queue) {
        for (size_t i = 0; i < logh.nRecords(); i++) {
            LogPackIo packIo;
            packIo.set(logh, i);
            const walb::LogRecord &rec = packIo.rec;
            if (!rec.hasData()) continue;
            const uint32_t ioSizePb = rec.ioSizePb(logh.pbs());
            for (size_t j = 0; j < ioSizePb; j++) {
                packIo.blockS.addBlock(readBlock(fileR));
            }
            if (!rec.hasDataForChecksum()) { continue; }
            /* Only normal IOs will be inserted. */
            queue.push(std::move(packIo));
        }
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    WlogVerifier v(config);
    v.run();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("verify_wlog")
