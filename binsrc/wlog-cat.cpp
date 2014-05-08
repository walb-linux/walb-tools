/**
 * @file
 * @brief Read walb log device and archive it.
 * @author HOSHINO Takashi
 *
 * (C) 2012 Cybozu Labs, Inc.
 */
#include <string>
#include <cstdio>
#include <stdexcept>
#include <memory>
#include <deque>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

/**
 * Wlcat configuration.
 */
class Config
{
private:
    std::string ldevPath_;
    std::string outPath_;
    uint64_t beginLsid_;
    uint64_t endLsid_;
    bool isVerbose_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , outPath_("-")
        , beginLsid_(0)
        , endLsid_(-1)
        , isVerbose_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    const std::string& outPath() const { return outPath_; }
    bool isOutStdout() const { return outPath_ == "-"; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (beginLsid() >= endLsid()) {
            throw RT_ERR("beginLsid must be < endLsid.");
        }
    }

private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlcat: extract wlog from a log device.");
        opt.appendOpt(&outPath_, "-", "o", "PATH: output wlog path. '-' for stdout. (default: '-')");
        opt.appendOpt(&beginLsid_, 0, "b", "LSID: begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid_, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&ldevPath_, "LOG_DEVICE_PATH");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

class WlogExtractor
{
private:
    const uint64_t beginLsid_, endLsid_;
    walb::device::AsyncWldevReader ldevReader_;
    walb::device::SuperBlock &super_;
    const uint32_t pbs_;
    bool isVerbose_;

    using LogPackHeader = walb::LogPackHeader;
    using LogPackIo = walb::LogPackIo;
public:
    WlogExtractor(const std::string &ldevPath,
                  uint64_t beginLsid, uint64_t endLsid,
                  bool isVerbose = false)
        : beginLsid_(beginLsid), endLsid_(endLsid)
        , ldevReader_(ldevPath)
        , super_(ldevReader_.super())
        , pbs_(ldevReader_.pbs())
        , isVerbose_(isVerbose) {
    }
    DISABLE_COPY_AND_ASSIGN(WlogExtractor);
    DISABLE_MOVE(WlogExtractor);
    /**
     * Read walb log from the device and write to outFd with wl header.
     */
    void run(int outFd) {
        if (outFd <= 0) throw RT_ERR("outFd is not valid.");
        walb::log::Writer writer(outFd);

        /* Set lsids. */
        uint64_t beginLsid = beginLsid_;
        if (beginLsid < super_.getOldestLsid()) {
            beginLsid = super_.getOldestLsid();
        }

        const uint32_t salt = super_.getLogChecksumSalt();
        const uint32_t pbs = super_.getPhysicalBlockSize();

        /* Create and write walblog header. */
        walb::log::FileHeader wh;
        wh.init(pbs, salt, super_.getUuid2(), beginLsid, endLsid_);
        writer.writeHeader(wh);

        /* Read and write each logpack. */
        if (isVerbose_) {
            ::fprintf(::stderr, "beginLsid: %" PRIu64 "\n", beginLsid);
        }
        ldevReader_.reset(beginLsid);
        uint64_t lsid = beginLsid;
        uint64_t totalPaddingPb = 0;
        uint64_t nPacks = 0;
        LogPackHeader packH(nullptr, pbs, salt);
        while (lsid < endLsid_) {
            bool isEnd = false;
            readAheadLoose();
            if (!readLogpackHeader(packH, lsid)) {
                if (isVerbose_) {
                    ::fprintf(::stderr, "Caught invalid logpack header error.\n");
                }
                isEnd = true;
                break;
            }
            std::queue<LogPackIo> q;
            isEnd = readAllLogpackIos(packH, q);
            writer.writePack(packH, toBlocks(q));
            lsid = packH.nextLogpackLsid();
            totalPaddingPb += packH.totalPaddingPb();
            nPacks++;
            if (isEnd) break;
        }
        if (isVerbose_) {
            ::fprintf(::stderr, "endLsid: %" PRIu64 "\n"
                      "lackOfLogPb: %" PRIu64 "\n"
                      "totalPaddingPb: %" PRIu64 "\n"
                      "nPacks: %" PRIu64 "\n",
                      lsid, endLsid_ - lsid, totalPaddingPb, nPacks);
        }
        writer.close();
    }
    using AlignedArray = walb::AlignedArray;
private:
    void readAheadLoose() {
        ldevReader_.readAhead(0.5);
    }
    AlignedArray readBlock() {
        AlignedArray b(pbs_);
        ldevReader_.read(b.data(), 1);
        return b;
    }
    /**
     * Get block list from packIo list.
     */
    static std::queue<AlignedArray> toBlocks(std::queue<LogPackIo> &src) {
        std::queue<AlignedArray> dst;
        while (!src.empty()) {
            LogPackIo& packIo = src.front();
            walb::LogBlockShared &blockS = packIo.blockS;
            for (size_t i = 0; i < blockS.nBlocks(); i++) {
                dst.push(std::move(blockS.getBlock(i)));
            }
            src.pop();
        }
        return dst;
    }

    /**
     * Read a logpack header.
     * RETURN:
     *   false if got invalid logpack header.
     */
    bool readLogpackHeader(LogPackHeader &packH, uint64_t lsid) {
        packH.setBlock(readBlock());
        if (!packH.isValid()) return false;
        if (packH.header().logpack_lsid != lsid) {
            ::fprintf(::stderr, "logpack %" PRIu64 " is not the expected one %" PRIu64 "."
                      , packH.header().logpack_lsid, lsid);
            return false;
        }
        return true;
    }
    /**
     * Read all IOs data of a logpack.
     *
     * RETURN:
     *   true if logpack has shrinked and should end.
     */
    bool readAllLogpackIos(LogPackHeader &logh, std::queue<LogPackIo> &q) {
        bool isEnd = false;
        for (size_t i = 0; i < logh.nRecords(); i++) {
            LogPackIo packIo;
            packIo.set(logh, i);
            if (readLogpackIo(packIo, logh.pbs())) {
                q.push(std::move(packIo));
            } else {
                if (isVerbose_) { logh.print(::stderr); }
                const uint64_t prevLsid = logh.nextLogpackLsid();
                logh.shrink(i);
                const uint64_t currentLsid = logh.nextLogpackLsid();
                if (isVerbose_) { logh.print(::stderr); }
                isEnd = true;
                if (isVerbose_) {
                    ::fprintf(::stderr, "Logpack shrink from %" PRIu64 " to %" PRIu64 "\n",
                              prevLsid, currentLsid);
                }
                break;
            }
        }
        return isEnd;
    }
    /**
     * Read a logpack data.
     */
    bool readLogpackIo(LogPackIo& packIo, uint32_t pbs) {
        const walb::LogRecord &rec = packIo.rec;
        if (!rec.hasData()) return true;
        readAheadLoose();
        const uint32_t ioSizePb = rec.ioSizePb(pbs);
        for (size_t i = 0; i < ioSizePb; i++) {
            packIo.blockS.addBlock(readBlock());
        }
        return packIo.isValid();
    }
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);
    Config config(argc, argv);
    config.check();

    WlogExtractor extractor(
        config.ldevPath(), config.beginLsid(), config.endLsid(),
        config.isVerbose());
    cybozu::util::File fileW;
    if (config.isOutStdout()) {
        fileW.setFd(1);
    } else {
        fileW.open(config.outPath(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
    extractor.run(fileW.fd());
    fileW.close();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
