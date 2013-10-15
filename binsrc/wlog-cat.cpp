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

#include "stdout_logger.hpp"

#include "util.hpp"
#include "walb_log_dev.hpp"
#include "aio_util.hpp"
#include "memory_buffer.hpp"
#include "walb/walb.h"
#include "fileorfd.hpp"

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
		opt.appendOpt(&isVerbose_, false, "v", "verbose messages to stderr.");
        opt.appendOpt(&outPath_, "-", "o", "PATH: output wlog path. '-' for stdout. (default: '-')");
		opt.appendOpt(&beginLsid_, 0, "b", "LSID: begin lsid to restore. (default: 0)");
		opt.appendOpt(&endLsid_, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
		opt.appendHelp("h", "show this message.");
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
    walb::log::AsyncDevReader ldevReader_;
    walb::log::SuperBlock &super_;
    cybozu::util::BlockAllocator<uint8_t> ba_;
    bool isVerbose_;

    using PackHeader = walb::log::PackHeaderRaw;
    using PackIo = walb::log::PackIoRaw;
    using Block = std::shared_ptr<uint8_t>;

public:
    WlogExtractor(const std::string &ldevPath,
                  uint64_t beginLsid, uint64_t endLsid,
                  bool isVerbose = false)
        : beginLsid_(beginLsid), endLsid_(endLsid)
        , ldevReader_(ldevPath)
        , super_(ldevReader_.super())
        , ba_(ldevReader_.queueSize() * 2, ldevReader_.pbs(), ldevReader_.pbs())
        , isVerbose_(isVerbose) {
    }
    DISABLE_COPY_AND_ASSIGN(WlogExtractor);
    DISABLE_MOVE(WlogExtractor);
    /**
     * Read walb log from the device and write to outFd with wl header.
     */
    void operator()(int outFd) {
        if (outFd <= 0) throw RT_ERR("outFd is not valid.");
        walb::log::Writer writer(outFd);

        /* Set lsids. */
        uint64_t beginLsid = beginLsid_;
        if (beginLsid < super_.getOldestLsid()) {
            beginLsid = super_.getOldestLsid();
        }

        uint32_t salt = super_.getLogChecksumSalt();
        uint32_t pbs = super_.getPhysicalBlockSize();

        /* Create and write walblog header. */
        walb::log::FileHeader wh;
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
        PackHeader packH(nullptr, super_.getPhysicalBlockSize(),
                         super_.getLogChecksumSalt());
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
            std::queue<PackIo> q;
            isEnd = readAllLogpackData(packH, q);
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
private:
    void readAheadLoose() {
        ldevReader_.readAhead(0.5);
    }
    Block readBlock() {
        Block b = ba_.alloc();
        ldevReader_.read(reinterpret_cast<char *>(b.get()), 1);
        return b;
    }
    /**
     * Get block list from packIo list.
     */
    static std::queue<Block> toBlocks(std::queue<PackIo> &src) {
        std::queue<Block> dst;
        while (!src.empty()) {
            PackIo packIo = std::move(src.front());
            src.pop();
            walb::log::BlockData &blockD = packIo.blockData();
            for (size_t i = 0; i < blockD.nBlocks(); i++) {
                dst.push(blockD.getBlock(i));
            }
        }
        assert(src.empty());
        return dst;
    }

    /**
     * Read a logpack header.
     * RETURN:
     *   false if got invalid logpack header.
     */
    bool readLogpackHeader(PackHeader &packH, uint64_t lsid) {
        packH.reset(readBlock());
#if 0
        packH.print(::stderr);
#endif
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
    bool readAllLogpackData(PackHeader &logh, std::queue<PackIo> &q) {
        bool isEnd = false;
        for (size_t i = 0; i < logh.nRecords(); i++) {
            PackIo packIo(logh, i);
            try {
                readLogpackData(packIo);
                q.push(std::move(packIo));
            } catch (walb::log::InvalidIo& e) {
                if (isVerbose_) { logh.print(::stderr); }
                uint64_t prevLsid = logh.nextLogpackLsid();
                logh.shrink(i);
                uint64_t currentLsid = logh.nextLogpackLsid();
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
    void readLogpackData(PackIo& packIo) {
        walb::log::RecordRaw &rec = packIo.record();
        if (!rec.hasData()) { return; }
        //::printf("ioSizePb: %u\n", logd.ioSizePb()); //debug
        readAheadLoose();
        for (size_t i = 0; i < rec.ioSizePb(); i++) {
            packIo.blockData().addBlock(readBlock());
        }
        if (!packIo.isValid()) {
            //if (isVerbose_) packIo.print(::stderr);
            throw walb::log::InvalidIo();
        }
    }
};

int main(int argc, char* argv[])
try {
    Config config(argc, argv);
    config.check();

    WlogExtractor extractor(
        config.ldevPath(), config.beginLsid(), config.endLsid(),
        config.isVerbose());
    FileOrFd fof;
    if (config.isOutStdout()) {
        fof.setFd(1);
    } else {
        fof.open(config.outPath(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
    extractor(fof.fd());
    fof.close();
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
	return 1;
} catch (...) {
    LOGe("Caught other error.\n");
	return 1;
}

/* end of file. */
