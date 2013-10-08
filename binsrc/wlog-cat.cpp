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
#include <queue>
#include <memory>
#include <deque>

#include <unistd.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <getopt.h>

#include "stdout_logger.hpp"

#include "util.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "memory_buffer.hpp"
#include "walb/walb.h"

constexpr size_t DEFAULT_MAX_IO_SIZE = 65536;

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
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : ldevPath_()
        , outPath_("-")
        , beginLsid_(0)
        , endLsid_(-1)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& ldevPath() const { return ldevPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    const std::string& outPath() const { return outPath_; }
    bool isOutStdout() const { return outPath_ == "-"; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("ldevPath: %s\n"
                 "outPath: %s\n"
                 "beginLsid: %" PRIu64 "\n"
                 "endLsid: %" PRIu64 "\n"
                 "verbose: %d\n"
                 "isHelp: %d\n",
                 ldevPath().c_str(), outPath().c_str(),
                 beginLsid(), endLsid(),
                 isVerbose(), isHelp());
        int i = 0;
        for (const auto &s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (beginLsid() >= endLsid()) {
            throwError("beginLsid must be < endLsid.");
        }
        if (ldevPath_.empty()) {
            throwError("Specify log device path.");
        }
        if (outPath_.empty()) {
            throwError("Specify output wlog path.");
        }
    }

    class Error : public std::runtime_error {
    public:
        explicit Error(const std::string &msg)
            : std::runtime_error(msg) {}
    };

private:
    /* Option ids. */
    enum Opt {
        OUT_PATH = 1,
        BEGIN_LSID,
        END_LSID,
        VERBOSE,
        HELP,
    };

    void throwError(const char *format, ...) const {
        va_list args;
        std::string msg;
        va_start(args, format);
        try {
            msg = cybozu::util::formatStringV(format, args);
        } catch (...) {}
        va_end(args);
        throw Error(msg);
    }

    void parse(int argc, char* argv[]) {
        while (1) {
            const struct option long_options[] = {
                {"outPath", 1, 0, Opt::OUT_PATH},
                {"beginLsid", 1, 0, Opt::BEGIN_LSID},
                {"endLsid", 1, 0, Opt::END_LSID},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "o:b:e:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::OUT_PATH:
            case 'o':
                outPath_ = std::string(optarg);
                break;
            case Opt::BEGIN_LSID:
            case 'b':
                beginLsid_ = ::atoll(optarg);
                break;
            case Opt::END_LSID:
            case 'e':
                endLsid_ = ::atoll(optarg);
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
                throwError("Unknown option.");
            }
        }

        while(optind < argc) {
            args_.push_back(std::string(argv[optind++]));
        }
        if (!args_.empty()) {
            ldevPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlcat: extract wlog from a log device.\n"
            "Usage: wlcat [options] LOG_DEVICE_PATH\n"
            "Options:\n"
            "  -o, --outPath PATH:   output wlog path. '-' for stdout. (default: '-')\n"
            "  -b, --beginLsid LSID: begin lsid to restore. (default: 0)\n"
            "  -e, --endLsid LSID:   end lsid to restore. (default: -1)\n"
            "  -v, --verbose:        verbose messages to stderr.\n"
            "  -h, --help:           show this message.\n");
    }
};

/**
 * Walb log device reader using aio.
 */
class WalbLdevReader
{
private:
    cybozu::util::BlockDevice bd_;
    const uint32_t pbs_;
    const uint32_t bufferPb_; /* [physical block]. */
    const uint32_t maxIoPb_;
    std::shared_ptr<char> buffer_;
    walb::log::SuperBlock super_;
    cybozu::aio::Aio aio_;
    uint64_t aheadLsid_;
    std::queue<std::pair<uint32_t, uint32_t> > ioQ_; /* aioKey, ioPb */
    uint32_t aheadIdx_;
    uint32_t readIdx_;
    uint32_t pendingPb_;
    uint32_t remainingPb_;
public:
    /**
     * @wldevPath walb log device path.
     * @bufferSize buffer size to read ahead [byte].
     * @maxIoSize max IO size [byte].
     */
    WalbLdevReader(const std::string &wldevPath, uint32_t bufferSize,
                   uint32_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : bd_(wldevPath.c_str(), O_RDONLY | O_DIRECT)
        , pbs_(bd_.getPhysicalBlockSize())
        , bufferPb_(bufferSize / pbs_)
        , maxIoPb_(maxIoSize / pbs_)
        , buffer_(cybozu::util::allocateBlocks<char>(pbs_, pbs_, bufferPb_))
        , super_(bd_)
        , aio_(bd_.getFd(), bufferPb_)
        , aheadLsid_(0)
        , ioQ_()
        , aheadIdx_(0)
        , readIdx_(0)
        , pendingPb_(0)
        , remainingPb_(0) {
        if (bufferPb_ == 0) {
            throw RT_ERR("bufferSize must be more than physical block size.");
        }
        if (maxIoPb_ == 0) {
            throw RT_ERR("maxIoSize must be more than physical block size.");
        }
    }
    ~WalbLdevReader() noexcept {
        while (!ioQ_.empty()) {
            try {
                uint32_t aioKey, ioPb;
                std::tie(aioKey, ioPb) = ioQ_.front();
                aio_.waitFor(aioKey);
            } catch (...) {
            }
            ioQ_.pop();
        }
    }
    uint32_t pbs() const { return pbs_; }
    uint32_t queueSize() const { return bufferPb_; }
    walb::log::SuperBlock &super() { return super_; }
    /**
     * Read data.
     * @data buffer pointer to fill.
     * @pb size [physical block].
     */
    void read(char *data, size_t pb) {
        while (0 < pb) {
            readBlock(data);
            data += pbs_;
            pb--;
        }
    }
    /**
     * Invoke asynchronous read IOs to fill the buffer.
     */
    void readAhead() {
        size_t n = 0;
        while (remainingPb_ + pendingPb_ < bufferPb_) {
            prepareAheadIo();
            n++;
        }
        if (0 < n) {
            aio_.submit();
        }
    }
    /**
     * Invoke readAhead() if the buffer usage is less than a specified ratio.
     * @ratio 0.0 <= ratio <= 1.0
     */
    void readAhead(float ratio) {
        float used = remainingPb_ + pendingPb_;
        float total = bufferPb_;
        if (used / total < ratio) {
            readAhead();
        }
    }
    /**
     * Reset current IOs and start read from a lsid.
     */
    void reset(uint64_t lsid) {
        /* Wait for all pending aio(s). */
        while (!ioQ_.empty()) {
            uint32_t aioKey, ioPb;
            std::tie(aioKey, ioPb) = ioQ_.front();
            aio_.waitFor(aioKey);
            ioQ_.pop();
        }
        /* Reset indicators. */
        aheadLsid_ = lsid;
        readIdx_ = 0;
        aheadIdx_ = 0;
        pendingPb_ = 0;
        remainingPb_ = 0;
    }
private:
    char *getBuffer(size_t idx) {
        return buffer_.get() + idx * pbs_;
    }
    void plusIdx(uint32_t &idx, size_t diff) {
        idx += diff;
        assert(idx <= bufferPb_);
        if (idx == bufferPb_) {
            idx = 0;
        }
    }
    uint32_t decideIoPb() const {
        uint64_t ioPb = maxIoPb_;
        /* available buffer size. */
        uint64_t availBufferPb0 = bufferPb_ - (remainingPb_ + pendingPb_);
        ioPb = std::min(ioPb, availBufferPb0);
        /* Internal ring buffer edge. */
        uint64_t availBufferPb1 = bufferPb_ - aheadIdx_;
        ioPb = std::min(ioPb, availBufferPb1);
        /* Log device ring buffer edge. */
        uint64_t s = super_.getRingBufferSize();
        ioPb = std::min(ioPb, s - aheadLsid_ % s);
        assert(ioPb <= std::numeric_limits<uint32_t>::max());
        return ioPb;
    }
    void prepareAheadIo() {
        /* Decide IO size. */
        uint32_t ioPb = decideIoPb();
        assert(aheadIdx_ + ioPb <= bufferPb_);
        uint64_t off = super_.getOffsetFromLsid(aheadLsid_);
#ifdef DEBUG
        uint64_t off1 = super_.getOffsetFromLsid(aheadLsid_ + ioPb);
        assert(off < off1 || off1 == super_.getRingBufferOffset());
#endif

        /* Prepare an IO. */
        char *buf = getBuffer(aheadIdx_);
        uint32_t aioKey = aio_.prepareRead(off * pbs_, ioPb * pbs_, buf);
        assert(aioKey != 0);
        ioQ_.push(std::make_pair(aioKey, ioPb));
        aheadLsid_ += ioPb;
        pendingPb_ += ioPb;
        plusIdx(aheadIdx_, ioPb);
    }
    void readBlock(char *data) {
        if (remainingPb_ == 0 && pendingPb_ == 0) {
            readAhead();
        }
        if (remainingPb_ == 0) {
            assert(0 < pendingPb_);
            assert(!ioQ_.empty());
            uint32_t aioKey, ioPb;
            std::tie(aioKey, ioPb) = ioQ_.front();
            ioQ_.pop();
            aio_.waitFor(aioKey);
            remainingPb_ = ioPb;
            assert(ioPb <= pendingPb_);
            pendingPb_ -= ioPb;
        }
        assert(0 < remainingPb_);
        ::memcpy(data, getBuffer(readIdx_), pbs_);
        remainingPb_--;
        plusIdx(readIdx_, 1);
    }
};

class WalbLogReader
{
private:
    const Config &config_;
    WalbLdevReader ldevReader_;
    walb::log::SuperBlock &super_;
    cybozu::util::BlockAllocator<uint8_t> ba_;

    using PackHeader = walb::log::PackHeaderRaw;
    using PackIo = walb::log::PackIoRaw;
    using Block = std::shared_ptr<uint8_t>;

public:
    WalbLogReader(const Config &config, size_t bufferSize,
                   size_t maxIoSize = DEFAULT_MAX_IO_SIZE)
        : config_(config)
        , ldevReader_(config.ldevPath(), bufferSize, maxIoSize)
        , super_(ldevReader_.super())
        , ba_(ldevReader_.queueSize() * 2, ldevReader_.pbs(), ldevReader_.pbs()) {
    }
    DISABLE_COPY_AND_ASSIGN(WalbLogReader);
    DISABLE_MOVE(WalbLogReader);
    /**
     * Read walb log from the device and write to outFd with wl header.
     */
    void catLog(int outFd) {
        if (outFd <= 0) {
            throw RT_ERR("outFd is not valid.");
        }
        cybozu::util::FdWriter fdw(outFd);

        /* Set lsids. */
        u64 beginLsid = config_.beginLsid();
        if (beginLsid < super_.getOldestLsid()) {
            beginLsid = super_.getOldestLsid();
        }

        u32 salt = super_.getLogChecksumSalt();
        u32 pbs = super_.getPhysicalBlockSize();

        /* Create and write walblog header. */
        walb::log::FileHeader wh;
        wh.init(pbs, salt, super_.getUuid(), beginLsid, config_.endLsid());
        wh.write(outFd);

        /* Read and write each logpack. */
        if (config_.isVerbose()) {
            ::fprintf(::stderr, "beginLsid: %" PRIu64 "\n", beginLsid);
        }
        ldevReader_.reset(beginLsid);
        u64 lsid = beginLsid;
        u64 totalPaddingPb = 0;
        u64 nPacks = 0;
        while (lsid < config_.endLsid()) {
            bool isEnd = false;
            readAheadLoose();
            std::unique_ptr<PackHeader> loghP;
            try {
                loghP = readLogpackHeader(lsid);
            } catch (walb::log::InvalidLogpackHeader& e) {
                if (config_.isVerbose()) {
                    ::fprintf(::stderr, "Caught invalid logpack header error.\n");
                }
                isEnd = true;
                break;
            }
            PackHeader &logh = *loghP;
            std::queue<PackIo> q;
            isEnd = readAllLogpackData(logh, q);
            writeLogpack(fdw, logh, q);
            lsid = logh.nextLogpackLsid();
            totalPaddingPb += logh.totalPaddingPb();
            nPacks++;
            if (isEnd) { break; }
        }
        if (config_.isVerbose()) {
            ::fprintf(::stderr, "endLsid: %" PRIu64 "\n"
                      "lackOfLogPb: %" PRIu64 "\n"
                      "totalPaddingPb: %" PRIu64 "\n"
                      "nPacks: %" PRIu64 "\n",
                      lsid, config_.endLsid() - lsid, totalPaddingPb, nPacks);
        }
        /* Write termination block. */
        PackHeader logh(ba_.alloc(), pbs, salt);
        logh.setEnd();
        logh.write(fdw);
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
     * Read a logpack header.
     */
    std::unique_ptr<PackHeader> readLogpackHeader(uint64_t lsid) {
        Block block = readBlock();
        std::unique_ptr<PackHeader> logh(
            new PackHeader(
                block, super_.getPhysicalBlockSize(),
                super_.getLogChecksumSalt()));
#if 0
        logh->print(::stderr);
#endif
        if (!logh->isValid()) {
            throw walb::log::InvalidLogpackHeader();
        }
        if (logh->header().logpack_lsid != lsid) {
            throw walb::log::InvalidLogpackHeader(
                cybozu::util::formatString(
                    "logpack %" PRIu64 " is not the expected one %" PRIu64 ".",
                    logh->header().logpack_lsid, lsid));
        }
        return logh;
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
            } catch (walb::log::InvalidLogpackData& e) {
                if (config_.isVerbose()) { logh.print(::stderr); }
                uint64_t prevLsid = logh.nextLogpackLsid();
                logh.shrink(i);
                uint64_t currentLsid = logh.nextLogpackLsid();
                if (config_.isVerbose()) { logh.print(::stderr); }
                isEnd = true;
                if (config_.isVerbose()) {
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
            //if (config_.isVerbose()) packIo.print(::stderr);
            throw walb::log::InvalidLogpackData();
        }
    }
    /**
     * Write a logpack.
     */
    void writeLogpack(
        cybozu::util::FdWriter &fdw, PackHeader &logh,
        std::queue<PackIo> &q) {
        if (logh.nRecords() == 0) {
            return;
        }
        /* Write the header. */
        fdw.write(logh.ptr<char>(), logh.pbs());
        /* Write the IO data. */
        size_t nWritten = 0;
        while (!q.empty()) {
            PackIo packIo = std::move(q.front());
            q.pop();
            walb::log::RecordRaw &rec = packIo.record();
            if (!rec.hasData()) { continue; }
            for (size_t i = 0; i < rec.ioSizePb(); i++) {
                fdw.write(packIo.blockData().rawData<const char>(i), logh.pbs());
                nWritten++;
            }
        }
        assert(nWritten == logh.totalIoSize());
    }
};

int main(int argc, char* argv[])
{
    int ret = 0;
    const size_t BUFFER_SIZE = 4 * 1024 * 1024; /* 4MB */

    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();

        WalbLogReader wlReader(config, BUFFER_SIZE);
        if (config.isOutStdout()) {
            wlReader.catLog(1);
        } else {
            cybozu::util::FileOpener fo(
                config.outPath(),
                O_WRONLY | O_CREAT | O_TRUNC,
                S_IRWXU | S_IRGRP | S_IROTH);
            wlReader.catLog(fo.fd());
            cybozu::util::FdWriter(fo.fd()).fdatasync();
            fo.close();
        }
    } catch (Config::Error& e) {
        LOGe("Command line error: %s\n\n", e.what());
        Config::printHelp();
        ret = 1;
    } catch (std::runtime_error& e) {
        LOGe("Error: %s\n", e.what());
        ret = 1;
    } catch (std::exception& e) {
        LOGe("Exception: %s\n", e.what());
        ret = 1;
    } catch (...) {
        LOGe("Caught other error.\n");
        ret = 1;
    }

    return ret;
}

/* end of file. */
