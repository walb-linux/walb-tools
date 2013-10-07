/**
 * @file
 * @brief WalB log pretty printer.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
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
#include "fileio.hpp"
#include "walb_log.hpp"
#include "aio_util.hpp"
#include "memory_buffer.hpp"
#include "walb/walb.h"

/**
 * Command line configuration.
 */
class Config
{
private:
    std::string inWlogPath_;
    uint64_t beginLsid_;
    uint64_t endLsid_;
    bool isVerbose_;
    bool isHelp_;
    std::vector<std::string> args_;

public:
    Config(int argc, char* argv[])
        : inWlogPath_("-")
        , beginLsid_(0)
        , endLsid_(-1)
        , isVerbose_(false)
        , isHelp_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& inWlogPath() const { return inWlogPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    bool isInputStdin() const { return inWlogPath_ == "-"; }
    bool isVerbose() const { return isVerbose_; }
    bool isHelp() const { return isHelp_; }

    void print() const {
        ::printf("inWlogPath: %s\n"
                 "beginLsid: %" PRIu64 "\n"
                 "endLsid: %" PRIu64 "\n"
                 "verbose: %d\n"
                 "isHelp: %d\n"
                 , inWlogPath().c_str()
                 , beginLsid(), endLsid()
                 , isVerbose(), isHelp());
        int i = 0;
        for (const auto &s : args_) {
            ::printf("arg%d: %s\n", i++, s.c_str());
        }
    }

    static void printHelp() {
        ::printf("%s", generateHelpString().c_str());
    }

    void check() const {
        if (endLsid() <= beginLsid()) {
            throwError("beginLsid must be < endLsid.");
        }
        if (inWlogPath_.empty()) {
            throwError("Specify walb log path.");
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
        IN_WLOG_PATH = 1,
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
                {"inWlogPath", 1, 0, Opt::IN_WLOG_PATH},
                {"beginLsid", 1, 0, Opt::BEGIN_LSID},
                {"endLsid", 1, 0, Opt::END_LSID},
                {"verbose", 0, 0, Opt::VERBOSE},
                {"help", 0, 0, Opt::HELP},
                {0, 0, 0, 0}
            };
            int option_index = 0;
            int c = ::getopt_long(argc, argv, "i:b:e:vh", long_options, &option_index);
            if (c == -1) { break; }

            switch (c) {
            case Opt::IN_WLOG_PATH:
            case 'i':
                inWlogPath_ = std::string(optarg);
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
            inWlogPath_ = args_[0];
        }
    }

    static std::string generateHelpString() {
        return cybozu::util::formatString(
            "Wlog-show: pretty-print wlog input.\n"
            "Usage: wlog-show [options]\n"
            "Options:\n"
            "  -i, --inWlogPath PATH: input wlog path. '-' for stdin. (default: '-')\n"
            "  -b, --beginLsid LSID:  begin lsid to restore. (default: 0)\n"
            "  -e, --endLsid LSID:    end lsid to restore. (default: -1)\n"
            "  -v, --verbose:         verbose messages to stderr.\n"
            "  -h, --help:            show this message.\n");
    }
};

namespace walb {
namespace log {

/**
 * Walb log reader.
 */
class FileReader /* final */
{
private:
    int fd_;
    cybozu::util::FdReader fdr_;
    bool isReadHeader_;
    bool isEnd_;

    std::shared_ptr<FileHeader> wh_;
    std::shared_ptr<PackHeaderRaw> pack_;
    uint16_t recIdx_;
    uint16_t totalSize_;

public:
    explicit FileReader(int fd)
        : fd_(fd), fdr_(fd)
        , isReadHeader_(false)
        , isEnd_(false)
        , pack_()
        , recIdx_(0)
        , totalSize_(0) {}

    ~FileReader() noexcept {}

    std::shared_ptr<FileHeader> readHeader() {
        if (isReadHeader_) {
            throw RT_ERR("Log header has been called already.");
        }
        isReadHeader_ = true;
        wh_.reset(new FileHeader());
        wh_->read(fdr_);
        if (!wh_->isValid(true)) {
            throw RT_ERR("invalid walb log header.");
        }
        return wh_;
    }

    std::shared_ptr<PackDataRaw> readLog() {
        if (!isReadHeader_) {
            throw RT_ERR("readHeader");
        }
        fillPackIfNeed();
        if (!pack_) {
            return std::shared_ptr<PackDataRaw>();
        }

        std::shared_ptr<PackDataRaw> logd(
            new PackDataRaw(*pack_, recIdx_));

        if (logd->hasData()) {
            for (size_t i = 0; i < logd->ioSizePb(); i++) {
                try {
                    std::shared_ptr<uint8_t> b = allocB();
                    fdr_.read(reinterpret_cast<char *>(b.get()), wh_->pbs());
                    logd->addBlock(b);
                } catch (cybozu::util::EofError &e) {
                    throw InvalidLogpackData();
                }
            }
        }
        if (!logd->isValid()) {
            logd->print();
            ::printf("invalid....\n"); /* debug */
            throw InvalidLogpackData();
        }

        recIdx_++;
        return logd;
    }

private:
    void fillPackIfNeed() {
        assert(isReadHeader_);
        if (isEnd_ || (pack_ && recIdx_ < pack_->nRecords())) { return; }

        std::shared_ptr<uint8_t> b = allocB();
        try {
            fdr_.read(reinterpret_cast<char *>(b.get()), wh_->pbs());
            pack_.reset(new PackHeaderRaw(b, wh_->pbs(), wh_->salt()));
            if (!pack_->isValid()) {
                throw RT_ERR("Invalid logpack header.");
            }
            if (pack_->isEnd()) {
                pack_.reset();
                isEnd_ = true;
                return;
            }
            recIdx_ = 0;
            // pack_->print();
        } catch (cybozu::util::EofError &e) {
            pack_.reset();
            isEnd_ = true;
        }
    }

    std::shared_ptr<uint8_t> allocB() {
        assert(isReadHeader_);
        return cybozu::util::allocateBlocks<uint8_t>(LOGICAL_BLOCK_SIZE, wh_->pbs());
    }
};

/**
 * Pretty printer of walb log.
 */
class Printer
{
private:
    const Config &config_;

public:
    Printer(const Config &config)
        : config_(config) {}

    void run() {
        int fd = 0;
        std::shared_ptr<cybozu::util::FileOpener> fo;
        if (!config_.isInputStdin()) {
            fo.reset(new cybozu::util::FileOpener(config_.inWlogPath(), O_RDONLY));
            fd = fo->fd();
        }
        FileReader reader(fd);

        std::shared_ptr<FileHeader> wh = reader.readHeader();
        wh->print();

        std::shared_ptr<PackDataRaw> log = reader.readLog();
        try {
            while (log) {
                log->printOneline();
                log = reader.readLog();
            }
        } catch (InvalidLogpackData &e) {
            /* now editing */
            throw;
        }
        if (fo) { fo->close(); }
    }
};

}} //namespace walb::log

int main(int argc, char* argv[])
{
    try {
        Config config(argc, argv);
        if (config.isHelp()) {
            Config::printHelp();
            return 0;
        }
        config.check();
        walb::log::Printer printer(config);
        printer.run();
        return 0;
    } catch (Config::Error& e) {
        LOGe("Command line error: %s\n\n", e.what());
        Config::printHelp();
        return 1;
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
