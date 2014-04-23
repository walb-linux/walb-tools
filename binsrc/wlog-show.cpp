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

#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "fileio.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "memory_buffer.hpp"
#include "walb/walb.h"
#include "walb_util.hpp"

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
public:
    Config(int argc, char* argv[])
        : inWlogPath_("-")
        , beginLsid_(0)
        , endLsid_(-1)
        , isVerbose_(false) {
        parse(argc, argv);
    }

    const std::string& inWlogPath() const { return inWlogPath_; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    bool isInputStdin() const { return inWlogPath_ == "-"; }
    bool isVerbose() const { return isVerbose_; }

    void check() const {
        if (endLsid() <= beginLsid()) {
            throw RT_ERR("beginLsid must be < endLsid.");
        }
    }
private:
    void parse(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Wlog-show: pretty-print wlog input.");
        opt.appendOpt(&beginLsid_, 0, "b", "LSID: begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid_, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendParamOpt(&inWlogPath_, "-", "PATH", ": input wlog path. '-' for stdin. (default: '-')");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
    }
};

int main(int argc, char* argv[]) try
{
    walb::util::setLogSetting("-", false);

    Config config(argc, argv);
    config.check();

    cybozu::util::File fileR;
    if (!config.isInputStdin()) {
        fileR.open(config.inWlogPath(), O_RDONLY);
    } else {
        fileR.setFd(0); /* stdin */
    }
    walb::log::Printer printer;
    printer(fileR.fd());
} catch (std::exception& e) {
    LOGe("Exception: %s\n", e.what());
    return 1;
} catch (...) {
    LOGe("Caught other error.\n");
    return 1;
}

/* end of file. */
