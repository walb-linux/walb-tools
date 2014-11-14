/**
 * @file
 * @brief Update walb log header.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "walb_log_file.hpp"
#include "aio_util.hpp"
#include "walb/walb.h"
#include "cybozu/atoi.hpp"
#include "walb_util.hpp"

using namespace walb;

/**
 * Command line configuration.
 */
class Config
{
private:
    uint64_t beginLsid_;
    uint64_t endLsid_;
    uint32_t salt_;
    std::vector<u8> uuid_;
    bool isVerbose_;
    std::string inWlogPath_;
    std::vector<std::string> args_;
    cybozu::Option opt;

public:
    Config(int argc, char* argv[])
        : beginLsid_(0)
        , endLsid_(-1)
        , salt_(0)
        , uuid_()
        , isVerbose_(false)
        , args_() {
        parse(argc, argv);
    }

    const std::string& inWlogPath() const { return args_[0]; }
    uint64_t beginLsid() const { return beginLsid_; }
    uint64_t endLsid() const { return endLsid_; }
    uint32_t salt() const { return salt_; }
    const std::vector<u8>& uuid() const { return uuid_; }
    bool isVerbose() const { return isVerbose_; }
    bool isSetBeginLsid() const { return opt.isSet(&beginLsid_); }
    bool isSetEndLsid() const { return opt.isSet(&endLsid_); }
    bool isSetSalt() const { return opt.isSet(&salt_); }
    bool isSetUuid() const { return !uuid_.empty(); }

private:
    void setUuid(const std::string &uuidStr) {
        if (uuidStr.size() != 32) {
            throw RT_ERR("Invalid UUID string.");
        }
        uuid_.resize(UUID_SIZE);
        for (size_t i = 0; i < UUID_SIZE; i++) {
            /* ex. "ff" -> 255 */
            uuid_[i] = cybozu::hextoi(&uuidStr[i * 2], 2);
        }
    }

    void parse(int argc, char* argv[]) {
        std::string uuidStr;
        opt.setDescription("Wlupdate: update wlog file header.");
        opt.appendOpt(&beginLsid_, 0, "b", "LSID: begin lsid.");
        opt.appendOpt(&endLsid_, uint64_t(-1), "e", "LSID: end lsid.");
        opt.appendOpt(&salt_, 0, "s", "SALT: logpack salt.");
        opt.appendOpt(&uuidStr, "", "u", "UUID: uuid in hex string.");
        opt.appendBoolOpt(&isVerbose_, "v", ": verbose messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        opt.appendParam(&inWlogPath_, "WLOG_PATH", ": walb log path. must be seekable.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            exit(1);
        }
        if (!uuidStr.empty()) {
            setUuid(uuidStr);
        }
    }
};

class WalbLogUpdater
{
private:
    const Config &config_;
public:
    WalbLogUpdater(const Config &config)
        : config_(config) {}

    void update() {
        cybozu::util::File file(config_.inWlogPath(), O_RDWR);
        WlogFileHeader wh;

        /* Read header. */
        file.lseek(0, SEEK_SET);
        wh.readFrom(file);
        if (!wh.isValid(true)) {
            throw RT_ERR("invalid wlog header.");
        }
        std::cerr << wh << std::endl; /* debug */

        /* Update */
        bool updated = false;
        struct walblog_header& h = wh.header();

        if (config_.isSetBeginLsid()) {
            updated = true;
            h.begin_lsid = config_.beginLsid();
        }
        if (config_.isSetEndLsid()) {
            updated = true;
            h.end_lsid = config_.endLsid();
        }
        if (config_.isSetSalt()) {
            updated = true;
            h.log_checksum_salt = config_.salt();
        }
        if (config_.isSetUuid()) {
            updated = true;
            ::memcpy(h.uuid, &config_.uuid()[0], UUID_SIZE);
        }

        /* Write header if necessary. */
        if (updated) {
            if (!wh.isValid(false)) {
                throw RT_ERR("Updated header is invalid.");
            }
            file.lseek(0, SEEK_SET);
            wh.writeTo(file);
            std::cerr << wh << std::endl; /* debug */
        } else {
            ::fprintf(::stderr, "Not updated.\n");
        }
    }
};

int doMain(int argc, char* argv[])
{
    Config config(argc, argv);
    WalbLogUpdater wlUpdater(config);
    wlUpdater.update();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-update")
