/**
 * Walb device controller.
 */
#include <sstream>
#include <vector>
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "wdev_util.hpp"
#include "wdev_log.hpp"
#include "walb_logger.hpp"
#include "walb/ioctl.h"

using namespace walb;

const uint32_t DEFAULT_MAX_LOGPACK_KB = 32;
const uint32_t DEFAULT_MAX_PENDING_MB = 32;
const uint32_t DEFAULT_MIN_PENDING_MB = 16;
const uint32_t DEFAULT_QUEUE_STOP_TIMEOUT_MS = 100;
const uint32_t DEFAULT_FLUSH_INTERVAL_MB = 16;
const uint32_t DEFAULT_FLUSH_INTERVAL_MS = 100;
const uint32_t DEFAULT_NUM_PACK_BULK = 128;
const uint32_t DEFAULT_NUM_IO_BULK = 1024;

std::string generateUsage();

template <typename T>
struct Opt
{
    const T defaultValue;
    const char *name;
    const char *description;
    bool putDefault;
};

struct OptS
{
    const std::string defaultValueS;
    const char *name;
    const char *description;
    bool putDefault;
};

template <typename T>
OptS fromOpt(const Opt<T> &opt)
{
    std::stringstream ss;
    ss << opt.defaultValue;
    return {ss.str(), opt.name, opt.description, opt.putDefault};
}

struct Param
{
    const char *name;
    const char *description;
};

const Opt<uint32_t> maxLogpackKbOpt = {
    DEFAULT_MAX_LOGPACK_KB, "maxl", "SIZE : max logpack size [KiB]", true};
const Opt<uint32_t> maxPendingMbOpt = {
    DEFAULT_MAX_PENDING_MB, "maxp", "SIZE : max pending size [MiB]", true};
const Opt<uint32_t> minPendingMbOpt = {
    DEFAULT_MIN_PENDING_MB, "minp", "SIZE : min pending size [MiB]", true};
const Opt<uint32_t> queueStopTimeoutMsOpt = {
    DEFAULT_QUEUE_STOP_TIMEOUT_MS, "qp", "PERIOD : queue stopping period [ms]", true};
const Opt<uint32_t> flushIntervalMbOpt = {
    DEFAULT_FLUSH_INTERVAL_MB, "fs", "SIZE : flush interval size [MiB]", true};
const Opt<uint32_t> flushIntervalMsOpt = {
    DEFAULT_FLUSH_INTERVAL_MS, "fp", "PERIOD : flush interval period [ms]", true};
const Opt<uint32_t> numPackBulkOpt = {
    DEFAULT_NUM_PACK_BULK, "bp", "SIZE : number of packs in bulk", true};
const Opt<uint32_t> numIoBulkOpt = {
    DEFAULT_NUM_IO_BULK, "bi", "SIZE : numer of IOs in bulk", true};
const Opt<std::string> nameOpt = {
    "", "n", "NAME : walb device name (default: decided automatically)", false};
const Opt<bool> noDiscardOpt = {false, "nd", ": disable discard IOs", false};

const OptS maxLogpackKbOptS = fromOpt(maxLogpackKbOpt);
const OptS maxPendingMbOptS = fromOpt(maxPendingMbOpt);
const OptS minPendingMbOptS = fromOpt(minPendingMbOpt);
const OptS queueStopTimeoutMsOptS = fromOpt(queueStopTimeoutMsOpt);
const OptS flushIntervalMbOptS = fromOpt(flushIntervalMbOpt);
const OptS flushIntervalMsOptS = fromOpt(flushIntervalMsOpt);
const OptS numPackBulkOptS = fromOpt(numPackBulkOpt);
const OptS numIoBulkOptS = fromOpt(numIoBulkOpt);
const OptS nameOptS = fromOpt(nameOpt);
const OptS noDiscardOptS = fromOpt(noDiscardOpt);

const Param ldevParam = {"LDEV", ": log device path"};
const Param ddevParam = {"DDEV", ": data device path"};
const Param wdevParam = {"WDEV", ": walb device path"};
const Param sizeLbParam = {"SIZE_LB", ": [logical block] "
                           "suffix k,m,g,t supported. "
                           "(default: underlying data device size)"};
const Param timeoutSecParam = {"TIMEOUT", "[sec] 0 means no timeout. (default: 0)"};
const Param lsidParam = {"LSID", ": log sequence id"};
const Param intervalMsParam = {"INTERVAL", "[ms]"};

struct Option
{
    cybozu::Option opt;

    std::string cmd;
    StrVec params;
    bool isDebug;

    struct walb_start_param sParam;

    std::string name;
    bool noDiscard;

    Option(int argc, char *argv[]) {
        opt.appendParam(&cmd, "command", "command name");
        opt.appendParamVec(&params, "remaining", "remaining parameters");
        opt.appendBoolOpt(&isDebug, "debug", "debug option");

        appendOpt(&sParam.max_logpack_kb, maxLogpackKbOpt);
        appendOpt(&sParam.max_pending_mb, maxPendingMbOpt);
        appendOpt(&sParam.min_pending_mb, minPendingMbOpt);
        appendOpt(&sParam.queue_stop_timeout_ms, queueStopTimeoutMsOpt);
        appendOpt(&sParam.log_flush_interval_mb, flushIntervalMbOpt);
        appendOpt(&sParam.log_flush_interval_ms, flushIntervalMsOpt);
        appendOpt(&sParam.n_pack_bulk, numPackBulkOpt);
        appendOpt(&sParam.n_io_bulk, numIoBulkOpt);

        appendOpt(&name, nameOpt);
        appendOpt(&noDiscard, noDiscardOpt);

        opt.setDescription("walb device controller.");
        opt.setUsage(generateUsage());
        opt.appendHelp("h");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
    template <typename T>
    void appendOpt(T *pvar, const Opt<T> &optT) {
        opt.appendOpt(pvar, optT.defaultValue, optT.name, optT.description);
    }
};

struct BdevInfo
{
    uint64_t sizeLb; /* block device size [logical block]. */
    uint32_t pbs; /* physical block size [byte]. */
    cybozu::FileStat stat;

    void load(int fd) {
        sizeLb = cybozu::util::getBlockDeviceSize(fd) / LBS;
        pbs = cybozu::util::getPhysicalBlockSize(fd);
        stat = cybozu::FileStat(fd);
    }
    void load(const std::string &path) {
        cybozu::util::File file(path, O_RDONLY);
        load(file.fd());
        file.close();
    }
};

std::ostream &operator<<(std::ostream &os, const struct walb_start_param &sParam)
{
    os << "name: " << sParam.name << ", "
       << "max_pending_mb: " << sParam.max_pending_mb << ", "
       << "min_pending_mb: " << sParam.min_pending_mb << ", "
       << "queue_stop_timeout_ms: " << sParam.queue_stop_timeout_ms << ", "
       << "max_logpack_kb: " << sParam.max_logpack_kb << ", "
       << "log_flush_interval_mb: " << sParam.log_flush_interval_mb << ", "
       << "log_flush_interval_ms: " << sParam.log_flush_interval_ms << ", "
       << "n_pack_bulk: " << sParam.n_pack_bulk << ", "
       << "n_io_bulk: " << sParam.n_io_bulk;
    return os;
}

void verifyPbs(const BdevInfo &ldevInfo, const BdevInfo &ddevInfo, const char *msg)
{
    if (ldevInfo.pbs != ddevInfo.pbs) {
        throw cybozu::Exception(msg)
            << "pbs size differ" << ldevInfo.pbs << ddevInfo.pbs;
    }
}

void invokeWalbctlIoctl(struct walb_ctl &ctl, const char *msg)
{
    cybozu::util::File ctlFile(WALB_CONTROL_PATH, O_RDWR);
    if (::ioctl(ctlFile.fd(), WALB_IOCTL_CONTROL, &ctl) < 0) {
        throw cybozu::Exception(msg)
            << "ioctl failed" << WALB_CONTROL_PATH << cybozu::ErrorNo();
    }
}

/******************************************************************************
 * Handlers.
 ******************************************************************************/

void formatLdev(const Option &opt)
{
    std::string ldev, ddev;
    cybozu::util::parseStrVec(opt.params, 0, 2, {&ldev, &ddev});

    cybozu::FilePath ldevPath(ldev);
    cybozu::FilePath ddevPath(ldev);
    if (!ldevPath.stat().isBlock()) {
        throw cybozu::Exception(__func__) << "ldev is not block device" << ldev;
    }
    if (!ddevPath.stat().isBlock()) {
        throw cybozu::Exception(__func__) << "ddev is not block device" << ddev;
    }

    BdevInfo ldevInfo, ddevInfo;
    cybozu::util::File ldevFile(ldev, O_RDWR | O_DIRECT);
    int fd = ldevFile.fd();
    ldevInfo.load(fd);
    ddevInfo.load(ddev);
    verifyPbs(ldevInfo, ddevInfo, __func__);
    if (!opt.noDiscard && cybozu::util::isDiscardSupported(fd)) {
        cybozu::util::issueDiscard(fd, 0, ldevInfo.sizeLb);
    }
    device::initWalbMetadata(fd, ldevInfo.pbs, ddevInfo.sizeLb, ldevInfo.sizeLb, opt.name);
    ldevFile.fdatasync();
    ldevFile.close();

    LOGs.debug() << "format-ldev done";
}

void createWdev(const Option &opt)
{
    std::string ldev, ddev;
    cybozu::util::parseStrVec(opt.params, 0, 2, {&ldev, &ddev});

    struct walb_start_param u2kParam; // userland -> kernel.
    struct walb_start_param k2uParam; // kernel -> userland.
    struct walb_ctl ctl = {
        .command = WALB_IOCTL_START_DEV,
        .u2k = { .wminor = WALB_DYNAMIC_MINOR,
                 .buf_size = sizeof(struct walb_start_param),
                 .buf = (void *)&u2kParam, },
        .k2u = { .buf_size = sizeof(struct walb_start_param),
                 .buf = (void *)&k2uParam, },
    };

    // Check parameters.
    if (!::is_walb_start_param_valid(&opt.sParam)) {
        throw cybozu::Exception(__func__)
            << "invalid start param."
            << opt.sParam;
    }

    // Check underlying block devices.
    BdevInfo ldevInfo, ddevInfo;
    ldevInfo.load(ldev);
    ddevInfo.load(ddev);
    verifyPbs(ldevInfo, ddevInfo, __func__);

    // Make ioctl data.
    ::memcpy(&u2kParam, &opt.sParam, sizeof(u2kParam));
    if (opt.name.empty()) {
        u2kParam.name[0] = '\0';
    } else {
        ::snprintf(u2kParam.name, DISK_NAME_LEN, "%s", opt.name.c_str());
    }
    ctl.u2k.lmajor = ldevInfo.stat.majorId();
    ctl.u2k.lminor = ldevInfo.stat.minorId();
    ctl.u2k.dmajor = ddevInfo.stat.majorId();
    ctl.u2k.dminor = ddevInfo.stat.minorId();

    invokeWalbctlIoctl(ctl, __func__);
    assert(::strnlen(k2uParam.name, DISK_NAME_LEN) < DISK_NAME_LEN);

    ::printf("name %s\n"
             "major %u\n"
             "minor %u\n"
             , k2uParam.name, ctl.k2u.wmajor, ctl.k2u.wminor);

    LOGs.debug() << "create-wdev done";
}

void deleteWdev(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    struct walb_ctl ctl = {
        .command = WALB_IOCTL_STOP_DEV,
        .u2k = { .buf_size = 0, },
        .k2u = { .buf_size = 0, },
    };

    BdevInfo wdevInfo;
    wdevInfo.load(wdev);
    ctl.u2k.wmajor = wdevInfo.stat.majorId();
    ctl.u2k.wminor = wdevInfo.stat.minorId();
    invokeWalbctlIoctl(ctl, __func__);

    LOGs.debug() << "delete-wdev done";
}

void setCheckpointInterval(const Option &opt)
{
    std::string wdev, intervalMsStr;
    cybozu::util::parseStrVec(opt.params, 0, 2, {&wdev, &intervalMsStr});
    const uint32_t intervalMs = cybozu::atoi(intervalMsStr);

    device::setValueByIoctl<uint32_t>(
        wdev, WALB_IOCTL_SET_CHECKPOINT_INTERVAL, intervalMs);

    LOGs.debug() << "set-checkpoint-interval done";
}

void getCheckpointInterval(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    const uint32_t intervalMs =
        device::getValueByIoctl<uint32_t>(wdev, WALB_IOCTL_GET_CHECKPOINT_INTERVAL);
    std::cout << intervalMs << std::endl;

    LOGs.debug() << "get-checkpoint-interval done";
}

void forceCheckpoint(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    device::takeCheckpoint(wdev);

    LOGs.debug() << "force-checkpoint done";
}

void setOldestLsid(const Option &opt)
{
    std::string wdev, lsidStr;
    cybozu::util::parseStrVec(opt.params, 0, 2, {&wdev, &lsidStr});
    const uint64_t lsid = cybozu::atoi(lsidStr);

    device::setOldestLsid(wdev, lsid);

    LOGs.debug() << "set-oldest-lsid done";
}

void getOldestLsid(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    std::cout << device::getLsid(wdev, WALB_IOCTL_GET_OLDEST_LSID) << std::endl;
}

void getWrittenLsid(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    std::cout << device::getLsid(wdev, WALB_IOCTL_GET_WRITTEN_LSID) << std::endl;
}

void getPermanentLsid(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    std::cout << device::getLsid(wdev, WALB_IOCTL_GET_PERMANENT_LSID) << std::endl;
}

void getCompletedLsid(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    std::cout << device::getLsid(wdev, WALB_IOCTL_GET_COMPLETED_LSID) << std::endl;
}

void getLogUsage(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    std::cout << device::getLogUsagePb(wdev) << std::endl;
}

void getLogCapacity(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    std::cout << device::getLogCapacityPb(wdev) << std::endl;
}

void isFlushCapable(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    const int res = device::isFlushCapable(wdev) ? 1 : 0;
    std::cout << res << std::endl;
}

void resize(const Option &opt)
{
    std::string wdev, sizeLbStr;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev, &sizeLbStr});

    uint64_t sizeLb = 0;
    if (!sizeLbStr.empty()) {
        sizeLb = cybozu::util::fromUnitIntString(sizeLbStr);
    }

    device::resize(wdev, sizeLb);

    LOGs.debug() << "resize done";
}

void resetWal(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    device::resetWal(wdev);

    LOGs.debug() << "reset-wal done";
}

void isLogOverflow(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    const int res = device::isOverflow(wdev) ? 1 : 0;
    std::cout << res << std::endl;
}

void clearWal(const Option &opt)
{
    const size_t intervalMs = 500;
    std::string wdev, timeoutSecStr;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev, &timeoutSecStr});

    size_t timeoutSec = 0; // default.
    if (!timeoutSecStr.empty()) timeoutSec = cybozu::atoi(timeoutSecStr);

    /*
     * We want to remove all logs which lsid is < permanentLsid.
     */
    const uint64_t permanentLsid = device::getLsid(wdev, WALB_IOCTL_GET_PERMANENT_LSID);

    /*
     * If writtenLsid < permanentLsid, we must wait for the corresponding data IOs done,
     */
    uint64_t elapsedMs = 0;
    uint64_t writtenLsid = device::getLsid(wdev, WALB_IOCTL_GET_WRITTEN_LSID);
    bool isTimeout = true;
    while (timeoutSec == 0 || elapsedMs / 1000 < timeoutSec) {
        if (writtenLsid >= permanentLsid) {
            isTimeout = false;
            break;
        }
        LOGs.warn() << "wait a bit while writtenLsid < permanentLsid"
                    << writtenLsid << permanentLsid;
        util::sleepMs(intervalMs);
        elapsedMs += intervalMs;
        writtenLsid = device::getLsid(wdev, WALB_IOCTL_GET_WRITTEN_LSID);
    }
    if (isTimeout) throw cybozu::Exception(__func__) << "timeout";

    /*
     * Force write writtenLsid to the superblock
     * because oldestLsid <= writtenLsid must be kept always.
     */
    device::takeCheckpoint(wdev);
    device::setOldestLsid(wdev, permanentLsid);

    LOGs.debug() << "clear-wal done";
}

void freeze(const Option &opt)
{
    std::string wdev, timeoutSecStr;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev, &timeoutSecStr});

    uint32_t timeoutSec = 0;
    if (!timeoutSecStr.empty()) {
        timeoutSec = cybozu::atoi(timeoutSecStr);
    }

    device::setValueByIoctl<uint32_t>(wdev, WALB_IOCTL_FREEZE, timeoutSec);

    LOGs.debug() << "freeze done";
}

void melt(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    const int dummy = 0;
    device::setValueByIoctl<int>(wdev, WALB_IOCTL_MELT, dummy);

    LOGs.debug() << "melt done";
}

void isFrozen(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    const int res = device::getValueByIoctl<int>(wdev, WALB_IOCTL_IS_FROZEN) ? 1 : 0;
    std::cout << res << std::endl;
}

void getVersion(const Option &)
{
    uint32_t version;

    cybozu::util::File file(WALB_CONTROL_PATH, O_RDONLY);
    if (::ioctl(file.fd(), WALB_IOCTL_VERSION, &version) < 0) {
        throw cybozu::Exception(__func__) << "ioctl failed" << cybozu::ErrorNo();
    }
    file.close();

    ::printf("%u.%u.%u\n"
             , (version & 0x00ff0000) >> 16
             , (version & 0x0000ff00) >> 8
             , (version & 0x000000ff));
}

void defaultRunner(const Option &opt)
{
    throw cybozu::Exception(__func__)
        << "not implemented yet" << opt.cmd;
}

/******************************************************************************
 * Data and functions for main().
 ******************************************************************************/

using Runner = void (*)(const Option &);

struct Command
{
    Runner runner;
    std::string name;
    std::vector<Param> paramV;
    std::vector<OptS> optSV;
    const char *more;

    std::string shortHelp() const {
        std::stringstream ss;
        ss << name << " ";
        for (const Param param : paramV) {
            ss << param.name << " ";
        }
        if (!optSV.empty()) {
            ss << "[options] ";
        }
        if (more && *more) {
            ss << more;
        }
        ss << std::endl;
        return ss.str();
    }
    std::string longHelp() const {
         std::stringstream ss;
         ss << shortHelp();
         for (const Param &param : paramV) {
             ss << "  "
                << param.name << " " <<  param.description
                << std::endl;
         }
         for (const OptS &optS : optSV) {
             ss << "  -"
                << optS.name << " " << optS.description;
             if (optS.putDefault) {
                 ss << " (default:" << optS.defaultValueS << ")";
             }
             ss << std::endl;
         }
         return ss.str();
    }
};

const std::vector<Command> commandVec_ = {
    {formatLdev, "format-ldev", {ldevParam, ddevParam},
     {nameOptS, noDiscardOptS}, ""},

    {createWdev, "create-wdev", {ldevParam, ddevParam},
     {nameOptS, maxLogpackKbOptS, maxPendingMbOptS, minPendingMbOptS,
      queueStopTimeoutMsOptS, flushIntervalMbOptS, flushIntervalMsOptS,
      numPackBulkOptS, numIoBulkOptS}, ""},

    {deleteWdev, "delete-wdev", {wdevParam}, {}, ""},

    {setCheckpointInterval, "set-checkpoint-interval", {wdevParam, intervalMsParam}, {}, ""},
    {getCheckpointInterval, "get-checkpoint-interval", {wdevParam}, {}, ""},
    {forceCheckpoint, "force-checkpoint", {wdevParam}, {}, ""},

    {setOldestLsid, "set-oldest-lsid", {wdevParam, lsidParam}, {}, ""},
    {getOldestLsid, "get-oldest-lsid", {wdevParam}, {}, ""},
    {getWrittenLsid, "get-written-lsid", {wdevParam}, {}, ""},
    {getPermanentLsid, "get-permanent-lsid", {wdevParam}, {}, ""},
    {getCompletedLsid, "get-completed-lsid", {wdevParam}, {}, ""},

    {getLogUsage, "get-log-usage", {wdevParam}, {}, ""},
    {getLogCapacity, "get-log-capacity", {wdevParam}, {}, ""},

    {isFlushCapable, "is-flush-capable", {wdevParam}, {}, ""},

    {resize, "resize", {wdevParam, sizeLbParam}, {}, ""},
    {resetWal, "reset-wal", {wdevParam}, {}, ""},
    {isLogOverflow, "is-log-overflow", {wdevParam}, {}, ""},
    {clearWal, "clear-wal", {wdevParam, timeoutSecParam}, {}, ""},

    {freeze, "freeze", {wdevParam, timeoutSecParam}, {}, ""},
    {melt, "melt", {}, {}, ""},
    {isFrozen, "is-flozen", {}, {}, ""},

    {getVersion, "get-version", {}, {}, ""},

    {defaultRunner, "help", {}, {}, "COMMAND"},
};


const Command &getCommand(const std::string &name)
{
    for (const Command &cmd : commandVec_) {
        if (cmd.name == name) return cmd;
    }
    throw cybozu::Exception(__func__) << "command not found" << name;
}

void help(const StrVec &params)
{
    if (params.empty()) {
        std::cout << generateUsage();
        return;
    }
    std::cout << getCommand(params[0]).longHelp();
}

void dispatch(const Option &opt)
{
    if (opt.cmd.empty()) {
        throw cybozu::Exception(__func__) << "specify command name.";
    }
    if (opt.cmd == "help") {
        help(opt.params);
        return;
    }
    for (const Command &cmd : commandVec_) {
        if (cmd.name == opt.cmd) {
            cmd.runner(opt);
            return;
        }
    }
    throw cybozu::Exception(__func__) << "command not found" << opt.cmd;
};

std::string generateUsage()
{
    std::stringstream ss;
    ss << "Command list:" << std::endl;
    for (const Command &cmd : commandVec_) {
        ss << cmd.shortHelp();
    }
    return ss.str();
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    walb::util::setLogSetting("-", opt.isDebug);
    dispatch(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdevc")
