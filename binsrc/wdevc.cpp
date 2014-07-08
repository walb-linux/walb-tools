/**
 * Walb device controller.
 */
#include <sstream>
#include <vector>
#include "cybozu/option.hpp"
#include "walb_util.hpp"
#include "wdev_log.hpp"

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
const Opt<uint64_t> lsid0Opt = {
    uint64_t(-1), "lb", "LSID : begin log sequence id (default: oldest_lsid)", false};
const Opt<uint64_t> lsid1Opt = {
    uint64_t(-1), "le", "LSID : end log sequence id (default: permanent_lsid)", false};
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
const OptS lsid0OptS = fromOpt(lsid0Opt);
const OptS lsid1OptS = fromOpt(lsid1Opt);
const OptS nameOptS = fromOpt(nameOpt);
const OptS noDiscardOptS = fromOpt(noDiscardOpt);

const Param ldevParam = {"LDEV", ": log device path"};
const Param ddevParam = {"DDEV", ": data device path"};
const Param wdevParam = {"WDEV", ": walb device path"};
const Param wldevParam = {"WLDEV", ": walb log device path"};
const Param sizeParam = {"SIZE", ""};
const Param lsidParam = {"LSID", ": log sequence id"};
const Param intervalMsParam = {"INTERVAL", "[ms]"};

struct Option
{
    cybozu::Option opt;

    std::string cmd;
    StrVec params;
    bool isDebug;

    uint32_t maxLogpackKb, maxPendingMb, minPendingMb,
        queueStopTimeoutMs, flushIntervalMb, flushIntervalMs,
        numPackBulk, numIoBulk;
    uint64_t lsid0, lsid1;
    std::string name;
    bool noDiscard;

    Option(int argc, char *argv[]) {
        opt.appendParam(&cmd, "command", "command name");
        opt.appendParamVec(&params, "remaining", "remaining parameters");
        opt.appendBoolOpt(&isDebug, "debug", "debug option");

        appendOpt(&maxLogpackKb, maxLogpackKbOpt);
        appendOpt(&maxPendingMb, maxPendingMbOpt);
        appendOpt(&minPendingMb, minPendingMbOpt);
        appendOpt(&queueStopTimeoutMs, queueStopTimeoutMsOpt);
        appendOpt(&flushIntervalMb, flushIntervalMbOpt);
        appendOpt(&flushIntervalMs, flushIntervalMsOpt);
        appendOpt(&numPackBulk, numPackBulkOpt);
        appendOpt(&numIoBulk, numIoBulkOpt);
        appendOpt(&lsid0, lsid0Opt);
        appendOpt(&lsid1, lsid1Opt);
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
};

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
    {
        cybozu::util::File ddevFile(ddev, O_RDONLY | O_DIRECT);
        ddevInfo.load(ddevFile.fd());
        ddevFile.close();
    }

    if (ldevInfo.pbs != ddevInfo.pbs) {
        throw cybozu::Exception(__func__)
            << "pbs size differ" << ldevInfo.pbs << ddevInfo.pbs;
    }
    if (!opt.noDiscard && cybozu::util::isDiscardSupported(fd)) {
        cybozu::util::issueDiscard(fd, 0, ldevInfo.sizeLb);
    }
    device::initWalbMetadata(fd, ldevInfo.pbs, ddevInfo.sizeLb, ldevInfo.sizeLb, opt.name);
    ldevFile.fdatasync();
    ldevFile.close();
}

void createWdev(const Option &opt)
{
    std::string ldev, ddev;
    cybozu::util::parseStrVec(opt.params, 0, 2, {&ldev, &ddev});
    // QQQ
}

void deleteWdev(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});
    // QQQ
}

void setCheckpointInterval(const Option &opt)
{
    std::string wdev, sizeStr;
    cybozu::util::parseStrVec(opt.params, 0, 2, {&wdev, &sizeStr});
    const uint64_t size = cybozu::atoi(sizeStr);
    // QQQ
}

void getCheckpointInterval(const Option &opt)
{
    std::string wdev;
    cybozu::util::parseStrVec(opt.params, 0, 1, {&wdev});

    // QQQ
}

void defaultRunner(const Option &opt)
{
    throw cybozu::Exception(__func__)
        << "not implemented yet" << opt.cmd;
}

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

    {defaultRunner, "cat-wldev", {wldevParam}, {lsid0OptS, lsid1OptS}, " > WLOG"},
    {defaultRunner, "show-wldev", {wldevParam}, {lsid0OptS, lsid1OptS}, ""},
    {defaultRunner, "show-wlog", {}, {lsid0OptS, lsid1OptS}, " < WLOG"},
    {defaultRunner, "redo-wlog", {ddevParam}, {lsid0OptS, lsid1OptS}, " < WLOG"},
    {defaultRunner, "redo", {ldevParam, ddevParam}, {}, ""},

    {defaultRunner, "set-oldest-lsid", {}, {}, ""},
    {defaultRunner, "get-oldest-lsid", {}, {}, ""},
    {defaultRunner, "get-written-lsid", {}, {}, ""},
    {defaultRunner, "get-permanent-lsid", {}, {}, ""},
    {defaultRunner, "get-completed-lsid", {}, {}, ""},

    {defaultRunner, "get-log-usage", {}, {}, ""},
    {defaultRunner, "get-log-capacity", {}, {}, ""},

    {defaultRunner, "is-flush-capable", {}, {}, ""},

    {defaultRunner, "resize", {}, {}, ""},
    {defaultRunner, "reset-wal", {}, {}, ""},
    {defaultRunner, "is-log-overflow", {}, {}, ""},

    {defaultRunner, "freeze", {}, {}, ""},
    {defaultRunner, "melt", {}, {}, ""},
    {defaultRunner, "is-flozen", {}, {}, ""},

    {defaultRunner, "get-version", {}, {}, ""},

    {defaultRunner, "search-valid-lsid", {}, {}, ""},
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
    dispatch(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdevc")
