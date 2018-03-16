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
#include "version.hpp"
#include "description.hpp"
#include "linux/walb/ioctl.h"

using namespace walb;

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

struct CommandBase {
    virtual void setup(cybozu::Option&) {}
    virtual int run() { return 0; }
};

void appendOptName(cybozu::Option& opt, std::string& name)
{
    opt.appendOpt(&name, "", "n", "NAME : walb device name (default: decided automatically)");
}
void appendParamLdev(cybozu::Option& opt, std::string& ldev)
{
    opt.appendParam(&ldev, "LDEV", ": log device path");
}
void appendParamDdev(cybozu::Option& opt, std::string& ddev)
{
    opt.appendParam(&ddev, "DDEV", ": data device path");
}
void appendParamWdev(cybozu::Option& opt, std::string& wdev)
{
    opt.appendParam(&wdev, "WDEV", ": walb device path");
}
void appendParamTimeoutSec(cybozu::Option& opt, size_t& timeoutSec)
{
    opt.appendParamOpt(&timeoutSec, 0, "TIMEOUT", "[sec] : 0 means no timeout. (default: 0)");
}

struct FormatLdev : CommandBase {
    std::string ldev;
    std::string ddev;
    std::string name;
    bool noDiscard;
    void setup(cybozu::Option& opt) override {
        appendParamLdev(opt, ldev);
        appendParamDdev(opt, ddev);
        appendOptName(opt, name);
        opt.appendBoolOpt(&noDiscard, "nd", ": disable discard IOs");
    }
    int run() override {
        cybozu::FilePath ldevPath(ldev);
        cybozu::FilePath ddevPath(ldev);
        if (!ldevPath.stat().isBlock()) {
            LOGs.error() << "ldev is not block device" << ldev;
            return 1;
        }
        if (!ddevPath.stat().isBlock()) {
            LOGs.error() << "ddev is not block device" << ddev;
            return 1;
        }
        BdevInfo ldevInfo, ddevInfo;
        cybozu::util::File ldevFile(ldev, O_RDWR | O_DIRECT);
        int fd = ldevFile.fd();
        ldevInfo.load(fd);
        ddevInfo.load(ddev);
        verifyPbs(ldevInfo, ddevInfo, __func__);
        if (!noDiscard && cybozu::util::isDiscardSupported(fd)) {
            cybozu::util::issueDiscard(fd, 0, ldevInfo.sizeLb);
        }
        device::initWalbMetadata(fd, ldevInfo.pbs, ddevInfo.sizeLb, ldevInfo.sizeLb, name);
        {
            device::SuperBlock super;
            super.read(fd);
            std::cout << super << std::endl;
        }
        ldevFile.fdatasync();
        ldevFile.close();
        LOGs.debug() << "format-ldev done";
        return 0;
    }
} g_formatLdev;

struct CreateWdev : CommandBase {
    std::string ldev;
    std::string ddev;
    std::string name;
    struct walb_start_param sParam;

    void setup(cybozu::Option& opt) override {
        appendParamLdev(opt, ldev);
        appendParamDdev(opt, ddev);
        appendOptName(opt, name);

        const struct Uint32Opt {
            uint32_t *ptr;
            uint32_t defaultVal;
            const char *opt;
            const char *doc;
        } uint32OptTbl[] = {
            { &sParam.max_logpack_kb, 32, "maxl", "SIZE : max logpack size [KiB]" },
            { &sParam.max_pending_mb, 32, "maxp", "SIZE : max pending size [MiB]" },
            { &sParam.min_pending_mb, 16, "minp", "SIZE : min pending size [MiB]" },
            { &sParam.queue_stop_timeout_ms, 100, "qp", "PERIOD : queue stopping period [ms]" },
            { &sParam.log_flush_interval_mb, 16, "fs", "SIZE : flush interval size [MiB]" },
            { &sParam.log_flush_interval_ms, 100, "fp", "PERIOD : flush interval period [ms]" },
            { &sParam.n_pack_bulk, 128, "bp", "SIZE : number of packs in bulk" },
            { &sParam.n_io_bulk, 1024, "bi", "SIZE : numer of IOs in bulk" },
        };
        for (const Uint32Opt& p : uint32OptTbl) {
            opt.appendOpt(p.ptr, p.defaultVal, p.opt, p.doc);
        }
    }
    int run() override {
        struct walb_start_param u2k; // userland -> kernel.
        struct walb_start_param k2u; // kernel -> userland.
        struct walb_ctl ctl;
        memset(&ctl, 0, sizeof(ctl));
        ctl.command = WALB_IOCTL_START_DEV,
            ctl.u2k.wminor = WALB_DYNAMIC_MINOR;
        ctl.u2k.buf_size = sizeof(struct walb_start_param);
        ctl.u2k.buf = (void *)&u2k;
        ctl.k2u.buf_size = sizeof(struct walb_start_param);
        ctl.k2u.buf = (void *)&k2u;
        // Check parameters.
        if (!::is_walb_start_param_valid(&sParam)) {
            LOGs.error() << "invalid start param." << sParam;
            return 1;
        }
        // Check underlying block devices.
        BdevInfo ldevInfo, ddevInfo;
        ldevInfo.load(ldev);
        ddevInfo.load(ddev);
        verifyPbs(ldevInfo, ddevInfo, __func__);
        // Make ioctl data.
        ::memcpy(&u2k, &sParam, sizeof(u2k));
        if (name.empty()) {
            u2k.name[0] = '\0';
        } else {
            ::snprintf(u2k.name, DISK_NAME_LEN, "%s", name.c_str());
        }
        ctl.u2k.lmajor = ldevInfo.stat.majorId();
        ctl.u2k.lminor = ldevInfo.stat.minorId();
        ctl.u2k.dmajor = ddevInfo.stat.majorId();
        ctl.u2k.dminor = ddevInfo.stat.minorId();

        invokeWalbctlIoctl(ctl, __func__);
        assert(::strnlen(k2u.name, DISK_NAME_LEN) < DISK_NAME_LEN);

        ::printf("name %s\n"
                 "major %u\n"
                 "minor %u\n"
                 , k2u.name, ctl.k2u.wmajor, ctl.k2u.wminor);
        LOGs.debug() << "create-wdev done";
        return 0;
    }
} g_createWdev;

struct DeleteWdev : CommandBase {
    std::string wdev;
    bool force;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
        opt.appendBoolOpt(&force, "f", "force to delete.");
    }
    int run() override {
        struct walb_ctl ctl;
        memset(&ctl, 0, sizeof(ctl));
        ctl.command = WALB_IOCTL_STOP_DEV;
        ctl.val_int = (force ? 1 : 0);

        BdevInfo wdevInfo;
        wdevInfo.load(wdev);
        ctl.u2k.wmajor = wdevInfo.stat.majorId();
        ctl.u2k.wminor = wdevInfo.stat.minorId();
        invokeWalbctlIoctl(ctl, __func__);

        LOGs.debug() << "delete-wdev done";
        return 0;
    }
} g_deleteWdev;

struct ListWdev : CommandBase {
    void setup(cybozu::Option&) override {}
    int run() override {
        uint minor[2] = {0, uint(-1)};
        struct walb_disk_data ddata[32];
        struct walb_ctl ctl;
        memset(&ctl, 0, sizeof(ctl));
        ctl.command = WALB_IOCTL_LIST_DEV;
        ctl.u2k.wminor = WALB_DYNAMIC_MINOR;
        ctl.u2k.buf_size = sizeof(minor);
        ctl.u2k.buf = (void *)&minor[0];
        ctl.k2u.buf_size = sizeof(ddata);
        ctl.k2u.buf = (void *)&ddata[0];
        for (;;) {
            invokeWalbctlIoctl(ctl, __func__);
            const size_t nr = ctl.val_int;
            if (nr == 0) break;
            for (size_t i = 0; i < nr; i++) {
                struct walb_disk_data &d = ddata[i];
                d.name[DISK_NAME_LEN - 1] = '\0';
                ::printf("%u %u %s\n", d.major, d.minor, d.name);
                minor[0] = d.minor + 2; // starting minor id for next ioctl.
                (void)minor[0];
            }
        }
        LOGs.debug() << "list done";
        return 0;
    }
} g_listWdev;

struct SetCheckpointInterval : CommandBase {
    std::string wdev;
    uint32_t intervalMs;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
        opt.appendParam(&intervalMs, "interval msec");
    }
    int run() override {
        device::setValueByIoctl<uint32_t>(
            wdev, WALB_IOCTL_SET_CHECKPOINT_INTERVAL, intervalMs);
        LOGs.debug() << "set-checkpoint-interval done";
        return 0;
    }
} g_setCheckpointInterval;

struct GetCheckpointInterval : CommandBase {
    std::string wdev;
    uint32_t intervalMs;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
    }
    int run() override {
        const uint32_t intervalMs =
            device::getValueByIoctl<uint32_t>(wdev, WALB_IOCTL_GET_CHECKPOINT_INTERVAL);
        std::cout << intervalMs << std::endl;
        LOGs.debug() << "get-checkpoint-interval done";
        return 0;
    }
} g_getCheckpointInterval;

struct ForceCheckpoint : CommandBase {
    std::string wdev;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
    }
    int run() override {
        device::takeCheckpoint(wdev);
        LOGs.debug() << "force-checkpoint done";
        return 0;
    }
} g_forceCheckpoint;

struct SetOldestLsid : CommandBase {
    std::string wdev;
    uint64_t lsid;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
        opt.appendParam(&lsid, "LSID", ": lsid");
    }
    int run() override {
        device::setOldestLsid(wdev, lsid);
        LOGs.debug() << "set-oldest-lsid done";
        return 0;
    }
} g_setOldestLsid;

struct GetLsid : CommandBase {
    std::string wdev;
    int getId_;
    GetLsid(int getId) : getId_(getId) {}

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
    }
    int run() override {
        std::cout << device::getLsid(wdev, getId_) << std::endl;
        return 0;
    }
} g_getOldestLsid(WALB_IOCTL_GET_OLDEST_LSID),
                     g_getWrittenLsid(WALB_IOCTL_GET_WRITTEN_LSID),
                     g_getPermanentLsid(WALB_IOCTL_GET_PERMANENT_LSID),
                     g_getCompletedLsid(WALB_IOCTL_GET_COMPLETED_LSID);

template<class R>
struct GetWdevVal : CommandBase {
    std::string wdev;
    R (*f_)(const std::string&);
    explicit GetWdevVal(R (*f)(const std::string&)) : f_(f) {}
    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
    }
    int run() override {
        std::cout << f_(wdev) << std::endl;
        return 0;
    }
};

GetWdevVal<uint64_t> g_getLogUsage(&device::getLogUsagePb);
GetWdevVal<uint64_t> g_getLogCapacity(&device::getLogCapacityPb);
GetWdevVal<bool> g_isFlushCapable(&device::isFlushCapable);
GetWdevVal<bool> g_isLogOverflow(&device::isOverflow);

bool isFrozenByIoctl(const std::string& wdev)
{
    return device::getValueByIoctl<int>(wdev, WALB_IOCTL_IS_FROZEN);
}

GetWdevVal<bool> g_isFrozen(&isFrozenByIoctl);

struct Resize : CommandBase {
    std::string wdev;
    uint64_t sizeLb;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
        opt.appendParamOpt(&sizeLb, 0, "SIZE");
    }
    int run() override {
        device::resize(wdev, sizeLb);
        return 0;
    }
} g_resize;

struct ResetWal : CommandBase {
    std::string wdev;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
    }
    int run() override {
        device::resetWal(wdev);
        LOGs.debug() << "reset-wal done";
        return 0;
    }
} g_resetWal;

struct ClearWal : CommandBase {
    std::string wdev;
    size_t timeoutSec;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
        appendParamTimeoutSec(opt, timeoutSec);
    }
    int run() override {
        const size_t intervalMs = 500;
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
        return 0;
    }
} g_clearWal;

struct Freeze : CommandBase {
    std::string wdev;
    size_t timeoutSec;

    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
        appendParamTimeoutSec(opt, timeoutSec);
    }
    int run() override {
        device::setValueByIoctl<uint32_t>(wdev, WALB_IOCTL_FREEZE, timeoutSec);
        LOGs.debug() << "freeze done";
        return 0;
    }
} g_freeze;

struct Melt : CommandBase {
    std::string wdev;
    void setup(cybozu::Option& opt) override {
        appendParamWdev(opt, wdev);
    }
    int run() override {
        const int dummy = 0;
        device::setValueByIoctl<int>(wdev, WALB_IOCTL_MELT, dummy);

        LOGs.debug() << "melt done";
        return 0;
    }
} g_melt;

struct GetVersion : CommandBase {
    void setup(cybozu::Option&) override {
    }
    int run() override {
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
        return 0;
    }
} g_getVersion;

struct CommandInfo {
    const char *cmdName;
    CommandBase *cmd;
    const char *help;
} g_cmdTbl[] = {
    { "format-ldev", &g_formatLdev, "Format a log device." },
    { "create-wdev", &g_createWdev, "Create a walb device." },
    { "delete-wdev", &g_deleteWdev, "Delete a walb device." },
    { "list", &g_listWdev, "List all walb device in the system." },
    { "set-checkpoint-interval", &g_setCheckpointInterval, "Set checkpoint interval." },
    { "get-checkpoint-interval", &g_getCheckpointInterval, "Get checkpoint interval." },
    { "force-checkpoint", &g_forceCheckpoint, "Execute checkpointing immediately." },
    { "set-oldest-lsid", &g_setOldestLsid, "Set oldest lsid (to clear wlogs)" },
    { "get-oldest-lsid", &g_getOldestLsid, "Get oldest lsid." },
    { "get-written-lsid", &g_getWrittenLsid, "Get written lsid." },
    { "get-permanent-lsid", &g_getPermanentLsid, "Get permanent lsid." },
    { "get-completed-lsid", &g_getCompletedLsid, "Get complete lsid." },
    { "get-log-usage", &g_getLogUsage, "Get log usage [physical block]." },
    { "get-log-capacity", &g_getLogCapacity, "Get log capacity [physical block]." },
    { "is-flush-capable", &g_isFlushCapable, "Check flush request capability." },
    { "resize", &g_resize, "Resize a walb device (only growing is supported)." },
    { "is-log-overflow", &g_isLogOverflow, "Check log overflow." },
    { "reset-wal", &g_resetWal, "Reset log device to recover from wlog overflow." },
    { "clear-wal", &g_clearWal, "Clear all wlogs." },
    { "freeze", &g_freeze, "Freeze temporarily a walb device." },
    { "is-flozen", &g_isFrozen, "Check a walb device is frozen or not." },
    { "melt", &g_melt, "Melt a freezed walb device." },
    { "get-version", &g_getVersion, "Get version of the walb kernel device driver." },
};

CommandInfo* getCommand(const std::string& cmd)
{
    for (CommandInfo& ci : g_cmdTbl) {
        if (cmd == ci.cmdName) return &ci;
    }
    return nullptr;
}

class Dispatcher {
    cybozu::Option opt1;
    cybozu::Option opt2;
    bool isDebug;
    CommandInfo *ci;
    void setup1stOption() {
        const std::string desc = getDescription("walb device controller");
        opt1.setDescription(desc);
        std::string usage =
            "usage: wdevc [<opt>] <command> [<args>] [<opt>]\n\n"
            "Command list:\n";
        size_t maxLen = 0;
        for (const CommandInfo& ci : g_cmdTbl) {
            maxLen = std::max(maxLen, ::strlen(ci.cmdName));
        }
        maxLen++;
        for (const CommandInfo& ci : g_cmdTbl) {
            opt1.appendDelimiter(ci.cmdName);
            usage += cybozu::util::formatString("  %-*s%s\n", maxLen, ci.cmdName, ci.help);
        }
        opt1.appendBoolOpt(&isDebug, "debug");
        opt1.appendHelp("h");
        usage += "\n""Option list:\n";
        usage += "  -debug  show debug messages to stderr.\n";
        usage += "  -h      show this help.\n\n";
        usage += "See wdevc <command> -h for more information on a specific command.";
        opt1.setUsage(usage);
    }
    int parse1(int argc, char *argv[]) {
        setup1stOption();
        if (!opt1.parse(argc, argv)) return 0;
        const int cmdPos = opt1.getNextPositionOfDelimiter();
        if (cmdPos == 0) return 0;
        const std::string cmdName = argv[cmdPos - 1];
        ci = getCommand(cmdName);
        if (ci == nullptr) return 0;
        ci->cmd->setup(opt2);
        opt2.appendHelp("h");
        return cmdPos;
    }
public:
    int run(int argc, char *argv[])
        {
            const int cmdPos = parse1(argc, argv);
            if (cmdPos == 0) {
                opt1.usage();
                return 1;
            }
            const char *cmdName = argv[cmdPos - 1];
            if (!opt2.parse(argc, argv, cmdPos)) {
                opt2.usage();
                return 1;
            }
            if (isDebug) {
                std::cerr << "common options" << std::endl;
                std::cerr << opt1 << std::endl;
                std::cerr << "options for " << cmdName << std::endl;
                std::cerr << opt2;
            }
            walb::util::setLogSetting("-", isDebug);
            return ci->cmd->run();
        }
};

int doMain(int argc, char* argv[])
{
    Dispatcher disp;
    return disp.run(argc, argv);
}

DEFINE_ERROR_SAFE_MAIN("wdevc")
