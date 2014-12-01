/**
 * @file
 * @brief lvm wrapper manager.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include "cybozu/option.hpp"
#include "lvm.hpp"
#include "util.hpp"
#include "walb_util.hpp"

struct Option : public cybozu::Option
{
    std::string vgName;
    std::string lvName;
    std::string tpName;
    std::string command;
    std::vector<std::string> args;
    uint64_t size;
    bool isWritable;
    Option() {
        appendOpt(&vgName, "", "vg", "volume group name");
        appendOpt(&lvName, "", "lv", "logical volume name");
        appendOpt(&tpName, "", "tp", "thinpool name");
        appendOpt(&size, 0, "s", "size");
        appendBoolOpt(&isWritable, "w", "take writable snapshot");
        appendParam(&command, "command name");
        appendParamVec(&args, "args", "command-specified arguments");
        appendHelp("h");

        std::string usage = cybozu::util::formatString(
            "Usage: lvm-mgr (-vg [volume group] (-lv [logical volume])) command [command arguments] [options]\n"
            "Command list:\n"
            "  listlv: print logical volume list\n"
            "  listvg: print volume group list\n"
            "  listsnap: list snapshots of a specified vg and lv.\n"
            "  create [lvname]: create a lv. specify -vg and -s. option: -tp.\n"
            "  snap [snapname]: create a snapshot with a name. specify -vg and -lv. option: -s, -w.\n"
            "  remove [name]: remove a lv or a snapshot. specify -vg.\n"
            "  resize [name]: resize a lv or a snapshot. specify -vg and -s.\n"
            "  parent [snapname]: get parent logical volume. specify -vg.\n"
            "  createpool [poolname]: create a lv for thin provsioning pool. specify -vg and -s.\n"
            "  exists-tp: check existance of a thin pool. specify -vg and -tp.\n"
            "Options:\n"
            "  -vg [volume group name]:\n"
            "  -lv [logical volume name]:\n"
            "  -tp [thinpool name]:\n"
            "  -s [size]: specify size. you can use suffix in [kmgtKMGT].\n"
            "             k/m/g/t means kilo/mega/giga/tera bytes.\n"
            "             K/M/G/T means kibi/mebi/gibi/tebi bytes.\n"
            "  -w: take writable snapshot.\n"
            );
        setUsage(usage);
    }

    void checkVgName() const {
        if (vgName.empty()) {
            throw std::runtime_error("Specify vg.");
        }
    }
    void checkTpName() const {
        if (tpName.empty()) {
            throw std::runtime_error("Specify tp.");
        }
    }
    void checkLvName() const {
        if (lvName.empty()) {
            throw std::runtime_error("Specify lv.");
        }
    }
    void checkSize() const {
        if (size == 0) {
            throw std::runtime_error("Specify positive size.");
        }
    }
    void checkNumArgs(size_t size) const {
        if (args.size() < size) {
            throw std::runtime_error("Specify enough arguments.");
        }
        for (size_t i = 0; i < size; i++) {
            if (args[i].empty()) {
                throw std::runtime_error("Do not specify empty string.");
            }
        }
    }
    uint64_t sizeLb() const {
        return size / cybozu::lvm::LBS;
    }
};

void dispatch(const Option &opt)
{
    if (opt.command.empty()) {
        throw std::runtime_error("Specify a command.");
    }

    if (opt.command == "listlv") {
        for (cybozu::lvm::Lv &lv : cybozu::lvm::listLv(opt.vgName)) {
            lv.print();
        }
    } else if (opt.command == "listvg") {
        for (cybozu::lvm::Vg &vg : cybozu::lvm::listVg()) {
            vg.print();
        }
    } else if (opt.command == "listsnap") {
        opt.checkLvName();
        cybozu::lvm::Lv lv = cybozu::lvm::locate(opt.vgName, opt.lvName);
        for (cybozu::lvm::Lv &snap : lv.snapshotList()) {
            snap.print();
        }
    } else if (opt.command == "create") {
        opt.checkVgName();
        opt.checkNumArgs(1);
        std::string lvName = opt.args[0];
        opt.checkSize();
        cybozu::lvm::Vg vg = cybozu::lvm::getVg(opt.vgName);
        cybozu::lvm::Lv lv;
        if (opt.tpName.empty()) {
            lv = vg.create(lvName, opt.sizeLb());
        } else {
            lv = vg.createThin(opt.tpName, lvName, opt.sizeLb());
        }
        lv.print();
        ::printf("created.\n");
    } else if (opt.command == "snap") {
        opt.checkVgName();
        opt.checkLvName();
        cybozu::lvm::Lv lv = cybozu::lvm::locate(opt.vgName, opt.lvName);
        if (lv.isSnapshot() || lv.attr().isTypeThinpool()) {
            throw std::runtime_error("Specify logical/thin volume.");
        }
        opt.checkNumArgs(1);
        std::string name = opt.args[0];
        cybozu::lvm::Lv snap;
        if (lv.isThinVolume()) {
            snap = lv.createSnapshot(name, opt.isWritable);
        } else {
            const uint64_t sizeLb =
                opt.sizeLb() == 0 ? uint64_t((double)(lv.sizeLb()) * 1.2) : opt.sizeLb();
            snap = lv.createSnapshot(name, opt.isWritable, sizeLb);
        }
        snap.print();
        ::printf("snapshot created.\n");
    } else if (opt.command == "remove") {
        opt.checkVgName();
        opt.checkNumArgs(1);
        std::string name = opt.args[0];
        cybozu::lvm::Lv lv = cybozu::lvm::locate(opt.vgName, name);
        lv.print();
        lv.remove();
        ::printf("removed.\n");
    } else if (opt.command == "resize") {
        opt.checkVgName();
        opt.checkNumArgs(1);
        opt.checkSize();
        std::string name = opt.args[0];
        cybozu::lvm::Lv lv = cybozu::lvm::locate(opt.vgName, name);
        lv.print();
        lv.resize(opt.sizeLb());
        ::printf("resized to %" PRIu64 " [logical block].\n", opt.sizeLb());
    } else if (opt.command == "parent") {
        opt.checkVgName();
        opt.checkNumArgs(1);
        std::string snapName = opt.args[0];
        cybozu::lvm::Lv lv = cybozu::lvm::locate(opt.vgName, snapName);
        if (!lv.isSnapshot()) {
            throw std::runtime_error("Specify a snapshot.");
        }
        lv.parent().print();
    } else if (opt.command == "createpool") {
        opt.checkVgName();
        opt.checkNumArgs(1);
        std::string poolName = opt.args[0];
        opt.checkSize();
        cybozu::lvm::Vg vg = cybozu::lvm::getVg(opt.vgName);
        cybozu::lvm::Lv lv = vg.createThinpool(poolName, opt.sizeLb());
        lv.print();
        ::printf("created.\n");
    } else if (opt.command == "exists-tp") {
        opt.checkVgName();
        opt.checkTpName();
        ::printf("%d\n", cybozu::lvm::tpExists(opt.vgName, opt.tpName));
    } else {
        ::printf("command %s is not supported now.\n", opt.command.c_str());
        opt.usage();
    }
}

int doMain(int argc, char *argv[])
{
    Option opt;
    opt.parse(argc, argv);
    dispatch(opt);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("lvm-mgr")
