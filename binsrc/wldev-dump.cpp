/**
 * @file
 * @brief Dump data in arbitral lsid range from wldev or ldev device.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio_util.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    std::string wldevPath;
    uint64_t bgnLsid;
    uint64_t endLsid;
    std::string outPath;
    bool dontUseAio;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("wldev-dump: dump lsid range from a walb log device.");

        opt.appendParam(&wldevPath, "LOG_DEVICE_PATH");
        opt.appendParam(&bgnLsid, "BEGIN_LSID");
        opt.appendParam(&endLsid, "END_LSID");

        opt.appendOpt(&outPath, "-", "o", "PATH: output dump file path. '-' for stdout. (default: '-')");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (bgnLsid >= endLsid) {
            throw RT_ERR("bgnLsid must be < endLsid.");
        }
    }
    bool isOutStdout() const { return outPath == "-"; }
};

void setupOutputFile(cybozu::util::File &fileW, const Option &opt)
{
    if (opt.isOutStdout()) {
        fileW.setFd(1);
    } else {
        fileW.open(opt.outPath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    }
}

template <typename Reader>
void dumpWldev(const Option& opt)
{
    Reader reader(opt.wldevPath);
    device::SuperBlock &super = reader.super();
    const uint32_t pbs = super.pbs();
    cybozu::util::File fileW;
    setupOutputFile(fileW, opt);
    AlignedArray buf(pbs);
    uint64_t lsid = opt.bgnLsid;
    reader.reset(lsid);
    while (lsid < opt.endLsid) {
        reader.read(buf.data(), buf.size());
        fileW.write(buf.data(), buf.size());
        lsid++;
    }
    fileW.fsync();
    fileW.close();
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    if (opt.dontUseAio) {
        dumpWldev<device::SimpleWldevReader>(opt);
    } else {
        dumpWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wldev-dump")
