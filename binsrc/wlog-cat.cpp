/**
 * @file
 * @brief Read walb log device and save as a wlog file.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "wdev_log.hpp"
#include "aio_util.hpp"
#include "linux/walb/walb.h"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    std::string wldevPath;
    std::string outPath;
    uint64_t bgnLsid;
    uint64_t endLsid;
    bool dontUseAio;
    bool dontShrink;
    bool isVerbose;
    bool isDebug;
    bool doForce;

    Option(int argc, char* argv[])
        : wldevPath()
        , outPath("-")
        , bgnLsid(0)
        , endLsid(-1)
        , dontUseAio(false)
        , isVerbose(false)
        , isDebug(false) {

        cybozu::Option opt;
        opt.setDescription("wlog-cat: extract wlog from a walb log device.");
        opt.appendOpt(&outPath, "-", "o", "PATH: output wlog path. '-' for stdout. (default: '-')");
        opt.appendOpt(&bgnLsid, 0, "b", "LSID: begin lsid to restore. (default: 0)");
        opt.appendOpt(&endLsid, uint64_t(-1), "e", "LSID: end lsid to restore. (default: 0xffffffffffffffff)");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio.");
        opt.appendBoolOpt(&dontShrink, "s", ": do not shrink logpack (ignoring log IO invalidness).");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose output to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");
        opt.appendBoolOpt(&doForce, "f", ": ignore oldest lsid in the superblock.");

        opt.appendParam(&wldevPath, "LOG_DEVICE_PATH");
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
void catWldev(const Option& opt)
{
    Reader reader(opt.wldevPath);
    device::SuperBlock &super = reader.super();
    const uint32_t pbs = super.pbs();
    const uint32_t salt = super.salt();
    uint64_t bgnLsid = opt.bgnLsid;
    const uint64_t oldestLsid = super.getOldestLsid();
    if (!opt.doForce && bgnLsid < oldestLsid) {
        bgnLsid = oldestLsid;
    }
    cybozu::util::File fileW;
    setupOutputFile(fileW, opt);
    WlogWriter writer(std::move(fileW));

    /* Create and write walblog header. */
    WlogFileHeader wh;
    wh.init(pbs, salt, super.getUuid(), bgnLsid, opt.endLsid);
    writer.writeHeader(wh);
    if (opt.isVerbose || opt.isDebug) std::cerr << wh << std::endl;

    /* Read and write each logpack. */
    reader.reset(bgnLsid);
    std::queue<AlignedArray> ioQ;
    uint64_t lsid = bgnLsid;
    LogStatistics logStat;
    logStat.init(bgnLsid, opt.endLsid);
    LogPackHeader packH(pbs, salt);
    bool isNotShrinked = true;
    while (lsid < opt.endLsid && isNotShrinked) {
        if (!readLogPackHeader(reader, packH, lsid)) break;
        isNotShrinked = readAllLogIos(reader, packH, ioQ, !opt.dontShrink);
        if (!isNotShrinked && packH.nRecords() == 0) break;
        writer.writePack(packH, std::move(ioQ));
        assert(ioQ.empty());
        if (opt.isDebug) std::cerr << packH << std::endl;
        if (opt.isVerbose) logStat.update(packH);
        lsid = packH.nextLogpackLsid();
    }
    writer.close();

    if (opt.isVerbose) std::cerr << logStat << std::endl;
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    if (opt.dontUseAio) {
        catWldev<device::SimpleWldevReader>(opt);
    } else {
        catWldev<device::AsyncWldevReader>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-cat")
