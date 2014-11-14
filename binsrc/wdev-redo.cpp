/**
 * @file
 * @brief Redo logs from ldev on ddev.
 */
#include "cybozu/option.hpp"
#include "wdev_log.hpp"
#include "walb_log_redo.hpp"

using namespace walb;

struct Option
{
    std::string ldevPath;
    std::string ddevPath;
    bool isDiscard;
    bool isZeroDiscard;
    bool dontUseAio;
    bool isVerbose;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Redo logs from ldev on ddev.\n"
                           "Walb device will redo automatically.\n"
                           "This is just for test.\n");
        opt.appendBoolOpt(&isDiscard, "d", ": issue discard for discard logs.");
        opt.appendBoolOpt(&isZeroDiscard, "z", ": zero-clear for discard logs.");
        opt.appendBoolOpt(&dontUseAio, "noaio", ": do not use aio");
        opt.appendBoolOpt(&isVerbose, "v", ": verbose messages to stderr.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendParam(&ldevPath, "LDEV_PATH");
        opt.appendParam(&ddevPath, "DDEV_PATH");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (isDiscard && isZeroDiscard) {
            throw RT_ERR("Do not specify both -d and -z together.");
        }
    }
};

template <typename LogReader, typename BdevWriter>
void redo(const Option &opt)
{
    LogReader reader(opt.ldevPath);
    device::SuperBlock &super = reader.super();
    const uint32_t pbs = super.pbs();
    const uint32_t salt = super.salt();
    const uint64_t bgnLsid = super.getWrittenLsid();
    reader.reset(bgnLsid);

    if (opt.isVerbose) super.print();
    WlogRedoConfig cfg;
    cfg = {opt.ddevPath, opt.isVerbose, opt.isDiscard, opt.isZeroDiscard,
           pbs, salt, bgnLsid, true, false};

    bool isShrinked;
    LogPackHeader packH;
    uint64_t writtenLsid;
    if (opt.dontUseAio) {
        WlogApplyer<SimpleBdevWriter> applyer(cfg);
        isShrinked = applyer.run(reader, &writtenLsid);
        applyer.getPackHeader(packH);
    } else {
        WlogApplyer<AsyncBdevWriter> applyer(cfg);
        isShrinked = applyer.run(reader, &writtenLsid);
        applyer.getPackHeader(packH);
    }
    if (bgnLsid == writtenLsid) {
        // Redo is not required.
        return;
    }

    // Write the last logpack header if shrinked.
    cybozu::util::File ldevFile(opt.ldevPath, O_RDWR | O_DIRECT);
    assert(writtenLsid == packH.nextLogpackLsid());
    if (opt.isVerbose) {
        std::cout << "isShrinked: " << isShrinked << std::endl
                  << "writtenLsid: " << writtenLsid << std::endl
                  << "lastPackHeader: " << packH << std::endl;
    }
    if (isShrinked) {
        const uint64_t lsid = packH.header().logpack_lsid;
        if (packH.nRecords() == 0) {
            packH.setEnd();
            packH.updateChecksum();
            assert(writtenLsid == lsid);
        }
        ldevFile.pwrite(packH.rawData(), pbs, super.getOffsetFromLsid(lsid) * pbs);
    }

    // Update the superblock.
    device::SuperBlock super2;
    super2.copyFrom(super);
    super2.setWrittenLsid(writtenLsid);
    super2.write(ldevFile.fd());

    ldevFile.fdatasync();
    ldevFile.close();
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    if (opt.dontUseAio) {
        redo<device::SimpleWldevReader, SimpleBdevWriter>(opt);
    } else {
        redo<device::AsyncWldevReader, AsyncBdevWriter>(opt);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdev-redo")
