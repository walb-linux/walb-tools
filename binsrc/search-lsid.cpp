/**
 * Search lsid for all physical blocks.
 */
#include "cybozu/exception.hpp"
#include "cybozu/option.hpp"
#include "wdev_log.hpp"
#include "walb_logger.hpp"

using namespace walb;

struct Option
{
    bool isDebug;
    std::string ldevPath;
    uint64_t bgnAddr;
    uint64_t endAddr;
    std::vector<uint64_t> lsidV;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;

        opt.setDescription("search logpack header block in a wldev image.");
        opt.appendParam(&ldevPath, "WLDEV_PATH", ": walb log device path or its image file path.");
        opt.appendOpt(&bgnAddr, 0, "bgn", ": begin address to search [physical block]");
        opt.appendOpt(&endAddr, uint64_t(-1), "end", ": end address to search [physical block]");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendParamVec(&lsidV, "LIST_LIST", ": lsid list");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (lsidV.empty()) {
            throw cybozu::Exception("specify lsids.");
        }
        if (bgnAddr >= endAddr) {
            throw cybozu::Exception("bad bgn/end address.") << bgnAddr << endAddr;
        }
    }
};

void probe(const LogPackHeader& logh, uint64_t addr, uint64_t lsid)
{
    if (logh.logpackLsid() != lsid) return;
    std::cout << "addr[pb]: " << addr << std::endl;
    std::cout << "lsid: " << lsid << std::endl;
    const bool valid0 = logh.isValid(false);
    const bool valid1 = logh.isValid(true);
    if (valid1) {
        std::cout << "valid (w/i checksum)" << "\n" << logh << std::endl;
    } else if (valid0) {
        std::cout << "valid (w/o checksum)" << "\n" << logh << std::endl;
    } else {
        std::cout << "invalid" << std::endl;
    }
    ::fflush(::stdout);
}

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File file(opt.ldevPath, O_RDONLY | O_DIRECT);
    device::SuperBlock super;
    super.read(file.fd());

    const uint32_t pbs = super.getPhysicalBlockSize();
    const uint32_t salt = super.getLogChecksumSalt();
    const uint64_t metadataSize = super.getMetadataSize();
    const uint64_t ringBufSize = super.getRingBufferSize();

    uint64_t addr = std::max(opt.bgnAddr, metadataSize);
    const uint64_t endAddr = std::min(opt.endAddr, metadataSize + ringBufSize);
    AlignedArray buf(pbs);
    file.lseek(addr * pbs);
    while (addr < endAddr) {
        file.read(buf.data(), buf.size());
        const LogPackHeader logh(buf.data(), pbs, salt);
        for (uint64_t lsid : opt.lsidV) {
            probe(logh, addr, lsid);
        }
        addr++;
    };
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("search-lsid")
