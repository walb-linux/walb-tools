/**
 * Search invalid logpack in a walb log device.
 */
#include "cybozu/exception.hpp"
#include "cybozu/option.hpp"
#include "wdev_log.hpp"
#include "walb_logger.hpp"

using namespace walb;

struct Option
{
    bool isDebug, force, isHeaderOnly;
    std::string ldevPath;
    uint64_t bgnLsid;
    uint64_t endLsid;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;

        opt.setDescription("search invalid logpack in a wldev image.");
        opt.appendParam(&ldevPath, "WLDEV_PATH", ": walb log device path or its image file path.");
        opt.appendOpt(&bgnLsid, 0, "bgn", ": begin lsid to search");
        opt.appendOpt(&endLsid, uint64_t(-1), "end", ": end lsid to search");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendBoolOpt(&force, "f", ": ignore oldest and written lsid for range specification.");
        opt.appendBoolOpt(&isHeaderOnly, "head", ": check logpack header only, not logpack IOs.");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

void readLogPackHeader(cybozu::util::File& file, const device::SuperBlock& super, uint64_t lsid, LogPackHeader& logh)
{
    const uint64_t pb = super.getOffsetFromLsid(lsid);
    const uint32_t pbs = super.getPhysicalBlockSize();
    file.lseek(pb * pbs);
    logh.rawReadFrom(file);
}

bool validateLogPackHeader(const LogPackHeader& logh, uint64_t lsid)
{
    if (logh.logpackLsid() != lsid) {
        LOGs.info() << "LOGPACK_HEADER_INVALID_LSID" << lsid << logh.logpackLsid();
        return false;
    }
    bool valid0 = logh.isValid(true);
    bool valid1 = logh.isValid(false);
    if (valid0) return true;
    if (valid1) {
        LOGs.info() << "LOGPACK_HEADER_INVALID_CHECKSUM" << "\n" << logh;
    } else {
        LOGs.info() << "LOGPACK_HEADER_INVALID_DATA" << lsid;
    }
    return false;
}

uint64_t readAndValidateLogPackIo(cybozu::util::File& file, const device::SuperBlock& super, const LogPackHeader& logh)
{
    const uint32_t pbs = super.getPhysicalBlockSize();
    assert(logh.isValid());
    AlignedArray buf;
    for (size_t i = 0; i < logh.nRecords(); i++) {
        const WlogRecord &rec = logh.record(i);
        if (!rec.hasDataForChecksum()) continue;
        const uint64_t pb = super.getOffsetFromLsid(rec.lsid);
        const size_t ioSize = rec.ioSizeLb() * LBS;
        buf.resize(ioSize);
        file.pread(buf.data(), ioSize, pb * pbs);
        const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), logh.salt());
        if (csum == rec.checksum) continue;
        LOGs.info() << "INVALID_LOGPACK_IO" << rec << csum;
    }
    return logh.nextLogpackLsid();
}

uint64_t searchValidLsid(cybozu::util::File& file, const device::SuperBlock& super, uint64_t lsid, uint64_t endLsid)
{
    const uint32_t pbs = super.getPhysicalBlockSize();
    AlignedArray buf(pbs);
    while (lsid < endLsid) {
        const uint64_t pb = super.getOffsetFromLsid(lsid);
        file.pread(buf.data(), buf.size(), pb * pbs);
        LogPackHeader logh(buf.data(), pbs, super.getLogChecksumSalt());
        if (logh.logpackLsid() == lsid && logh.isValid(true)) {
            LOGs.info() << "FOUND_VALID_LSID" << lsid;
            return lsid;
        }
        lsid++;
    }
    return lsid; // not found.
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
    uint64_t lsid = opt.force ? opt.bgnLsid : std::max(opt.bgnLsid, super.getOldestLsid());
    const uint64_t endLsid = opt.force ? opt.endLsid : std::min(opt.endLsid, super.getWrittenLsid());
    if (lsid >= endLsid) throw cybozu::Exception("bad lsid range") << lsid << endLsid;

    bool validMode = true;
    while (lsid < endLsid) {
        if (validMode) {
            LOGs.debug() << "read and check" << lsid;
            LogPackHeader logh(pbs, salt);
            readLogPackHeader(file, super, lsid, logh);
            if (!validateLogPackHeader(logh, lsid)) {
                validMode = false;
                continue;
            }
            if (opt.isHeaderOnly) {
                lsid = logh.nextLogpackLsid();
            } else {
                lsid = readAndValidateLogPackIo(file, super, logh);
            }
        } else {
            LOGs.debug() << "search valid lsid from" << lsid;
            lsid = searchValidLsid(file, super, lsid, endLsid);
            validMode = true;
        }
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("search-invalid-logpack")
