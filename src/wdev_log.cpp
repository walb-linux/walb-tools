#include "wdev_log.hpp"

namespace walb {
namespace device {


uint64_t SuperBlock::getOffsetFromLsid(uint64_t lsid) const
{
    if (lsid == INVALID_LSID) {
        throw RT_ERR("Invalid lsid.");
    }
    uint64_t s = getRingBufferSize();
    if (s == 0) {
        throw RT_ERR("Ring buffer size must not be 0.");
    }
    return (lsid % s) + getRingBufferOffset();
}


void SuperBlock::format(uint32_t pbs, uint64_t ddevLb, uint64_t ldevLb, const std::string &name)
{
    init(pbs, true);
    super()->sector_type = SECTOR_TYPE_SUPER;
    super()->version = WALB_LOG_VERSION;
    super()->logical_bs = LBS;
    super()->physical_bs = pbs;
    super()->metadata_size = 0; // deprecated.
    cybozu::util::Random<uint32_t> rand;
    cybozu::Uuid uuid;
    uuid.setRand(rand);
    setUuid(uuid);
    super()->log_checksum_salt = rand.get32();
    super()->ring_buffer_size = ::addr_pb(pbs, ldevLb) - ::get_ring_buffer_offset(pbs);
    super()->oldest_lsid = 0;
    super()->written_lsid = 0;
    super()->device_size = ddevLb;
    if (name.empty()) {
        super()->name[0] = '\0';
    } else {
        snprintf(super()->name, DISK_NAME_LEN, "%s", name.c_str());
    }
    if (!isValid(false)) {
        cybozu::Exception(__func__) << "invalid super block.";
    }
}


void SuperBlock::read(int fd)
{
    cybozu::util::File file(fd);
    init(cybozu::util::getPhysicalBlockSize(fd), false);
    file.pread(data_.data(), pbs_, offset_ * pbs_);
    if (!isValid()) {
        throw RT_ERR("super block is invalid.");
    }
}


void SuperBlock::write(int fd)
{
    updateChecksum();
    if (!isValid()) {
        throw RT_ERR("super block is invalid.");
    }
    cybozu::util::File file(fd);
    file.pwrite(data_.data(), pbs_, offset_ * pbs_);
}


std::string SuperBlock::str() const
{
    return cybozu::util::formatString(
        "sectorType: %u\n"
        "version: %u\n"
        "checksum: %u\n"
        "lbs: %u\n"
        "pbs: %u\n"
        "metadataSize: %u\n"
        "salt: %u\n"
        "name: %s\n"
        "ringBufferSize: %" PRIu64 "\n"
        "oldestLsid: %" PRIu64 "\n"
        "writtenLsid: %" PRIu64 "\n"
        "deviceSize: %" PRIu64 "\n"
        "ringBufferOffset: %" PRIu64 "\n"
        "uuid: %s\n"
        , getSectorType()
        , getVersion()
        , getChecksum()
        , getLogicalBlockSize()
        , pbs()
        , getMetadataSize()
        , salt()
        , getName()
        , getRingBufferSize()
        , getOldestLsid()
        , getWrittenLsid()
        , getDeviceSize()
        , getRingBufferOffset()
        , getUuid().str().c_str());
}


void SuperBlock::init(uint32_t pbs, bool doZeroClear)
{
    if (!::is_valid_pbs(pbs)) {
        throw cybozu::Exception(__func__) << "invalid pbs";
    }
    pbs_ = pbs;
    offset_ = get1stSuperBlockOffsetStatic(pbs);
    data_.resize(pbs, doZeroClear);
}


bool SuperBlock::isValid(bool isChecksum) const
{
    if (::is_valid_super_sector_raw(super(), pbs_) == 0) {
        return false;
    }
    if (isChecksum) {
        return true;
    } else {
        return ::checksum(data_.data(), pbs_, 0) == 0;
    }
}


void AsyncWldevReader::reset(uint64_t lsid)
{
    /* Wait for all pending aio(s). */
    while (!ioQ_.empty()) {
        waitForIo();
    }
    /* Reset indicators. */
    aheadLsid_ = lsid;
    ringBuf_.reset();
}


void AsyncWldevReader::read(void *data, size_t size)
{
    char *ptr = (char *)data;
    while (size > 0) {
        prepareReadableData();
        const size_t s = ringBuf_.read(ptr, size);
        ptr += s;
        size -= s;
        readAhead();
    }
}


void AsyncWldevReader::skip(size_t size)
{
    while (size > 0) {
        prepareReadableData();
        const size_t s = ringBuf_.skip(size);
        size -= s;
        readAhead();
    }
}


bool AsyncWldevReader::prepareAheadIo()
{
    if (aio_.isQueueFull()) return false;
    const size_t ioSize = decideIoSize();
    if (ioSize == 0) return false;
    char *ptr = ringBuf_.prepare(ioSize);

    const uint64_t offPb = super_.getOffsetFromLsid(aheadLsid_);
    const size_t ioPb = ioSize / pbs_;
#ifdef DEBUG
    const uint64_t offPb1 = super_.getOffsetFromLsid(aheadLsid_ + ioPb);
    assert(offPb < offPb1 || offPb1 == super_.getRingBufferOffset());
#endif
    const uint64_t off = offPb * pbs_;
    const uint32_t aioKey = aio_.prepareRead(off, ioSize, ptr);
    assert(aioKey > 0);
    aheadLsid_ += ioPb;
    ioQ_.push({aioKey, ioSize});
    return true;
}


size_t AsyncWldevReader::decideIoSize() const
{
    if (ringBuf_.getFreeSize() < maxIoSize_) {
        /* There is not enough free space. */
        return 0;
    }
    size_t ioSize = maxIoSize_;
    /* Log device ring buffer edge. */
    uint64_t s = super_.getRingBufferSize();
    s = s - aheadLsid_ % s;
    ioSize = std::min<uint64_t>(ioSize / pbs_, s) * pbs_;
    /* Ring buffer available size. */
    return std::min(ioSize, ringBuf_.getAvailableSize());
}


void initWalbMetadata(
    int fd, uint32_t pbs, uint64_t ddevLb, uint64_t ldevLb, const std::string &name)
{
    assert(fd > 0);
    assert(pbs > 0);
    assert(ddevLb > 0);
    assert(ldevLb > 0);
    // name can be empty.

    SuperBlock super;
    super.format(pbs, ddevLb, ldevLb, name);
    super.updateChecksum();
    super.write(fd);
}


void fillZeroToLdev(const std::string& wdevName, uint64_t bgnLsid, uint64_t endLsid)
{
    assert(bgnLsid < endLsid);
    const std::string ldevPath = getUnderlyingLogDevPath(wdevName);
    cybozu::util::File file(ldevPath, O_RDWR | O_DIRECT);
    SuperBlock super;
    super.read(file.fd());
    const uint32_t pbs = super.pbs();
    const uint64_t rbOff = super.getRingBufferOffset();
    const uint64_t rbSize = super.getRingBufferSize();
    LsidSet lsids;
    getLsidSet(wdevName, lsids);
    if (endLsid > lsids.oldest) {
        throw cybozu::Exception("bad endLsid") << endLsid << lsids.oldest;
    }
    if (rbSize < lsids.latest && bgnLsid < lsids.latest - rbSize) {
        throw cybozu::Exception("bad bgnLsid") << bgnLsid << lsids.latest;
    }

    size_t bufPb = MEBI / pbs;
    AlignedArray buf(bufPb * pbs);
    ::memset(buf.data(), 0, buf.size());

    uint64_t lsid = bgnLsid;
    while (lsid < endLsid) {
        const uint64_t tmpPb0 = endLsid - lsid;
        const uint64_t tmpPb1 = rbSize - (lsid % rbSize); // ring buffer end.
        const uint64_t tmpPb2 = std::min<uint64_t>(tmpPb0, tmpPb1);
        const size_t minPb = std::min<uint64_t>(bufPb, tmpPb2);
        const uint64_t offPb = lsid % rbSize + rbOff;
        file.pwrite(buf.data(), minPb * pbs, offPb * pbs);
        lsid += minPb;
    }
}


} // namespace device
} // namespace walb
