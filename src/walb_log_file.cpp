#include "walb_log_file.hpp"

namespace walb {

void WlogFileHeader::init(uint32_t pbs, uint32_t salt, const cybozu::Uuid &uuid, uint64_t beginLsid, uint64_t endLsid)
{
    ::memset(data_.data(), 0, data_.size());
    header().sector_type = SECTOR_TYPE_WALBLOG_HEADER;
    header().version = WALB_LOG_VERSION;
    header().header_size = WALBLOG_HEADER_SIZE;
    header().log_checksum_salt = salt;
    header().logical_bs = LOGICAL_BLOCK_SIZE;
    header().physical_bs = pbs;
    uuid.copyTo(header().uuid);
    header().begin_lsid = beginLsid;
    header().end_lsid = endLsid;
}

bool WlogFileHeader::isValid(bool isChecksum) const
{
    CHECKd(header().sector_type == SECTOR_TYPE_WALBLOG_HEADER);
    CHECKd(header().version == WALB_LOG_VERSION);
    CHECKd(header().begin_lsid < header().end_lsid);
    if (isChecksum) {
        CHECKd(calcChecksum() == 0);
    }
    return true;
  error:
    return false;
}

std::string WlogFileHeader::str() const
{
    return cybozu::util::formatString(
        "wlog_h:"
        " ver %3u"
        " pbs %4u"
        " salt %08x"
        " uuid %s"
        " lsid %" PRIu64 " %" PRIu64 ""
        , header().version
        , header().physical_bs
        , header().log_checksum_salt
        , getUuid().str().c_str()
        , header().begin_lsid
        , header().end_lsid);
}


std::string WlogFileHeader::strDetail() const
{
    return cybozu::util::formatString(
        "wlog_header:\n"
        "  sector_type %d\n"
        "  version %u\n"
        "  header_size %u\n"
        "  log_checksum_salt %" PRIu32 " (%08x)\n"
        "  logical_bs %u\n"
        "  physical_bs %u\n"
        "  uuid %s\n"
        "  begin_lsid %" PRIu64 "\n"
        "  end_lsid %" PRIu64 ""
        , header().sector_type
        , header().version
        , header().header_size
        , header().log_checksum_salt
        , header().log_checksum_salt
        , header().logical_bs
        , header().physical_bs
        , getUuid().str().c_str()
        , header().begin_lsid
        , header().end_lsid);
}

void WlogReader::readHeader(WlogFileHeader &fileH)
{
    if (isReadHeader_) throw RT_ERR("Wlog file header has been read already.");
    isReadHeader_ = true;
    fileH.readFrom(fileR_);
    pbs_ = fileH.pbs();
    salt_ = fileH.salt();
    endLsid_ = fileH.beginLsid();
    packH_.init(pbs_, salt_);
}

bool WlogReader::readLog(WlogRecord& rec, AlignedArray& data, uint16_t *recIdxP)
{
    if (!fetchNextPackHeader()) return false;

    /* Copy to the record. */
    rec = packH_.record(recIdx_);

    /* Read to the data buffer. */
    if (!readLogIo(fileR_, packH_, recIdx_, data)) {
        throw cybozu::Exception(__func__) << "invalid log IO" << packH_.record(recIdx_);
    }

    if (recIdxP) *recIdxP = recIdx_;
    recIdx_++;
    return true;
}

bool WlogReader::fetchNextPackHeader()
{
    if (!isReadHeader_) {
        throw cybozu::Exception(__func__)
            << "You have not called readHeader() yet";
    }
    if (isEnd_) return false;
    if (isBegun_) {
        if (recIdx_ < packH_.nRecords()) return true;
    } else {
        isBegun_ = true;
    }

    if (!packH_.readFrom(fileR_) || packH_.isEnd()) {
        isEnd_ = true;
        return false;
    }
    recIdx_ = 0;
    endLsid_ = packH_.nextLogpackLsid();
    return true;
}


void WlogWriter::close()
{
    if (!isClosed_ && isWrittenHeader_) {
        writeEof();
        fileW_.close();
        isClosed_ = true;
    }
}

void WlogWriter::writeHeader(WlogFileHeader &fileH)
{
    if (isWrittenHeader_) throw RT_ERR("Wlog file header has been written already.");
    if (!fileH.isValid(false)) throw RT_ERR("Wlog file header is invalid.");
    fileH.writeTo(fileW_);
    isWrittenHeader_ = true;
    pbs_ = fileH.pbs();
    salt_ = fileH.salt();
    beginLsid_ = fileH.beginLsid();
    lsid_ = beginLsid_;
}

void WlogWriter::writePackIo(const AlignedArray &data)
{
    if (data.size() == 0) return;
    assert(data.size() % pbs_ == 0);
    fileW_.write(data.data(), data.size());
    const size_t ioSizePb = data.size() / pbs_;
    lsid_ += ioSizePb;
}

void WlogWriter::writePack(const LogPackHeader &header, std::queue<AlignedArray> &&ioQ)
{
    checkHeader(header);
    if (ioQ.size() != header.nRecordsHavingData()) {
        throw cybozu::Exception(__func__)
            << "bad queue size" << ioQ.size() << header.nRecordsHavingData();
    }
    writePackHeader(header);
    while (!ioQ.empty()) {
        writePackIo(ioQ.front());
        ioQ.pop();
    }
    assert(lsid_ == header.nextLogpackLsid());
}

void WlogWriter::checkHeader(const LogPackHeader &header) const
{
    if (!header.isValid()) {
        throw RT_ERR("Logpack header invalid.");
    }
    if (pbs_ != header.pbs()) {
        throw RT_ERR("pbs differs %u %u.", pbs_, header.pbs());
    }
    if (salt_ != header.salt()) {
        throw RT_ERR("salt differs %" PRIu32 " %" PRIu32 "."
                     , salt_, header.salt());
    }
    if (lsid_ != header.logpackLsid()) {
        throw RT_ERR("logpack lsid differs %" PRIu64 " %" PRIu64 "."
                     , lsid_, header.logpackLsid());
    }
}

void LogFile::skip(size_t size)
{
    if (seekable_) {
        lseek(size, SEEK_CUR);
        return;
    }
    AlignedArray buf(4096);
    while (size > 0) {
        const size_t s = std::min(buf.size(), size);
        read(buf.data(), s);
        size -= s;
    }
}

} // namespace walb
