#include "walb_log_net.hpp"

namespace walb {

void WlogSender::process(CompressedData& cd, bool doCompress) try
{
    if (doCompress) cd.compress();
    ctrl_.next();
    cd.send(packet_);
} catch (std::exception& e) {
    try {
        packet::StreamControl(packet_.sock()).error();
    } catch (...) {}
    logger_.error() << "WlogSender:process" << e.what();
}

/**
 * Send padding IO data also so that the receiver can create wlog file for debug purpose.
 */
void WlogSender::pushIo(const LogPackHeader &header, uint16_t recIdx, const char *data)
{
    verifyPbsAndSalt(header);
    const WlogRecord &rec = header.record(recIdx);
    if (!rec.hasData()) return;

    const size_t size = rec.ioSizePb(pbs_) * pbs_;
    CompressedData cd;
    cd.compressFrom(data, size);
    process(cd, false);
}

void WlogSender::verifyPbsAndSalt(const LogPackHeader &header) const
{
    if (header.pbs() != pbs_) {
        throw cybozu::Exception(NAME()) << "invalid pbs" << pbs_ << header.pbs();
    }
    if (header.salt() != salt_) {
        throw cybozu::Exception(NAME()) << "invalid salt" << salt_ << header.salt();
    }
}


bool WlogReceiver::process(CompressedData& cd)
{
    if (ctrl_.isNext()) {
        cd.recv(packet_);
        cd.uncompress();
        ctrl_.reset();
        return true;
    }
    if (ctrl_.isError()) {
        throw cybozu::Exception("WlogReceiver:process:isError");
    }
    return false;
}

bool WlogReceiver::popHeader(LogPackHeader &header)
{
    const char *const FUNC = __func__;
    CompressedData cd;
    if (!process(cd)) {
        return false;
    }
    assert(!cd.isCompressed());
    if (cd.rawSize() != pbs_) {
        throw cybozu::Exception(FUNC) << "invalid pack header size" << cd.rawSize() << pbs_;
    }
    header.copyFrom(cd.rawData(), pbs_);
    if (header.isEnd()) throw cybozu::Exception(FUNC) << "end header is not permitted";
    return true;
}

/**
 * data will be uncompressed IO data with padding (ioSizePb * pbs).
 */
void WlogReceiver::popIo(const WlogRecord &rec, AlignedArray &data)
{
    data.clear();
    if (!rec.hasData()) return;

    CompressedData cd;
    if (!process(cd)) {
        throw cybozu::Exception("WlogReceiver:popIo:failed") << rec;
    }
    assert(!cd.isCompressed());
    cd.moveTo(data);
    if (!rec.hasDataForChecksum()) return;

    const size_t ioSizeB = rec.ioSizeLb() * LBS;
    const uint32_t csum = cybozu::util::calcChecksum(data.data(), ioSizeB, salt_);
    if (csum != rec.checksum) {
        throw cybozu::Exception("WlogReceiver:popIo:invalid checksum") << rec << salt_ << csum;
    }
}

} //namespace walb
