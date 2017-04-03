#include "walb_log_net.hpp"

namespace walb {

CompressedData convertToCompressedData(const LogBlockShared &blockS, bool doCompress)
{
    const uint32_t pbs = blockS.pbs();
    const size_t n = blockS.nBlocks();
    assert(0 < n);
    AlignedArray d(n * pbs);
    // This is inefficient because of memcpy. Stream compressor will solve the problem.
    for (size_t i = 0; i < n; i++) {
        ::memcpy(&d[i * pbs], blockS.get(i), pbs);
    }
    CompressedData cd;
    cd.setUncompressed(std::move(d));
    if (doCompress) cd.compress();
    return cd;
}

void convertToLogBlockShared(LogBlockShared& blockS, const CompressedData &cd, uint32_t sizePb, uint32_t pbs)
{
    const char *const FUNC = __func__;
    AlignedArray v;
    cd.getUncompressed(v);
    if (sizePb * pbs != v.size()) throw cybozu::Exception(FUNC) << "invalid size" << v.size() << sizePb;
    blockS.init(pbs);
    blockS.resize(sizePb);
    // This is inefficient because of memcpy. Stream uncompressor will solve the problem.
    for (size_t i = 0; i < sizePb; i++) {
        ::memcpy(blockS.get(i), &v[i * pbs], pbs);
    }
}


void WlogSender::process(CompressedData& cd) try
{
    cd.compress();
    ctrl_.next();
    cd.send(packet_);
} catch (std::exception& e) {
    try {
        packet::StreamControl(packet_.sock()).error();
    } catch (...) {}
    logger_.error() << "WlogSender:process" << e.what();
}


void WlogSender::pushIo(const LogPackHeader &header, uint16_t recIdx, const LogBlockShared &blockS)
{
    verifyPbsAndSalt(header);
    const WlogRecord &rec = header.record(recIdx);
    if (rec.hasDataForChecksum()) {
        CompressedData cd = convertToCompressedData(blockS, false);
        assert(0 < cd.originalSize());
        process(cd);
    }
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

void WlogReceiver::popIo(const WlogRecord &rec, LogBlockShared &blockS)
{
    if (rec.hasDataForChecksum()) {
        CompressedData cd;
        if (!process(cd)) throw cybozu::Exception("WlogReceiver:popIo:failed.");
        convertToLogBlockShared(blockS, cd, rec.ioSizePb(pbs_), pbs_);
        verifyWlogChecksum(rec, blockS, salt_);
    } else {
        blockS.init(pbs_);
    }
}

} //namespace walb
