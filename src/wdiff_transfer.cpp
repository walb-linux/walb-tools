#include "wdiff_transfer.hpp"

namespace walb {

bool wdiffTransferClient(
    packet::Packet &pkt, DiffMerger &merger, const CompressOpt &cmpr,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    DiffStatistics &statOut)
{
    const size_t maxPushedNum = cmpr.numCpu * 2 + 1;
    ConverterQueue conv(maxPushedNum, cmpr.numCpu, true, cmpr.type, cmpr.level);
    statOut.clear();
    statOut.wdiffNr = -1;
    packet::StreamControl ctrl(pkt.sock());

    DiffRecIo recIo;
    DiffPacker packer;
    size_t pushedNum = 0;
    while (merger.getAndRemove(recIo)) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const DiffRecord& rec = recIo.record();
        const DiffIo& io = recIo.io();
        if (packer.add(rec, io.get())) continue;
        conv.push(packer.getPackAsArray());
        pushedNum++;
        packer.clear();
        packer.add(rec, io.get());
        if (pushedNum < maxPushedNum) continue;
        wdiff_transfer_local::sendPack(pkt, ctrl, statOut, conv.pop());
        pushedNum--;
    }
    if (!packer.empty()) {
        conv.push(packer.getPackAsArray());
    }
    conv.quit();
    for (compressor::Buffer pack = conv.pop(); !pack.empty(); pack = conv.pop()) {
        wdiff_transfer_local::sendPack(pkt, ctrl, statOut, pack);
    }
    ctrl.end();
    pkt.flush();
    return true;
}

bool wdiffTransferNoMergeClient(
    packet::Packet &pkt, cybozu::util::File &fileR,
    const std::atomic<int> &stopState, const ProcessStatus &ps)
{
    packet::StreamControl ctrl(pkt.sock());
    AlignedArray packHBuf(WALB_DIFF_PACK_SIZE);
    DiffPackHeader &packH = *(DiffPackHeader *)packHBuf.data();
    DiffStatistics statOut;
    AlignedArray pack;
    for (;;) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        try {
            packH.readFrom(fileR);
        } catch (cybozu::util::EofError &) {
            packH.setEnd();
        }
        if (packH.isEnd()) break;
        pack.resize(WALB_DIFF_PACK_SIZE + packH.total_size);
        ::memcpy(pack.data(), packHBuf.data(), packHBuf.size());
        fileR.read(pack.data() + WALB_DIFF_PACK_SIZE, packH.total_size);
        verifyDiffPack(pack.data(), pack.size(), true);
        wdiff_transfer_local::sendPack(pkt, ctrl, statOut, pack);
    }
    ctrl.end();
    pkt.flush();
    return true;
}

bool indexedWdiffTransferClient(
    packet::Packet &pkt, IndexedDiffReader& reader, const CompressOpt &cmpr,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    DiffStatistics &statOut)
{
    const size_t maxPushedNum = cmpr.numCpu * 2 + 1;
    ConverterQueue conv(maxPushedNum, cmpr.numCpu, true, cmpr.type, cmpr.level);
    packet::StreamControl ctrl(pkt.sock());
    statOut.clear();
    statOut.wdiffNr = -1;

    DiffIndexRecord irec;
    AlignedArray data;
    DiffPacker packer;
    size_t pushedNum = 0;
    while (reader.readDiff(irec, data)) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        DiffRecord rec;
        rec.io_address = irec.io_address;
        rec.io_blocks = irec.io_blocks;
        rec.flags = irec.flags;
        const char *dataPtr = nullptr;
        if (irec.isNormal()) {
            rec.compression_type = ::WALB_DIFF_CMPR_NONE;
            rec.data_offset = 0; // updated later.
            rec.data_size = irec.io_blocks * LOGICAL_BLOCK_SIZE;
            rec.checksum = irec.io_checksum;
            dataPtr = data.data() + (irec.io_offset * LOGICAL_BLOCK_SIZE);
        }

        if (packer.add(rec, dataPtr)) continue;
        conv.push(packer.getPackAsArray());
        pushedNum++;
        packer.clear();
        packer.add(rec, dataPtr);
        if (pushedNum < maxPushedNum) continue;
        wdiff_transfer_local::sendPack(pkt, ctrl, statOut, conv.pop());
        pushedNum--;
    }
    if (!packer.empty()) {
        conv.push(packer.getPackAsArray());
    }
    conv.quit();
    for (compressor::Buffer pack = conv.pop(); !pack.empty(); pack = conv.pop()) {
        wdiff_transfer_local::sendPack(pkt, ctrl, statOut, pack);
    }
    ctrl.end();
    pkt.flush();
    return true;
}

bool wdiffTransferServer(
    packet::Packet &pkt, int wdiffOutFd,
    const std::atomic<int> &stopState, const ProcessStatus &ps, uint64_t fsyncIntervalSize)
{
    const char *const FUNC = __func__;
    cybozu::util::File fileW(wdiffOutFd);
    AlignedArray buf;
    packet::StreamControl ctrl(pkt.sock());
    uint64_t writeSize = 0;
    while (ctrl.isNext()) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        size_t size;
        pkt.read(size);
        verifyDiffPackSize(size, FUNC);
        buf.resize(size);
        pkt.read(buf.data(), buf.size());
        verifyDiffPack(buf.data(), buf.size(), true);
        fileW.write(buf.data(), buf.size());
        writeSize += buf.size();
        if (writeSize >= fsyncIntervalSize) {
            fileW.fdatasync();
            writeSize = 0;
        }
        ctrl.reset();
    }
    if (!ctrl.isEnd()) {
        throw cybozu::Exception(FUNC) << "bad ctrl not end";
    }
    writeDiffEofPack(fileW);
    return true;
}

} // namespace walb
