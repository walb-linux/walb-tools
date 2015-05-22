#pragma once
#include <atomic>
#include "cybozu/socket.hpp"
#include "walb_diff_merge.hpp"
#include "walb_diff_compressor.hpp"
#include "walb_diff_pack.hpp"
#include "server_util.hpp"
#include "host_info.hpp"

namespace walb {

namespace wdiff_transfer_local {

inline void sendPack(packet::Packet& pkt, packet::StreamControl& ctrl, DiffStatistics& statOut, const compressor::Buffer& pack)
{
    ctrl.next();
    pkt.write<size_t>(pack.size());
    pkt.write(pack.data(), pack.size());
    statOut.update(*(const DiffPackHeader*)pack.data());
}

} // namespace wdiff_transfer_local

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool wdiffTransferClient(
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

/**
 * Wdiff header must have been written already before calling this.
 *
 * RETURN:
 *   false if force stopped.
 */
inline bool wdiffTransferServer(
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
