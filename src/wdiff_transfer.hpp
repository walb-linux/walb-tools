#pragma once
#include <atomic>
#include "cybozu/socket.hpp"
#include "walb_diff_merge.hpp"
#include "walb_diff_compressor.hpp"
#include "walb_diff_pack.hpp"
#include "protocol.hpp"

namespace walb {

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool wdiffTransferClient(
    packet::Packet &pkt, diff::Merger &merger,
    int cmprType, size_t cmprLevel, size_t cmprNumCPU,
    const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit)
{
    packet::StreamControl ctrl(pkt.sock());

    auto sendPack = [&](const Buffer& pack) {
        ctrl.next();
        const uint32_t size = pack.size();
        pkt.write(size);
        pkt.write(pack.data(), pack.size());
    };

    diff::RecIo recIo;
    const size_t maxPushedNum = cmprNumCPU * 2 - 1;
    ConverterQueue conv(maxPushedNum, cmprNumCPU, true, cmprType, cmprLevel);
    diff::Packer packer;
    size_t pushedNum = 0;
    while (merger.pop(recIo)) {
        if (stopState == ForceStopping || forceQuit) {
            return false;
        }
        const DiffRecord& rec = recIo.record();
        const DiffIo& io = recIo.io();
        if (packer.add(rec, io.get())) {
            continue;
        }
        conv.push(packer.getPackAsVector());
        pushedNum++;
        packer.reset();
        packer.add(rec, io.get());
        if (pushedNum < maxPushedNum) {
            continue;
        }
        sendPack(conv.pop());
        pushedNum--;
    }
    if (!packer.empty()) {
        conv.push(packer.getPackAsVector());
    }
    conv.quit();
    Buffer pack = conv.pop();
    while (!pack.empty()) {
        sendPack(pack);
        pack = conv.pop();
    }
    ctrl.end();
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
    const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit)
{
    const char *const FUNC = __func__;
    cybozu::util::FdWriter fdw(wdiffOutFd);
    Buffer buf;
    packet::StreamControl ctrl(pkt.sock());
    while (ctrl.isNext()) {
        if (stopState == ForceStopping || forceQuit) {
            return false;
        }
        uint32_t size;
        pkt.read(size);
        verifyDiffPackSize(size, FUNC);
        buf.resize(size);
        pkt.read(buf.data(), buf.size());
        verifyDiffPack(buf);
        fdw.write(buf.data(), buf.size());
        ctrl.reset();
    }
    if (!ctrl.isEnd()) {
        throw cybozu::Exception(FUNC) << "bad ctrl not end";
    }
    writeDiffEofPack(fdw);
    return true;
}

} // namespace walb
