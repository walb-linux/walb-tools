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

inline void popAndSendPack(packet::Packet& pkt, DiffStatistics& statOut, ConverterQueue& conv, std::atomic<bool>& failed)
{
    std::exception_ptr ep;
    packet::StreamControl ctrl(pkt.sock());
    try {
        statOut.clear();
        statOut.wdiffNr = -1;

        compressor::Buffer pack = conv.pop();
        while (!pack.empty()) {
            if (failed) {
                ctrl.error();
                throw cybozu::Exception(__func__) << "failed";
            }
            ctrl.next();
            pkt.write<size_t>(pack.size());
            pkt.write(pack.data(), pack.size());
            statOut.update(*(const DiffPackHeader*)pack.data());
            pack = conv.pop();
        }
        ctrl.end();
        return;
    } catch (...) {
        ep = std::current_exception();
    }

    // failure path.
    conv.quit();
    failed = true;
    conv.popAll();
    if (ep) std::rethrow_exception(ep);
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
    const size_t maxPushedNum = cmpr.numCpu * 2 - 1;
    ConverterQueue conv(maxPushedNum, cmpr.numCpu, true, cmpr.type, cmpr.level);
    std::atomic<bool> failed(false);

    cybozu::thread::ThreadRunner senderTh([&]() {
            wdiff_transfer_local::popAndSendPack(pkt, statOut, conv, failed);
        });
    senderTh.start();

    std::exception_ptr ep;
    try {
        DiffRecIo recIo;
        DiffPacker packer;
        while (merger.getAndRemove(recIo)) {
            if (stopState == ForceStopping || ps.isForceShutdown()) {
                failed = true;
                conv.quit();
                senderTh.joinNoThrow();
                return false;
            }
            const DiffRecord& rec = recIo.record();
            const DiffIo& io = recIo.io();
            if (packer.add(rec, io.get())) {
                continue;
            }
            if (!conv.push(packer.getPackAsArray())) {
                throw cybozu::Exception(__func__) << "push failed.";
            }
            packer.clear();
            packer.add(rec, io.get());
        }
        if (!packer.empty()) {
            if (!conv.push(packer.getPackAsArray())) {
                throw cybozu::Exception(__func__) << "push failed";
            }
        }
    } catch (...) {
        ep = std::current_exception();
        failed = true;
    }
    conv.quit();
    senderTh.join();
    if (ep) std::rethrow_exception(ep);
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
    const std::atomic<int> &stopState, const ProcessStatus &ps)
{
    const char *const FUNC = __func__;
    cybozu::util::File fileW(wdiffOutFd);
    Buffer buf;
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
        verifyDiffPack(buf);
        fileW.write(buf.data(), buf.size());
        writeSize += buf.size();
		if (writeSize >= MAX_FSYNC_DATA_SIZE) {
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
