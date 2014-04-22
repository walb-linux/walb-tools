#pragma once
#include <atomic>
#include <cassert>
#include "packet.hpp"
#include "walb_diff_virt.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_compressor.hpp"
#include "fileio.hpp"
#include "uuid.hpp"
#include "murmurhash3.hpp"
#include "server_util.hpp"
#include "thread_util.hpp"

namespace walb {

/**
 * Reader must have the member function: void read(void *data, size_t size).
 */
template <typename Reader>
inline bool dirtyHashSyncClient(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, uint32_t hashSeed,
    const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit)
{
    const char *const FUNC = __func__;
    packet::StreamControl recvCtl(pkt.sock());
    packet::StreamControl sendCtl(pkt.sock());
    diff::Packer packer;
    walb::PackCompressor compr(::WALB_DIFF_CMPR_SNAPPY);
    cybozu::murmurhash3::Hasher hasher(hashSeed);

    auto compressAndSend = [&]() {
        Buffer compBuf = compr.convert(packer.getPackAsVector().data());
        sendCtl.next();
        pkt.write<size_t>(compBuf.size());
        pkt.write(compBuf.data(), compBuf.size());
    };

    uint64_t addr = 0;
    uint64_t remainingLb = sizeLb;
    Buffer buf;
    for (;;) {
        if (stopState == ForceStopping || forceQuit) {
            return false;
        }
        const bool hasNext = recvCtl.isNext();
        if (hasNext) {
            if (remainingLb == 0) throw cybozu::Exception(FUNC) << "has next but remainingLb is zero";
        } else {
            if (remainingLb == 0) break;
            throw cybozu::Exception(FUNC) << "no next but remainingLb is not zero" << remainingLb;
        }
        cybozu::murmurhash3::Hash recvHash;
        pkt.read(recvHash);

        const uint16_t lb = std::min<uint64_t>(remainingLb, bulkLb);
        buf.resize(lb * LOGICAL_BLOCK_SIZE);
        reader.read(buf.data(), buf.size());

        const cybozu::murmurhash3::Hash bdHash = hasher(buf.data(), buf.size());
        if (recvHash != bdHash) {
            if (!packer.add(addr, lb, buf.data())) {
                compressAndSend();
                packer.add(addr, lb, buf.data());
            }
        }
        recvCtl.reset();

        remainingLb -= lb;
        addr += lb;
    }
    if (!packer.empty()) compressAndSend();
    if (recvCtl.isError()) {
        throw cybozu::Exception(FUNC) << "recvCtl";
    }
    sendCtl.end();
    return true;
}

template <typename Reader>
inline bool dirtyHashSyncServer(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, const cybozu::Uuid& uuid, uint32_t hashSeed,
    int outDiffFd, const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit)
{
    const char *const FUNC = __func__;

    std::atomic<bool> quit(false);

    auto readVirtualFullImageAndSendHash = [&]() {
        cybozu::murmurhash3::Hasher hasher(hashSeed);
        packet::StreamControl ctrl(pkt.sock());

        uint64_t remaining = sizeLb;
        try {
            while (remaining > 0) {
                if (stopState == ForceStopping || forceQuit) {
                    quit = true;
                    return;
                }
                const uint64_t lb = std::min<uint64_t>(remaining, bulkLb);
                Buffer buf(lb * LOGICAL_BLOCK_SIZE);
                reader.read(buf.data(), buf.size());
                const cybozu::murmurhash3::Hash hash = hasher(buf.data(), buf.size());
                ctrl.next();
                pkt.write(hash);
                remaining -= lb;
            }
            ctrl.end();
        } catch (std::exception& e) {
            ctrl.error();
            throw;
        }
    };
    cybozu::thread::ThreadRunner readerTh(readVirtualFullImageAndSendHash);
    readerTh.start();

    cybozu::util::FdWriter fdw(outDiffFd);

    {
        DiffFileHeader wdiffH;
        wdiffH.setMaxIoBlocksIfNecessary(bulkLb);
        wdiffH.setUuid(uuid);
        wdiffH.writeTo(fdw);
    }

    packet::StreamControl ctrl(pkt.sock());
    Buffer buf;
    while (ctrl.isNext()) {
        if (stopState == ForceStopping || forceQuit) {
            readerTh.join();
            return false;
        }
        size_t size;
        pkt.read(size);
        verifyDiffPackSize(size, FUNC);
        buf.resize(size);
        pkt.read(buf.data(), buf.size());
        verifyDiffPack(buf);
        fdw.write(buf.data(), buf.size());
        ctrl.reset();
    }
    if (ctrl.isError()) {
        throw cybozu::Exception(FUNC) << "client sent an error";
    }
    assert(ctrl.isEnd());
    writeDiffEofPack(fdw);

    readerTh.join();
    return !quit;
}

} // namespace walb
