#pragma once
#include <atomic>
#include <cassert>
#include "packet.hpp"
#include "walb_diff_virt.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_compressor.hpp"
#include "walb_diff_io.hpp"
#include "discard_type.hpp"
#include "fileio.hpp"
#include "uuid.hpp"
#include "murmurhash3.hpp"
#include "server_util.hpp"
#include "thread_util.hpp"

namespace walb {

namespace dirty_hash_sync_local {

inline void compressAndSend(
    packet::Packet &pkt, DiffPacker &packer,
    PackCompressor &compr, packet::StreamControl &sendCtl)
{
    compressor::Buffer compBuf = compr.convert(packer.getPackAsArray().data());
    sendCtl.next();
    pkt.write<size_t>(compBuf.size());
    pkt.write(compBuf.data(), compBuf.size());
}

} // namespace dirty_hash_sync_local

/**
 * Reader must have the member function: void read(void *data, size_t size).
 */
template <typename Reader>
inline bool dirtyHashSyncClient(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, uint32_t hashSeed,
    const std::atomic<int> &stopState, const ProcessStatus &ps)
{
    const char *const FUNC = __func__;
    packet::StreamControl recvCtl(pkt.sock());
    packet::StreamControl sendCtl(pkt.sock());
    DiffPacker packer;
    walb::PackCompressor compr(::WALB_DIFF_CMPR_SNAPPY);
    cybozu::murmurhash3::Hasher hasher(hashSeed);

    uint64_t addr = 0;
    uint64_t remainingLb = sizeLb;
    AlignedArray buf;
    for (;;) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
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
        if (recvHash != bdHash && !packer.add(addr, lb, buf.data())) {
            dirty_hash_sync_local::compressAndSend(pkt, packer, compr, sendCtl);
            packer.add(addr, lb, buf.data());
        } else {
            sendCtl.dummy(); // to avoid socket timeout.
        }
        recvCtl.reset();

        remainingLb -= lb;
        addr += lb;
    }
    if (!packer.empty()) {
        dirty_hash_sync_local::compressAndSend(pkt, packer, compr, sendCtl);
    }
    if (recvCtl.isError()) {
        throw cybozu::Exception(FUNC) << "recvCtl";
    }
    sendCtl.end();
    pkt.flush();
    return true;
}

/**
 * doWriteDiff is true, outFd means oupput wdiff fd.
 * otherwise, outFd means block device fd of full image store.
 *
 * fsyncIntervalSize [bytes].
 */
template <typename Reader>
inline bool dirtyHashSyncServer(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, const cybozu::Uuid& uuid, uint32_t hashSeed,
    bool doWriteDiff, int outFd, DiscardType discardType,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb,
    uint64_t fsyncIntervalSize)
{
    const char *const FUNC = __func__;

    std::atomic<bool> quit(false);

    auto readVirtualFullImageAndSendHash = [&]() {
        cybozu::murmurhash3::Hasher hasher(hashSeed);
        packet::StreamControl ctrl(pkt.sock());

        AlignedArray buf;
        uint64_t remaining = sizeLb;
        try {
            while (remaining > 0) {
                if (stopState == ForceStopping || ps.isForceShutdown()) {
                    quit = true;
                    return;
                }
                const uint64_t lb = std::min<uint64_t>(remaining, bulkLb);
                buf.resize(lb * LOGICAL_BLOCK_SIZE);
                reader.read(buf.data(), buf.size());
                const cybozu::murmurhash3::Hash hash = hasher(buf.data(), buf.size());
                ctrl.next();
                pkt.write(hash);
                remaining -= lb;
                progressLb += lb;
            }
            ctrl.end();
            pkt.flush();
        } catch (std::exception& e) {
            ctrl.error();
            throw;
        }
    };
    cybozu::thread::ThreadRunner readerTh(readVirtualFullImageAndSendHash);
    readerTh.start();

    cybozu::util::File fileW(outFd);
    std::vector<char> zero;

    if (doWriteDiff) {
        DiffFileHeader wdiffH;
        wdiffH.setMaxIoBlocksIfNecessary(bulkLb);
        wdiffH.setUuid(uuid);
        wdiffH.writeTo(fileW);
    }

    packet::StreamControl ctrl(pkt.sock());
    AlignedArray buf;
    uint64_t writeSize = 0;
    while (ctrl.isNext() || ctrl.isDummy()) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            readerTh.join();
            return false;
        }
        if (ctrl.isDummy()) {
            ctrl.reset();
            continue;
        }
        size_t size;
        pkt.read(size);
        verifyDiffPackSize(size, FUNC);
        buf.resize(size);
        pkt.read(buf.data(), buf.size());
        verifyDiffPack(buf.data(), buf.size(), true);
        if (doWriteDiff) {
            fileW.write(buf.data(), buf.size());
        } else {
            MemoryDiffPack pack(buf.data(), buf.size());
            issueDiffPack(fileW, discardType, pack, zero);
        }
        writeSize += buf.size();
        if (writeSize >= fsyncIntervalSize) {
            fileW.fdatasync();
            writeSize = 0;
        }
        ctrl.reset();
    }
    if (ctrl.isError()) {
        throw cybozu::Exception(FUNC) << "client sent an error";
    }
    assert(ctrl.isEnd());
    if (doWriteDiff) {
        writeDiffEofPack(fileW);
    } else {
        fileW.fdatasync();
    }

    readerTh.join();
    return !quit;
}

} // namespace walb
