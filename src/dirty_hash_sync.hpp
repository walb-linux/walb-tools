#pragma once
#include <atomic>
#include <cassert>
#include <mutex>
#include <condition_variable>
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
#include "throughput_util.hpp"

namespace walb {

namespace dirty_hash_sync_local {

inline void compressAndSend(
    packet::Packet &pkt, DiffPacker &packer, PackCompressor &compr)
{
    compressor::Buffer compBuf = compr.convert(packer.getPackAsArray().data());
    pkt.write<size_t>(compBuf.size());
    pkt.write(compBuf.data(), compBuf.size());
}

/**
 * func must send/receive just one byte.
 */
template <typename Func>
void doRetrySockIo(size_t nr, const char *msg, Func&& func)
{
    std::exception_ptr ep;
    bool failed = true;
    size_t ms = 1000;
    for (size_t i = 0; i < nr; i++) {
        if (i != 0) {
            util::sleepMs(ms);
            ms *= 2;
        }
        try {
            func();
            failed = false;
            break;
        } catch (...) {
            if (errno != EAGAIN) throw;
            LOGs.warn() << "doRetrySockIo:failed" << msg << i;
            ep = std::current_exception();
        }
    }
    if (failed) std::rethrow_exception(ep);
}

template <typename Reader>
void readAndSendHash(
    uint64_t& hashLb,
    packet::Packet& pkt, packet::StreamControl2& ctrl, Reader &reader,
    uint64_t sizeLb, size_t bulkLb,
    cybozu::murmurhash3::Hasher& hasher, AlignedArray& buf)
{
    const uint64_t lb = std::min<uint64_t>(sizeLb - hashLb, bulkLb);
    buf.resize(lb * LOGICAL_BLOCK_SIZE);
    reader.read(buf.data(), buf.size());
    const cybozu::murmurhash3::Hash hash = hasher(buf.data(), buf.size());
    doRetrySockIo(2, "ctrl.send.next", [&]() { ctrl.sendNext(); });
    pkt.write(hash);
    hashLb += lb;
}

inline void readPackAndWrite(
    uint64_t& writeSize, packet::Packet& pkt,
    cybozu::util::File& fileW, bool doWriteDiff, DiscardType discardType,
    uint64_t fsyncIntervalSize,
    AlignedArray& zero, AlignedArray& buf)
{
    const char *const FUNC = __func__;
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
}

} // namespace dirty_hash_sync_local

/**
 * Reader must have the member function: void read(void *data, size_t size).
 */
template <typename Reader>
bool dirtyHashSyncClient(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, uint32_t hashSeed,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    const std::atomic<uint64_t>& maxLbPerSec)
{
    const char *const FUNC = __func__;
    packet::StreamControl2 recvCtl(pkt.sock());
    packet::StreamControl2 sendCtl(pkt.sock());
    DiffPacker packer;
    walb::PackCompressor compr(::WALB_DIFF_CMPR_SNAPPY);
    cybozu::murmurhash3::Hasher hasher(hashSeed);
    ThroughputStabilizer thStab;

    uint64_t addr = 0;
    uint64_t remainingLb = sizeLb;
    AlignedArray buf;
    size_t cHash = 0, cSend = 0, cDummy = 0;
    try {
    for (;;) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        dirty_hash_sync_local::doRetrySockIo(4, "ctrl.recv", [&]() { recvCtl.recv(); });
        if (recvCtl.isNext()) {
            if (remainingLb == 0) throw cybozu::Exception(FUNC) << "has next but remainingLb is zero";
        } else {
            if (remainingLb == 0) break;
            throw cybozu::Exception(FUNC) << "no next but remainingLb is not zero" << remainingLb;
        }
        cybozu::murmurhash3::Hash recvHash;
        pkt.read(recvHash);
        cHash++;

        const uint32_t lb = std::min<uint64_t>(remainingLb, bulkLb);
        buf.resize(lb * LOGICAL_BLOCK_SIZE);
        reader.read(buf.data(), buf.size());

        // to avoid socket timeout.
        dirty_hash_sync_local::doRetrySockIo(4, "ctrl.send.dummy", [&]() { sendCtl.sendDummy(); });
        cDummy++; cSend++;

        const cybozu::murmurhash3::Hash bdHash = hasher(buf.data(), buf.size());
        const uint64_t bgnAddr = packer.empty() ? addr : packer.header()[0].io_address;
        if (addr - bgnAddr >= DIRTY_HASH_SYNC_MAX_PACK_AREA_LB && !packer.empty()) {
            dirty_hash_sync_local::doRetrySockIo(4, "ctrl.send.next0", [&]() { sendCtl.sendNext(); });
            cSend++;
            dirty_hash_sync_local::compressAndSend(pkt, packer, compr);
        }
        if (recvHash != bdHash && !packer.add(addr, lb, buf.data())) {
            dirty_hash_sync_local::doRetrySockIo(4, "ctrl.send.next1", [&]() { sendCtl.sendNext(); });
            cSend++;
            dirty_hash_sync_local::compressAndSend(pkt, packer, compr);
            packer.add(addr, lb, buf.data());
        }
        pkt.flush();
        remainingLb -= lb;
        addr += lb;
        thStab.setMaxLbPerSec(maxLbPerSec.load());
        thStab.addAndSleepIfNecessary(lb, 10, 100);
    }
    } catch (...) {
        LOGs.warn() << "SEND_CTL" << cHash << cSend << cDummy;
        throw;
    }
    if (!packer.empty()) {
        dirty_hash_sync_local::doRetrySockIo(4, "ctrl.send.next2", [&]() { sendCtl.sendNext(); });
        cSend++;
        dirty_hash_sync_local::compressAndSend(pkt, packer, compr);
    }
    if (recvCtl.isError()) {
        throw cybozu::Exception(FUNC) << "recvCtl";
    }
    dirty_hash_sync_local::doRetrySockIo(4, "ctrl.send.next2", [&]() { sendCtl.sendEnd(); });
    pkt.flush();

    LOGs.debug() << "SEND_CTL" << cHash << cSend << cDummy;
    return true;
}

/**
 * doWriteDiff is true, outFd means oupput wdiff fd.
 * otherwise, outFd means block device fd of full image store.
 *
 * fsyncIntervalSize [bytes].
 */
template <typename Reader>
bool dirtyHashSyncServer(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, const cybozu::Uuid& uuid, uint32_t hashSeed,
    bool doWriteDiff, int outFd, DiscardType discardType,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb,
    uint64_t fsyncIntervalSize)
{
    const char *const FUNC = __func__;

    std::atomic<bool> quit(false);

    std::atomic<uint64_t> recvLb(0);
    auto abortCondition = [&]() {
        return stopState == ForceStopping || ps.isForceShutdown();
    };

    auto readVirtualFullImageAndSendHash = [&]() {
        cybozu::murmurhash3::Hasher hasher(hashSeed);
        packet::StreamControl2 ctrl(pkt.sock());
        AlignedArray buf;
        uint64_t hashLb = 0;
        size_t sHash = 0;
        try {
            while (hashLb < sizeLb) {
                if (abortCondition()) {
                    quit = true;
                    return;
                }
                dirty_hash_sync_local::readAndSendHash(
                    hashLb, pkt, ctrl, reader, sizeLb, bulkLb, hasher, buf);
                sHash++;
                progressLb = hashLb;
            }
            ctrl.sendEnd();
            pkt.flush();
            LOGs.debug() << "SEND_CTL" << sHash;
        } catch (std::exception& e) {
            LOGs.warn() << "SEND_CTL" << sHash;
            ctrl.sendError();
            throw;
        }
    };
    cybozu::thread::ThreadRunner readerTh(readVirtualFullImageAndSendHash);
    readerTh.start();

    cybozu::util::File fileW(outFd);
    AlignedArray zero;

    if (doWriteDiff) {
        DiffFileHeader wdiffH;
        wdiffH.setUuid(uuid);
        wdiffH.writeTo(fileW);
    }

    packet::StreamControl2 ctrl(pkt.sock());
    AlignedArray buf;
    uint64_t writeSize = 0;
    size_t sDummy = 0, sRecv = 0;
    try {
    for (;;) {
        dirty_hash_sync_local::doRetrySockIo(4, "ctrl.recv", [&]() { ctrl.recv(); });
        sRecv++;
        if (!ctrl.isNext() && !ctrl.isDummy()) break;
        if (abortCondition()) {
            LOGs.warn() << "RECV_CTL" << sRecv << sDummy;
            readerTh.join();
            return false;
        }
        recvLb += bulkLb;
        if (ctrl.isDummy()) {
            sDummy++;
            continue;
        }
        dirty_hash_sync_local::readPackAndWrite(
            writeSize, pkt, fileW, doWriteDiff, discardType, fsyncIntervalSize, zero, buf);
    }
    } catch (...) {
        LOGs.warn() << "RECV_CTL" << sRecv << sDummy;
        throw;
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
    LOGs.debug() << "RECV_CTL" << sRecv << sDummy;
    return !quit;
}

/**
 * Single-threaded version.
 * For test.
 */
template <typename Reader>
bool dirtyHashSyncServer2(
    packet::Packet &pkt, Reader &reader,
    uint64_t sizeLb, uint64_t bulkLb, const cybozu::Uuid& uuid, uint32_t hashSeed,
    bool doWriteDiff, int outFd, DiscardType discardType,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb,
    uint64_t fsyncIntervalSize)
{
    const char *const FUNC = __func__;
    cybozu::util::File fileW(outFd);

    if (doWriteDiff) {
        DiffFileHeader wdiffH;
        wdiffH.setUuid(uuid);
        wdiffH.writeTo(fileW);
    }

    uint64_t hashLb = 0, recvLb = 0;
    AlignedArray buf0, buf1;
    packet::StreamControl2 ctrl(pkt.sock());
    AlignedArray zero;
    uint64_t writeSize = 0;
    cybozu::murmurhash3::Hasher hasher(hashSeed);
    size_t sHash = 0, sDummy = 0, sRecv = 0;

    try {
    for (;;) {
        if (stopState == ForceStopping || ps.isForceShutdown()) return false;

        bool sentHash = false;
        while (hashLb < recvLb + DIRTY_HASH_SYNC_READ_AHEAD_LB) {
            /*
             * CAUSION:
             * If DIRTY_HASH_SYNC_READ_AHEAD_LB is too large,
             * socket write will block.
             */
            dirty_hash_sync_local::readAndSendHash(
                hashLb, pkt, ctrl, reader, sizeLb, bulkLb, hasher, buf0);
            sHash++;
            sentHash = true;
            progressLb = hashLb;
            if (hashLb == sizeLb) ctrl.sendEnd();
        }
        if (sentHash) pkt.flush();

        dirty_hash_sync_local::doRetrySockIo(4, "ctrl.recv", [&]() { ctrl.recv(); });
        sRecv++;
        if (!ctrl.isNext() && !ctrl.isDummy()) break;
        recvLb += bulkLb; // may exceeds sizeLb.
        if (ctrl.isDummy()) {
            sDummy++;
            continue;
        }
        dirty_hash_sync_local::readPackAndWrite(
            writeSize, pkt, fileW, doWriteDiff, discardType, fsyncIntervalSize, zero, buf1);
    }
    } catch (...) {
        LOGs.warn() << "RECV_CTL" << sHash << sRecv << sDummy;
        throw;
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

    LOGs.debug() << "RECV_CTL" << sHash << sRecv << sDummy;
    return true;
}

} // namespace walb
