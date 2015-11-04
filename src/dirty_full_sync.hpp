#pragma once
#include <vector>
#include <atomic>
#include <deque>
#include <chrono>
#include <snappy.h>
#include "packet.hpp"
#include "fileio.hpp"
#include "walb_logger.hpp"
#include "bdev_reader.hpp"
#include "full_repl_state.hpp"
#include "snappy_util.hpp"
#include "cybozu/exception.hpp"
#include "throughput_util.hpp"

namespace walb {

/**
 * sizeLb is total size.
 *
 * RETURN:
 *   false if force stopped.
 */
inline bool dirtyFullSyncClient(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    const std::atomic<uint64_t>& maxLbPerSec)
{
    assert(startLb <= sizeLb);
    AlignedArray buf(bulkLb * LOGICAL_BLOCK_SIZE);
    AsyncBdevReader reader(bdevPath, startLb);
    std::string encBuf;
    ThroughputStabilizer thStab;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb - startLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        reader.read(&buf[0], size);
        if (cybozu::util::isAllZero(buf.data(), buf.size())) {
            pkt.write(0);
        } else {
            compressSnappy(buf, encBuf);
            pkt.write(encBuf.size());
            pkt.write(encBuf.data(), encBuf.size());
        }
        remainingLb -= lb;
        c++;
        thStab.setMaxLbPerSec(maxLbPerSec.load());
        thStab.addAndSleepIfNecessary(lb, 10, 100);
    }
    pkt.flush();
    packet::Ack(pkt.sock()).recv();
    LOGs.debug() << "number of sent packets" << c;
    return true;
}

/**
 * sizeLb is total size.
 * fullReplSt, fullReplStDir, and fullREplStFileName must be specified together.
 *
 * fsyncIntervalSize [bytes]
 *
 * RETURN:
 *   false if force stopped.
 */
inline bool dirtyFullSyncServer(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb,
    bool skipZero, uint64_t fsyncIntervalSize,
    FullReplState *fullReplSt = nullptr, const cybozu::FilePath &fullReplStDir = cybozu::FilePath(),
    const std::string &fullReplStFileName = "")
{
    const char *const FUNC = __func__;
    assert(startLb <= sizeLb);
    if (fullReplSt) {
        assert(fullReplStDir.stat().isDirectory());
        assert(!fullReplStFileName.empty());
    }
    cybozu::util::File file(bdevPath, O_RDWR);
    if (startLb != 0) {
        file.lseek(startLb * LOGICAL_BLOCK_SIZE);
    }
    const AlignedArray zeroBuf(bulkLb * LOGICAL_BLOCK_SIZE, true);
    AlignedArray buf(bulkLb * LOGICAL_BLOCK_SIZE);
    AlignedArray encBuf;

    progressLb = startLb;
    uint64_t c = 0;
    uint64_t remainingLb = sizeLb - startLb;
    uint64_t writeSize = 0;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        size_t encSize;
        pkt.read(encSize);
        if (encSize == 0) {
            if (skipZero) {
                file.lseek(size, SEEK_CUR);
            } else {
                file.write(zeroBuf.data(), size);
            }
        } else {
            encBuf.resize(encSize);
            pkt.read(&encBuf[0], encSize);
            buf.resize(size);
            uncompressSnappy(encBuf, buf, FUNC);
            file.write(&buf[0], size);
        }
        remainingLb -= lb;
        progressLb += lb;
        writeSize += size;
        if (writeSize >= fsyncIntervalSize) {
            file.fdatasync();
            writeSize = 0;
            if (fullReplSt) {
                fullReplSt->progressLb = progressLb;
                util::saveFile(fullReplStDir, fullReplStFileName, *fullReplSt);
            }
        }
        c++;
    }
    LOGs.debug() << "fdatasync start";
    file.fdatasync();
    LOGs.debug() << "fdatasync end";
    packet::Ack(pkt.sock()).send();
    pkt.flush();
    LOGs.debug() << "number of received packets" << c;
    return true;
}

} // namespace walb
