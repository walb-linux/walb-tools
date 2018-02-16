#include "dirty_full_sync.hpp"

namespace walb {


const size_t ASYNC_IO_BUFFER_SIZE = (32U << 20);  // bytes


bool dirtyFullSyncClient(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    const std::atomic<uint64_t>& maxLbPerSec)
{
    assert(startLb <= sizeLb);
    const uint64_t bulkSize = bulkLb * LOGICAL_BLOCK_SIZE;
    AlignedArray buf(bulkSize);
    AsyncBdevReader reader(bdevPath, startLb, ASYNC_IO_BUFFER_SIZE, bulkSize);
    std::string encBuf;
    ThroughputStabilizer thStab;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb - startLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const uint32_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        buf.resize(size);
        reader.read(buf.data(), buf.size());
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


#define USE_AIO_FOR_DIRTY_FULL_SYNC


bool dirtyFullSyncServer(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb,
    bool skipZero, uint64_t fsyncIntervalSize,
    FullReplState *fullReplSt, const cybozu::FilePath &fullReplStDir,
    const std::string &fullReplStFileName)
{
    const char *const FUNC = __func__;
    assert(startLb <= sizeLb);
    if (fullReplSt) {
        assert(fullReplStDir.stat().isDirectory());
        assert(!fullReplStFileName.empty());
    }
#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
    cybozu::util::File file(bdevPath, O_RDWR | O_DIRECT);
    AsyncBdevWriter writer(file.fd(), ASYNC_IO_BUFFER_SIZE);
#else
    cybozu::util::File file(bdevPath, O_RDWR);
    if (startLb != 0) {
        file.lseek(startLb * LOGICAL_BLOCK_SIZE);
    }
#endif

    const uint64_t bulkSize = bulkLb * LOGICAL_BLOCK_SIZE;
    const AlignedArray zeroBuf(bulkSize, true);
    AlignedArray buf(bulkSize);
    AlignedArray encBuf;

    progressLb = startLb;
    uint64_t offLb = startLb;
#ifndef USE_AIO_FOR_DIRTY_FULL_SYNC
    uint64_t fadvOffBgn = startLb * LOGICAL_BLOCK_SIZE;
#endif
    uint64_t c = 0;
    uint64_t remainingLb = sizeLb - startLb;
    uint64_t writeSize = 0;  // bytes.
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const uint32_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        size_t encSize;
        pkt.read(encSize);
        if (encSize == 0) {
            if (skipZero) {
#ifndef USE_AIO_FOR_DIRTY_FULL_SYNC
                file.lseek(size, SEEK_CUR);
#endif
            } else {
#ifndef USE_AIO_FOR_DIRTY_FULL_SYNC
                file.write(zeroBuf.data(), size);
#else
                writer.prepare(offLb, lb, zeroBuf.data());
                writer.submit();
#endif
            }
        } else {
            encBuf.resize(encSize);
            pkt.read(&encBuf[0], encSize);
            buf.resize(size);
            uncompressSnappy(encBuf, buf, FUNC);
#ifndef USE_AIO_FOR_DIRTY_FULL_SYNC
            file.write(&buf[0], size);
#else
            writer.prepare(offLb, lb, std::move(buf));
            writer.submit();
            buf.clear();
#endif
        }
        remainingLb -= lb;
        offLb += lb;
        progressLb = offLb;
        writeSize += size;
        if (writeSize >= fsyncIntervalSize) {
#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
            writer.waitForAll();
#endif
            file.fdatasync();
#ifndef USE_AIO_FOR_DIRTY_FULL_SYNC
            file.fadvise(fadvOffBgn, writeSize, POSIX_FADV_DONTNEED);
            fadvOffBgn = offLb * LOGICAL_BLOCK_SIZE;
#endif
            writeSize = 0;
            if (fullReplSt) {
                fullReplSt->progressLb = progressLb;
                util::saveFile(fullReplStDir, fullReplStFileName, *fullReplSt);
            }
        }
        c++;
    }
#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
    writer.waitForAll();
#endif
    LOGs.debug() << "fdatasync start";
    file.fdatasync();
    LOGs.debug() << "fdatasync end";
    packet::Ack(pkt.sock()).send();
    pkt.flush();
    LOGs.debug() << "number of received packets" << c;
    return true;
}

} // namespace walb
