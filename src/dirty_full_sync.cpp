#include "dirty_full_sync.hpp"
#include "thread_util.hpp"


#define USE_AIO_FOR_DIRTY_FULL_SYNC
//#undef USE_AIO_FOR_DIRTY_FULL_SYNC


namespace walb {

namespace dirty_full_sync_local {

const size_t ASYNC_IO_BUFFER_SIZE = (32U << 20);  // bytes

using Buffer = AlignedArray;

struct DualBuffer
{
    Buffer src;
    Buffer dst;
    uint32_t csum;

    // used by server only.
    uint64_t offLb;
    size_t lenLb;

    DualBuffer() = default;
    DualBuffer(const DualBuffer&) = delete;
    DualBuffer(DualBuffer&& rhs) noexcept : DualBuffer() { swap(rhs); }
    DualBuffer& operator=(const DualBuffer&) = delete;
    DualBuffer& operator=(DualBuffer&& rhs) noexcept { swap(rhs); return *this; }

    void clear() {
        src.clear();
        dst.clear();
    }
    void swap(DualBuffer& rhs) noexcept {
        std::swap(src, rhs.src);
        std::swap(dst, rhs.dst);
        std::swap(csum, rhs.csum);
        std::swap(offLb, rhs.offLb);
        std::swap(lenLb, rhs.lenLb);
    }
};


/**
 * To reuse memory.
 */
class DualBufferCache
{
    std::list<DualBuffer> list_;

public:
    void add(DualBuffer&& dbuf) {
        dbuf.clear(); // not really deallocated.
        list_.push_back(std::move(dbuf));
    }
    DualBuffer get() {
        if (list_.empty()) {
            return DualBuffer();
        } else {
            DualBuffer dbuf = std::move(list_.front());
            list_.pop_front();
            return dbuf;
        }
    }
};


void sendIoData(packet::Packet& pkt, const Buffer& dst, uint32_t csum)
{
    if (dst.empty()) {
        pkt.write(0);
    } else {
        pkt.write(dst.size());
        pkt.write(csum);
        pkt.write(dst.data(), dst.size());
    }
}


#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
void writeIoData(AsyncBdevWriter& writer, Buffer& dst, uint64_t offLb, size_t lenLb,
                 bool skipZero, const Buffer& zeroBuf)
{
    if (dst.empty()) {
        if (skipZero) return;
        writer.prepare(offLb, lenLb, zeroBuf.data());
        writer.submit();
    } else {
        assert(dst.size() == lenLb * LOGICAL_BLOCK_SIZE);
        writer.prepare(offLb, lenLb, std::move(dst));
        writer.submit();
        dst.clear();
    }
}
#else
void writeIoData(cybozu::util::File& file, Buffer& dst, size_t lenLb,
                 bool skipZero, const Buffer& zeroBuf)
{
    const size_t lenBytes = lenLb * LOGICAL_BLOCK_SIZE;
    if (dst.empty()) {
        if (skipZero) {
            file.lseek(lenBytes, SEEK_CUR);
        } else {
            file.write(zeroBuf.data(), lenBytes);
        }
    } else {
        assert(dst.size() == lenBytes);
        file.write(dst.data(), lenBytes);
    }
}
#endif


} // namespace dirty_full_sync_local



bool dirtyFullSyncClient(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb, const CompressOpt& cmprOpt,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    const std::atomic<uint64_t>& maxLbPerSec)
{
    assert(startLb <= sizeLb);
    AsyncBdevReader reader(bdevPath, startLb,
                           dirty_full_sync_local::ASYNC_IO_BUFFER_SIZE,
                           bulkLb * LOGICAL_BLOCK_SIZE);
    reader.setReadAheadUnlimited();
    dirty_full_sync_local::DualBufferCache dbufCache;
    using Buffer = dirty_full_sync_local::Buffer;
    using DualBuffer = dirty_full_sync_local::DualBuffer;
    ThroughputStabilizer thStab;
    const size_t maxPushedNum = cmprOpt.numCpu * 2 + 1;

    Compressor cmpr(cmprOpt.type, cmprOpt.level); // shared by all worker threads.
    cybozu::thread::ParallelConverter<DualBuffer, DualBuffer> pconv([&](DualBuffer&& dbuf) {
        const Buffer& src = dbuf.src;
        Buffer& dst = dbuf.dst;
        if (cybozu::util::isAllZero(src.data(), src.size())) {
            dst.resize(0);
            dbuf.csum = 0;
        } else {
            dst.resize(src.size() * 2 + 4096); // should be enough size.
            size_t s;
            if (cmpr.run(dst.data(), &s, dst.size(), src.data(), src.size())) {
                dst.resize(s);
            } else {
                // There is no fallback code currently.
                throw cybozu::Exception(__func__)
                    << "compress: dst buffer is not enough" << dst.size() << src.size();
            }
            dbuf.csum = cybozu::util::calcChecksum(dst.data(), dst.size(), 0);
        }
        return std::move(dbuf);
    });
    pconv.setLogger([](const std::exception& e) { LOGs.error() << e.what(); });
    pconv.start(cmprOpt.numCpu);

    auto popAndSendIoData = [&]() {
        DualBuffer dbuf;
        if (!pconv.pop(dbuf)) {
            throw cybozu::Exception(__func__) << "parallel converter failed";
        }
        dirty_full_sync_local::sendIoData(pkt, dbuf.dst, dbuf.csum);
        dbufCache.add(std::move(dbuf));
    };

    size_t pushedNum = 0;
    size_t c = 0;
    uint64_t remainingLb = sizeLb - startLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const uint32_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        DualBuffer dbuf = dbufCache.get();
        Buffer& src = dbuf.src;
        src.resize(lb * LOGICAL_BLOCK_SIZE);
        reader.read(src.data(), src.size());
        pconv.push(std::move(dbuf));
        remainingLb -= lb;
        c++;
        thStab.setMaxLbPerSec(maxLbPerSec.load());
        thStab.addAndSleepIfNecessary(lb, 10, 100);
        if (++pushedNum < maxPushedNum) continue;
        popAndSendIoData();
        pushedNum--;
    }
    pconv.sync();
    while (pushedNum > 0) {
        popAndSendIoData();
        pushedNum--;
    }
    pkt.flush();
    packet::Ack(pkt.sock()).recv();
    LOGs.debug() << "number of sent packets" << c;
    return true;
}


bool dirtyFullSyncServer(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb, const CompressOpt& cmprOpt,
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
    using Buffer = dirty_full_sync_local::Buffer;
    using DualBuffer = dirty_full_sync_local::DualBuffer;
    dirty_full_sync_local::DualBufferCache dbufCache;

#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
    cybozu::util::File file(bdevPath, O_RDWR | O_DIRECT);
    AsyncBdevWriter writer(file.fd(), dirty_full_sync_local::ASYNC_IO_BUFFER_SIZE);
#else
    cybozu::util::File file(bdevPath, O_RDWR);
    if (startLb != 0) {
        file.lseek(startLb * LOGICAL_BLOCK_SIZE);
    }
#endif

    const AlignedArray zeroBuf(bulkLb * LOGICAL_BLOCK_SIZE, true);
    const size_t maxPushedNum = cmprOpt.numCpu * 2 + 1;

    Uncompressor uncmpr(cmprOpt.type); // shared by all worker threads.
    cybozu::thread::ParallelConverter<DualBuffer, DualBuffer> pconv([&](DualBuffer&& dbuf) {
        const Buffer& src = dbuf.src;
        Buffer& dst = dbuf.dst;
        if (src.empty()) {
            // It means zero data.
            dst.resize(0);
        } else {
            size_t origSize = dbuf.lenLb * LOGICAL_BLOCK_SIZE;
            uint32_t csum = cybozu::util::calcChecksum(src.data(), src.size(), 0);
            if (csum != dbuf.csum) {
                throw cybozu::Exception(FUNC)
                    << "invalid checksum"
                    << cybozu::util::intToHexStr(csum, true)
                    << cybozu::util::intToHexStr(dbuf.csum, true)
                    << src.size() << origSize;
            }
            dst.resize(origSize);
            size_t s = uncmpr.run(dst.data(), dst.size(), src.data(), src.size());
            if (origSize != s) {
                throw cybozu::Exception(FUNC)
                    << "uncompress: bad size" << s << origSize;
            }
        }
        return std::move(dbuf);
    });
    pconv.setLogger([](const std::exception& e) { LOGs.error() << e.what(); });
    pconv.start(cmprOpt.numCpu);

    auto popAndWriteIoData = [&]() -> std::pair<uint64_t, size_t> {
        DualBuffer dbuf;
        if (!pconv.pop(dbuf)) {
            throw cybozu::Exception(FUNC) << "parallel converter failed";
        }
        const uint64_t offLb = dbuf.offLb;
        const size_t lenLb = dbuf.lenLb;
#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
        dirty_full_sync_local::writeIoData(writer, dbuf.dst, offLb, lenLb, skipZero, zeroBuf);
#else
        dirty_full_sync_local::writeIoData(file, dbuf.dst, lenLb, skipZero, zeroBuf);
#endif
        dbufCache.add(std::move(dbuf));
        return std::make_pair(offLb, lenLb);
    };

    auto saveFullReplState = [&]() {
        if (fullReplSt) {
            fullReplSt->progressLb = progressLb;
            util::saveFile(fullReplStDir, fullReplStFileName, *fullReplSt);
        }
    };

#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
    auto doSyncIfNecessary = [&](uint64_t& writeSize, size_t lb) {
        writeSize += lb * LOGICAL_BLOCK_SIZE;
        if (writeSize < fsyncIntervalSize) return;
        writer.waitForAll();
        file.fdatasync();
        saveFullReplState();
        writeSize = 0;
    };
#else
    auto doSyncIfNecessary = [&](uint64_t& writeSize, uint64_t& fadvOffBgn, uint64_t offLb, size_t lb) {
        writeSize += lb * LOGICAL_BLOCK_SIZE;
        if (writeSize < fsyncIntervalSize) return;
        file.fdatasync();
        file.fadvise(fadvOffBgn, writeSize, POSIX_FADV_DONTNEED);
        fadvOffBgn = (offLb + lb) * LOGICAL_BLOCK_SIZE;
        saveFullReplState();
        writeSize = 0;
    };
#endif

    progressLb = startLb;
    uint64_t offLb = startLb;
#ifndef USE_AIO_FOR_DIRTY_FULL_SYNC
    uint64_t fadvOffBgn = startLb * LOGICAL_BLOCK_SIZE;
#endif
    size_t pushedNum = 0;
    uint64_t c = 0;
    uint64_t remainingLb = sizeLb - startLb;
    uint64_t writeSize = 0;  // bytes.
    while (0 < remainingLb) {
        if (stopState == ForceStopping || ps.isForceShutdown()) {
            return false;
        }
        const uint32_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        size_t encSize;
        pkt.read(encSize);
        DualBuffer dbuf = dbufCache.get();
        Buffer& src = dbuf.src;
        if (encSize == 0) {
            src.resize(0);
        } else {
            pkt.read(dbuf.csum);
            src.resize(encSize);
            pkt.read(src.data(), src.size());
        }
        dbuf.offLb = offLb;
        dbuf.lenLb = lb;
        pconv.push(std::move(dbuf));
        offLb += lb;
        remainingLb -= lb;
        c++;
        if (++pushedNum < maxPushedNum) continue;

        uint64_t offLb2; size_t lb2;
        std::tie(offLb2, lb2) = popAndWriteIoData();
        pushedNum--;
        progressLb = offLb2 + lb2;
#ifdef USE_AIO_FOR_DIRTY_FULL_SYNC
        doSyncIfNecessary(writeSize, lb2);
#else
        doSyncIfNecessary(writeSize, fadvOffBgn, offLb2, lb2);
#endif
    }
    pconv.sync();
    while (pushedNum > 0) {
        popAndWriteIoData();
        pushedNum--;
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
