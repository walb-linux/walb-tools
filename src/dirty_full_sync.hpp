#pragma once
#include <vector>
#include <atomic>
#include <snappy.h>
#include "packet.hpp"
#include "fileio.hpp"
#include "walb_logger.hpp"
#include "bdev_reader.hpp"
#include "cybozu/exception.hpp"

namespace walb {

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool dirtyFullSyncClient(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const ProcessStatus &ps)
{
    std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
    AsyncBdevReader reader(bdevPath);
    std::string encBuf;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb;
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
            const size_t encSize = snappy::Compress(&buf[0], size, &encBuf);
            assert(encSize > 0);
            pkt.write(encSize);
            pkt.write(&encBuf[0], encSize);
        }
        remainingLb -= lb;
        c++;
    }
    packet::Ack(pkt.sock()).recv();
    LOGs.debug() << "number of sent packets" << c;
    return true;
}

namespace dirty_full_sync {

inline void uncompress(const std::vector<char> &src, std::vector<char> &dst, const char *msg)
{
    size_t decSize;
    if (!snappy::GetUncompressedLength(src.data(), src.size(), &decSize)) {
        throw cybozu::Exception(msg) << "GetUncompressedLength" << src.size();
    }
    if (decSize != dst.size()) {
        throw cybozu::Exception(msg) << "decSize differs" << decSize << dst.size();
    }
    if (!snappy::RawUncompress(src.data(), src.size(), dst.data())) {
        throw cybozu::Exception(msg) << "RawUncompress";
    }
}

} // namespace dirty_full_sync

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool dirtyFullSyncServer(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb, bool skipZero)
{
    const char *const FUNC = __func__;
    cybozu::util::File file(bdevPath, O_RDWR);
    const std::vector<char> zeroBuf(bulkLb * LOGICAL_BLOCK_SIZE);
    std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
    std::vector<char> encBuf;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb;
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
                file.write(&zeroBuf[0], size);
            }
        } else {
            encBuf.resize(encSize);
            pkt.read(&encBuf[0], encSize);
            buf.resize(size);
            dirty_full_sync::uncompress(encBuf, buf, FUNC);
            file.write(&buf[0], size);
        }
        remainingLb -= lb;
        progressLb += lb;
		writeSize += size;
		if (writeSize >= MAX_FSYNC_DATA_SIZE) {
            file.fdatasync();
            writeSize = 0;
		}
        c++;
    }
    LOGs.debug() << "fdatasync start";
    file.fdatasync();
    LOGs.debug() << "fdatasync end";
    packet::Ack(pkt.sock()).send();
    LOGs.debug() << "number of received packets" << c;
    return true;
}

} // namespace walb
