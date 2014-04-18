#pragma once
#include <vector>
#include <atomic>
#include <snappy.h>
#include "packet.hpp"
#include "fileio.hpp"
#include "walb_logger.hpp"
#include "cybozu/exception.hpp"

namespace walb {

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool dirtyFullSyncClient(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit)
{
    std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
    cybozu::util::BlockDevice bd(bdevPath, O_RDONLY);
    std::string encBuf;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || forceQuit) {
            return false;
        }
        const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        bd.read(&buf[0], size);
        const size_t encSize = snappy::Compress(&buf[0], size, &encBuf);
        pkt.write(encSize);
        pkt.write(&encBuf[0], encSize);
        remainingLb -= lb;
        c++;
    }
    LOGs.debug() << "number of sent packets" << c;
    return true;
}

/**
 * RETURN:
 *   false if force stopped.
 */
inline bool dirtyFullSyncServer(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t sizeLb, uint64_t bulkLb,
    const std::atomic<int> &stopState, const std::atomic<bool> &forceQuit)
{
    const char *const FUNC = __func__;
    cybozu::util::BlockDevice bd(bdevPath, O_RDWR);
    std::vector<char> buf(bulkLb * LOGICAL_BLOCK_SIZE);
    std::vector<char> encBuf;

    uint64_t c = 0;
    uint64_t remainingLb = sizeLb;
    while (0 < remainingLb) {
        if (stopState == ForceStopping || forceQuit) {
            return false;
        }
        const uint16_t lb = std::min<uint64_t>(bulkLb, remainingLb);
        const size_t size = lb * LOGICAL_BLOCK_SIZE;
        size_t encSize;
        pkt.read(encSize);
        if (encSize == 0) {
            throw cybozu::Exception(FUNC) << "encSize is zero";
        }
        encBuf.resize(encSize);
        pkt.read(&encBuf[0], encSize);
        size_t decSize;
        if (!snappy::GetUncompressedLength(&encBuf[0], encSize, &decSize)) {
            throw cybozu::Exception(FUNC)
                << "GetUncompressedLength" << encSize;
        }
        if (decSize != size) {
            throw cybozu::Exception(FUNC)
                << "decSize differs" << decSize << size;
        }
        if (!snappy::RawUncompress(&encBuf[0], encSize, &buf[0])) {
            throw cybozu::Exception(FUNC) << "RawUncompress";
        }
        bd.write(&buf[0], size);
        remainingLb -= lb;
        c++;
    }
    LOGs.debug() << "number of received packets" << c;
    bd.fdatasync();
    return true;
}

} // namespace walb
