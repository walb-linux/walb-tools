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

template <typename Buffer>
inline void sendPack(packet::Packet& pkt, packet::StreamControl& ctrl, DiffStatistics& statOut, const Buffer& pack)
{
    ctrl.next();
    pkt.write<size_t>(pack.size());
    pkt.write(pack.data(), pack.size());
    statOut.update(*(const DiffPackHeader*)pack.data());
}

} // namespace wdiff_transfer_local

/**
 * RETURN:
 *   false if force stopped.
 */
bool wdiffTransferClient(
    packet::Packet &pkt, DiffMerger &merger, const CompressOpt &cmpr,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    DiffStatistics &statOut);

/**
 * fileH: the position must be the first pack header.
 */
bool wdiffTransferNoMergeClient(
    packet::Packet &pkt, cybozu::util::File &fileR, const DiffFileHeader &fileH,
    const std::atomic<int> &stopState, const ProcessStatus &ps);

/**
 * Wdiff header must have been written already before calling this.
 *
 * RETURN:
 *   false if force stopped.
 */
bool wdiffTransferServer(
    packet::Packet &pkt, int wdiffOutFd,
    const std::atomic<int> &stopState, const ProcessStatus &ps, uint64_t fsyncIntervalSize);

} // namespace walb
