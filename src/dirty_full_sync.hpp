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
#include "bdev_writer.hpp"
#include "full_repl_state.hpp"
#include "snappy_util.hpp"
#include "cybozu/exception.hpp"
#include "throughput_util.hpp"
#include "server_util.hpp"

namespace walb {

/**
 * sizeLb is total size.
 *
 * RETURN:
 *   false if force stopped.
 */
bool dirtyFullSyncClient(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb, const CompressOpt& cmprOpt,
    const std::atomic<int> &stopState, const ProcessStatus &ps,
    const std::atomic<uint64_t>& maxLbPerSec);

/**
 * sizeLb is total size.
 * fullReplSt, fullReplStDir, and fullREplStFileName must be specified together.
 *
 * fsyncIntervalSize [bytes]
 *
 * RETURN:
 *   false if force stopped.
 */
 bool dirtyFullSyncServer(
    packet::Packet &pkt, const std::string &bdevPath,
    uint64_t startLb, uint64_t sizeLb, uint64_t bulkLb, const CompressOpt& cmprOpt,
    const std::atomic<int> &stopState, const ProcessStatus &ps, std::atomic<uint64_t> &progressLb,
    bool skipZero, uint64_t fsyncIntervalSize,
    FullReplState *fullReplSt = nullptr, const cybozu::FilePath &fullReplStDir = cybozu::FilePath(),
    const std::string &fullReplStFileName = "");

} // namespace walb
