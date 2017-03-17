#pragma once
/**
 * @file
 * @brief Converter from wlog to wdiff.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <memory>
#include <cassert>
#include <cstdio>
#include <cstring>

#include <chrono>
#include <thread>

#include "fileio.hpp"
#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "walb_diff_base.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_file.hpp"

namespace walb {

/**
 * Convert a logpack data to a diff data.
 *
 * RETURN:
 *   false if the pack IO is padding data.
 *   true if the pack IO is normal IO or discard or allzero.
 */
bool convertLogToDiff(
    uint32_t pbs, const WlogRecord &rec, const LogBlockShared& blockS,
    DiffRecord& mrec, DiffIo &diffIo, bool calcChecksum);

/**
 * Converter from walb logs to a walb diff.
 */
class DiffConverter /* final */
{
public:
    void convert(int inputLogFd, int outputWdiffFd,
                 uint32_t maxIoBlocks = DEFAULT_MAX_WDIFF_IO_BLOCKS);
private:
    /**
     * Convert a wlog.
     *
     * @lsid begin lsid.
     * @writtenBlocks written logical blocks.
     * @fd input wlog file descriptor.
     * @diffMem walb diff memory manager.
     *
     * RETURN:
     *   true if wlog is remaining, or false.
     */
    bool convertWlog(uint64_t &lsid, uint64_t &writtenBlocks, int fd, DiffMemory &diffMem);
};


bool convertLogToDiff(
    uint32_t pbs, const WlogRecord &rec, const LogBlockShared& blockS,
    DiffIndexRecord& mrec, AlignedArray &buf, bool calcChecksum);


class IndexedDiffConverter /* final */
{
public:
    void convert(int inputLogFd, int outputWdiffFd,
                 uint32_t maxIoBlocks = DEFAULT_MAX_WDIFF_IO_BLOCKS);
private:
    bool convertWlog(uint64_t &lsid, uint64_t &writtenBlocks, int fd,
                     IndexedDiffWriter &writer, DiffFileHeader &wdiffH);

};


} //namespace walb
