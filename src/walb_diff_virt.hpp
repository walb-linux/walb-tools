#pragma once
/**
 * @file
 * @brief walb diff virtual full image scanner.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <vector>
#include <string>
#include <memory>
#include "cybozu/option.hpp"
#include "fileio.hpp"
#include "walb_diff_base.hpp"
#include "walb_diff_file.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_merge.hpp"
#include "bdev_reader.hpp"


namespace walb {


#define USE_AIO_FOR_VIRT_SCAN
//#undef USE_AIO_FOR_VIRT_SCAN


#ifdef USE_AIO_FOR_VIRT_SCAN
using FileReader = AsyncBdevFileReader;
#else
using FileReader = SyncBdevFileReader;
#endif


/**
 * Virtual full image scanner.
 *
 * (1) Call readAndWriteTo() to write all the data to a file descriptor.
 * (2) Call read() multiple times for various purposes.
 */
class VirtualFullScanner
{
private:
    FileReader reader_;
    DiffMerger merger_;
    uint64_t addr_; /* Indicator of previous read amount [logical block]. */
    DiffRecIo recIo_; /* current diff rec IO. */
    uint32_t offInIo_; /* offset in the IO [logical block]. */
    bool isEndDiff_; /* true if there is no more wdiff IO. */
    bool emptyWdiff_;
    DiffStatistics statOut_;

    void init_inner(FileReader&& reader) {
        reader_ = std::move(reader);
        if (!reader.seekable()) {
            throw cybozu::Exception("non-seekable reader is not allowed");
        }
    }
public:
    /**
     * @baseFd a base image file descriptor.
     *   stdin (non-seekable) or a raw image file or a block device.
     * @wdiffPaths walb diff files. Each wdiff is sorted by time.
     */
    VirtualFullScanner()
        : reader_()
        , merger_()
        , addr_(0)
        , recIo_()
        , offInIo_(0)
        , isEndDiff_(false)
        , emptyWdiff_(false)
        , statOut_() {}

    void init(FileReader&& reader, const StrVec &wdiffPaths);
    void init(FileReader&& reader, std::vector<cybozu::util::File> &&fileV);

    /**
     * Write all data to a specified fd.
     *
     * @outputFd output file descriptor.
     * @bufSize buffer size [byte].
     */
    void readAndWriteTo(int outputFd, size_t bufSize);

    /**
     * Read a specified bytes.
     * @data buffer to be filled.
     * @size size trying to read [byte].
     *   This must be multiples of LOGICAL_BLOCK_SIZE.
     *
     * RETURN:
     *   Read size really [byte].
     *   0 means that the input reached the end.
     */
    size_t readSome(void *data, size_t size);

    /**
     * Try to read sized value.
     *
     * @data buffer to be filled.
     * @size size to read [byte].
     */
    void read(void *data, size_t size);

    const DiffStatistics& statIn() const {
        return merger_.statIn();
    }
    const DiffStatistics& statOut() const {
        return statOut_;
    }
    std::string memUsageStr() const {
        return merger_.memUsageStr();
    }
private:
    /**
     * Read from the base full image.
     * @data buffer.
     * @blks [logical block].
     * RETURN:
     *   really read size [byte].
     */
    size_t readBase(void *data, size_t blks);

    /**
     * Read from the current diff IO.
     * @data buffer.
     * @blks [logical block]. This must be <= remaining size.
     * RETURN:
     *   really read size [byte].
     */
    size_t readWdiff(void *data, size_t blks);

    /**
     * Skip to read the base image.
     */
    void skipBase(size_t blks);

    /**
     * Set recIo_ approximately.
     */
    void fillDiffIo();

    uint64_t currentDiffAddr() const {
        return recIo_.record().io_address + offInIo_;
    }
    uint32_t currentDiffBlocks() const {
        assert(offInIo_ <= recIo_.record().io_blocks);
        return recIo_.record().io_blocks - offInIo_;
    }
};

} //namespace walb
