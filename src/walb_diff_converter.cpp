#include "walb_diff_converter.hpp"

namespace walb {

/**
 * drec: checksum will not be calculated.
 */
bool convertLogToDiff(
    const WlogRecord &lrec, const void *data, DiffRecord& drec)
{
    /* Padding */
    if (lrec.isPadding()) return false;

    drec.init();
    drec.io_address = lrec.offset;
    drec.io_blocks = lrec.io_size;

    /* Discard */
    if (lrec.isDiscard()) {
        drec.setDiscard();
        drec.data_size = 0;
        return true;
    }

    /* AllZero */
    const size_t ioSizeB = lrec.io_size * LOGICAL_BLOCK_SIZE;
    if (cybozu::util::isAllZero(data, ioSizeB)) {
        drec.setAllZero();
        drec.data_size = 0;
        return true;
    }

    /* Normal */
    drec.compression_type = ::WALB_DIFF_CMPR_NONE;
    drec.data_offset = 0;
    drec.data_size = ioSizeB;
    drec.checksum = 0;

    return true;
}


void DiffConverter::convert(int inputLogFd, int outputWdiffFd, uint32_t maxIoBlocks)
{
    /* Prepare walb diff. */
    DiffMemory diffMem;
    diffMem.setMaxIoBlocks(maxIoBlocks);

    /* Loop */
    uint64_t lsid = -1;
    uint64_t writtenBlocks = 0;
    while (convertWlog(lsid, writtenBlocks, inputLogFd, diffMem)) {}

#ifdef DEBUG
    /* finalize */
    try {
        LOGd_("Check no overlapped.\n"); /* debug */
        diffMem.checkNoOverlappedAndSorted(); /* debug */
    } catch (std::runtime_error &e) {
        LOGe("checkNoOverlapped failed: %s\n", e.what());
    }
#endif

    /* Get statistics */
    LOGd_("\n"
          "Written blocks: %" PRIu64 "\n"
          "nBlocks: %" PRIu64 "\n"
          "nIos: %" PRIu64 "\n"
          "lsid: %" PRIu64 "\n",
          writtenBlocks, diffMem.getNBlocks(),
          diffMem.getNIos(), lsid);

    diffMem.writeTo(outputWdiffFd, ::WALB_DIFF_CMPR_SNAPPY);
}

bool DiffConverter::convertWlog(uint64_t &lsid, uint64_t &writtenBlocks, int fd, DiffMemory &diffMem)
{
    WlogReader reader(fd);

    /* Read walblog header. */
    WlogFileHeader wlHeader;
    try {
        reader.readHeader(wlHeader);
    } catch (cybozu::util::EofError &e) {
        return false;
    }

    /* Initialize walb diff db. */
    auto verifyUuid = [&]() {
        const cybozu::Uuid diffUuid = diffMem.header().getUuid();
        const cybozu::Uuid logUuid = wlHeader.getUuid();
        if (diffUuid != logUuid) {
            throw cybozu::Exception(__func__) << "uuid differ" << logUuid << diffUuid;
        }
    };
    if (lsid == uint64_t(-1)) {
        /* First time. */
        /* Initialize uuid. */
        diffMem.header().setUuid(wlHeader.getUuid());
        lsid = wlHeader.beginLsid();
    } else {
        /* Second or more. */
        if (lsid != wlHeader.beginLsid()) {
            throw RT_ERR("lsid mismatch.");
        }
        verifyUuid();
    }

    /* Convert each log. */
    WlogRecord lrec;
    AlignedArray buf;
    while (reader.readLog(lrec, buf)) {
        DiffRecord drec;
        if (convertLogToDiff(lrec, buf.data(), drec)) {
            if (!drec.isNormal()) buf.clear();
            diffMem.add(drec, std::move(buf));
            writtenBlocks += drec.io_blocks;
        }
    }

    lsid = reader.endLsid();
    ::fprintf(::stderr, "converted until lsid %" PRIu64 "\n", lsid);
    return true;
}

void IndexedDiffConverter::convert(int inputLogFd, int outputWdiffFd, uint32_t maxIoBlocks)
{
    IndexedDiffWriter writer;
    writer.setFd(outputWdiffFd);
    writer.setMaxIoBlocks(maxIoBlocks);
    DiffFileHeader wdiffH;

    /* Loop */
    uint64_t lsid = -1;
    uint64_t writtenBlocks = 0;
    while (convertWlog(lsid, writtenBlocks, inputLogFd, writer, wdiffH)) {}

#ifdef DEBUG
    /* finalize */
    try {
        LOGd_("Check no overlapped.\n"); /* debug */
        writer.checkNoOverlappedAndSorted(); /* debug */
    } catch (std::runtime_error &e) {
        LOGe("checkNoOverlapped failed: %s\n", e.what());
    }
#endif

    /* Get statistics */
    LOGd_("\n"
          "Written blocks: %" PRIu64 "\n"
          "lsid: %" PRIu64 "\n",
          writtenBlocks, lsid);

    writer.finalize();
}

bool IndexedDiffConverter::convertWlog(
    uint64_t &lsid, uint64_t &writtenBlocks, int fd,
    IndexedDiffWriter &writer, DiffFileHeader &wdiffH)
{
    WlogReader reader(fd);

    /* Read walblog header. */
    WlogFileHeader wlHeader;
    try {
        reader.readHeader(wlHeader);
    } catch (cybozu::util::EofError &e) {
        return false;
    }

    /* Initialize walb diff db. */
    auto verifyUuid = [&]() {
        const cybozu::Uuid diffUuid = wdiffH.getUuid();
        const cybozu::Uuid logUuid = wlHeader.getUuid();
        if (diffUuid != logUuid) {
            throw cybozu::Exception(__func__) << "uuid differ" << logUuid << diffUuid;
        }
    };
    if (lsid == uint64_t(-1)) {
        /* First time. */
        /* Initialize uuid. */
        wdiffH.setUuid(wlHeader.getUuid());
        wdiffH.type = WALB_DIFF_TYPE_INDEXED;
        writer.writeHeader(wdiffH);

        lsid = wlHeader.beginLsid();
    } else {
        /* Second or more. */
        if (lsid != wlHeader.beginLsid()) {
            throw RT_ERR("lsid mismatch.");
        }
        verifyUuid();
    }

    /* Convert each log. */
    WlogRecord lrec;
    AlignedArray buf;
    while (reader.readLog(lrec, buf)) {
        IndexedDiffRecord drec;
        if (convertLogToDiff(lrec, buf.data(), drec)) {
            writer.compressAndWriteDiff(drec, buf.data());
            writtenBlocks += drec.io_blocks;
        }
    }
    lsid = reader.endLsid();
    ::fprintf(::stderr, "converted until lsid %" PRIu64 "\n", lsid);
    return true;
}


/**
 * drec: checksums will be not set.
 * data: uncompressed data buffer with size lrec.ioSizeLb() * LOGICAL_BLOCK_SIZE.
 */
bool convertLogToDiff(const WlogRecord &lrec, const void *data, IndexedDiffRecord& drec)
{
    /* Padding */
    if (lrec.isPadding()) return false;

    drec.init();
    drec.io_address = lrec.offset;
    drec.io_blocks = lrec.io_size;

    /* Discard */
    if (lrec.isDiscard()) {
        drec.setDiscard();
        drec.data_size = 0;
        drec.orig_blocks = 0;
        return true;
    }

    /* AllZero */
    const size_t ioSizeB = lrec.io_size * LOGICAL_BLOCK_SIZE;
    if (cybozu::util::isAllZero(data, ioSizeB)) {
        drec.setAllZero();
        drec.data_size = 0;
        drec.orig_blocks = 0;
        return true;
    }

    /* Normal */
    assert(drec.isNormal());
    drec.orig_blocks = lrec.io_size;
    drec.compression_type = ::WALB_DIFF_CMPR_NONE;
    drec.data_offset = 0; // updated later.
    drec.io_offset = 0;
    drec.data_size = ioSizeB;

    // Checksums are not be calculated.

    return true;
}


} //namespace walb
