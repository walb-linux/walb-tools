#include "walb_diff_converter.hpp"

namespace walb {

bool convertLogToDiff(
    uint32_t pbs, const WlogRecord &rec, const LogBlockShared& blockS,
    DiffRecord& mrec, DiffIo &diffIo)
{
    /* Padding */
    if (rec.isPadding()) return false;

    mrec.init();
    mrec.io_address = rec.offset;
    mrec.io_blocks = rec.io_size;

    /* Discard */
    if (rec.isDiscard()) {
        mrec.setDiscard();
        mrec.data_size = 0;
        diffIo.set(mrec);
        return true;
    }

    /* AllZero */
    if (blockS.isAllZero(rec.ioSizeLb())) {
        mrec.setAllZero();
        mrec.data_size = 0;
        diffIo.set(mrec);
        return true;
    }

    /* Copy data from logpack data to diff io data. */
    assert(0 < rec.ioSizeLb());
    const size_t ioSizeB = rec.ioSizeLb() * LOGICAL_BLOCK_SIZE;
    AlignedArray buf(ioSizeB, false);
    size_t remaining = ioSizeB;
    size_t off = 0;
    const uint32_t ioSizePb = rec.ioSizePb(pbs);
    for (size_t i = 0; i < ioSizePb; i++) {
        const size_t copySize = std::min<size_t>(pbs, remaining);
        ::memcpy(&buf[off], blockS.get(i), copySize);
        off += copySize;
        remaining -= copySize;
    }
    diffIo.set(mrec);
    diffIo.data.swap(buf);

    /* Compression. (currently NONE). */
    mrec.compression_type = ::WALB_DIFF_CMPR_NONE;
    mrec.data_offset = 0;
    mrec.data_size = ioSizeB;

    /* Checksum. */
    mrec.checksum = diffIo.calcChecksum();

    return true;
}


void DiffConverter::convert(int inputLogFd, int outputWdiffFd, uint32_t maxIoBlocks)
{
    /* Prepare walb diff. */
    DiffMemory diffMem(maxIoBlocks);

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

    /* Block buffer. */
    const uint32_t pbs = wlHeader.pbs();

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
    LogBlockShared blockS;
    while (reader.readLog(lrec, blockS)) {
        DiffRecord diffRec;
        DiffIo diffIo;
        if (convertLogToDiff(pbs, lrec, blockS, diffRec, diffIo)) {
            diffMem.add(diffRec, std::move(diffIo));
            writtenBlocks += diffRec.io_blocks;
        }
    }

    lsid = reader.endLsid();
    ::fprintf(::stderr, "converted until lsid %" PRIu64 "\n", lsid);
    return true;
}

} //namespace walb
