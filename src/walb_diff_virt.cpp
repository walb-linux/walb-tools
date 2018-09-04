#include "walb_diff_virt.hpp"

namespace walb {

void VirtualFullScanner::init(FileReader&& reader, const StrVec &wdiffPaths)
{
    init_inner(std::move(reader));
    emptyWdiff_ = wdiffPaths.empty();
    if (!emptyWdiff_) {
        merger_.addWdiffs(wdiffPaths);
        merger_.prepare();
    }
    statOut_.clear();
}

void VirtualFullScanner::init(FileReader&& reader, std::vector<cybozu::util::File> &&fileV)
{
    init_inner(std::move(reader));
    emptyWdiff_ = fileV.empty();
    if (!emptyWdiff_) {
        merger_.addWdiffs(std::move(fileV));
        merger_.prepare();
    }
    statOut_.clear();
}

void VirtualFullScanner::readAndWriteTo(int outputFd, size_t bufSize)
{
    cybozu::util::File writer(outputFd);
    AlignedArray buf(bufSize, false);
    for (;;) {
        const size_t rSize = readSome(buf.data(), buf.size());
        if (rSize == 0) break;
        writer.write(buf.data(), rSize);
    }
    writer.fdatasync();
}

size_t VirtualFullScanner::readSome(void *data, size_t size)
{
    assert(size % LOGICAL_BLOCK_SIZE == 0);

    const uint32_t blks = size / LOGICAL_BLOCK_SIZE;
    assert(blks * LOGICAL_BLOCK_SIZE == size);

    fillDiffIo();
    if (emptyWdiff_ || isEndDiff_) {
        /* There is no remaining diff IOs. */
        return readBase(data, blks);
    }

    uint64_t diffAddr = currentDiffAddr();
    assert(addr_ <= diffAddr);
    if (addr_ == diffAddr) {
        /* Read wdiff IO partially. */
        const size_t blks0 = std::min(blks, currentDiffBlocks());
        return readWdiff(data, blks0);
    }
    /* Read the base image. */
    uint32_t blks0 = blks;
    uint64_t blksToIo = diffAddr - addr_;
    if (blksToIo < blks) {
        blks0 = blksToIo;
    }
    reader_.readAhead(blksToIo * LOGICAL_BLOCK_SIZE);
    return readBase(data, blks0);
}

void VirtualFullScanner::read(void *data, size_t size)
{
    char *p = (char *)data;
    while (0 < size) {
        size_t r = readSome(p, size);
        if (r == 0) throw cybozu::util::EofError();
        p += r;
        size -= r;
    }
}

size_t VirtualFullScanner::readBase(void *data, size_t blks)
{
    char *p = (char *)data;
    size_t size = blks * LOGICAL_BLOCK_SIZE;
    while (0 < size) {
        size_t r = reader_.readsome(p, size);
        if (r == 0) break;
        p += r;
        size -= r;
    }
    if (size % LOGICAL_BLOCK_SIZE != 0) {
        throw std::runtime_error(
            "input data is not a multiples of LOGICAL_BLOCK_SIZE.");
    }
    size_t readLb = blks - size / LOGICAL_BLOCK_SIZE;
    addr_ += readLb;
    return readLb * LOGICAL_BLOCK_SIZE;
}


size_t VirtualFullScanner::readWdiff(void *data, size_t blks)
{
    assert(recIo_.isValid());
    const DiffRecord& rec = recIo_.record();
    const AlignedArray &buf = recIo_.io();
    assert(offInIo_ < rec.io_blocks);
    if (rec.isNormal()) {
        size_t off = offInIo_ * LOGICAL_BLOCK_SIZE;
        ::memcpy(data, buf.data() + off, blks * LOGICAL_BLOCK_SIZE);
    } else {
        /* Read zero image for both ALL_ZERO and DISCARD.. */
        assert(rec.isDiscard() || rec.isAllZero());
        ::memset(data, 0, blks * LOGICAL_BLOCK_SIZE);
    }
    offInIo_ += blks;
    assert(offInIo_ <= rec.io_blocks);
    skipBase(blks);
    addr_ += blks;
    return blks * LOGICAL_BLOCK_SIZE;
}


void VirtualFullScanner::skipBase(size_t blks)
{
    reader_.lseek(blks * LOGICAL_BLOCK_SIZE, SEEK_CUR);
}


void VirtualFullScanner::fillDiffIo()
{
    if (emptyWdiff_ || isEndDiff_) return;
    const DiffRecord& rec = recIo_.record();
    /* At beginning time, rec.ioBlocks() returns 0. */
    assert(offInIo_ <= rec.io_blocks);
    if (offInIo_ == rec.io_blocks) {
        offInIo_ = 0;
        if (!merger_.getAndRemove(recIo_)) {
            isEndDiff_ = true;
            recIo_ = DiffRecIo();
            statOut_.wdiffNr = -1;
            statOut_.dataSize = -1;
        }
        statOut_.update(recIo_.record());
    }
}

} //namespace walb
