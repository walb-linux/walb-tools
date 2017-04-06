#include "walb_diff_base.hpp"

namespace walb {

bool DiffRecord::isValid() const
{
    try {
        verify();
        return true;
    } catch (...) {
        return false;
    }
}

void DiffRecord::verify() const
{
    if (!isNormal()) {
        if (isAllZero() && isDiscard()) {
            throw cybozu::Exception(NAME) << "allzero and discard flag is exclusive";
        }
        return;
    }
    if (::WALB_DIFF_CMPR_MAX <= compression_type) {
        throw cybozu::Exception(NAME) << "compression type is invalid";
    }
    if (io_blocks == 0) {
        throw cybozu::Exception(NAME) << "io_blocks must not be 0";
    }
}

void DiffRecord::print(::FILE *fp) const
{
    ::fprintf(fp, "----------\n"
              "ioAddress: %" PRIu64 "\n"
              "ioBlocks: %u\n"
              "compressionType: %u (%s)\n"
              "dataOffset: %u\n"
              "dataSize: %u\n"
              "checksum: %08x\n"
              "isAllZero: %d\n"
              "isDiscard: %d\n"
              , io_address, io_blocks
              , compression_type, compressionTypeToStr(compression_type).c_str()
              , data_offset, data_size
              , checksum, isAllZero(), isDiscard());
}

std::string DiffRecord::toStr(const char *prefix) const
{
    return cybozu::util::formatString(
        "%s""%" PRIu64 "\t%u\t%s\t%u\t%u\t%08x\t%c%c"
        , prefix, io_address, io_blocks
        , compressionTypeToStr(compression_type).c_str()
        , data_offset, data_size, checksum
        , isAllZero() ? 'Z' : '-', isDiscard() ? 'D' : '-');
}

std::vector<DiffRecord> DiffRecord::splitAll(uint32_t ioBlocks0) const
{
    if (ioBlocks0 == 0) {
        throw cybozu::Exception("splitAll: ioBlocks0 must not be 0.");
    }
    if (isCompressed()) {
        throw cybozu::Exception("splitAll: compressed data can not be splitted.");
    }
    std::vector<DiffRecord> v;
    uint64_t addr = io_address;
    uint32_t remaining = io_blocks;
    const bool isNormal = this->isNormal();
    while (remaining > 0) {
        uint32_t blks = std::min(ioBlocks0, remaining);
        v.push_back(*this);
        DiffRecord& r = v.back();
        r.io_address = addr;
        r.io_blocks = blks;
        if (isNormal) {
            r.data_size = blks * LOGICAL_BLOCK_SIZE;
        }
        addr += blks;
        remaining -= blks;
    }
    assert(!v.empty());
    return v;
}


std::vector<AlignedArray> splitIoDataAll(const AlignedArray &buf, uint32_t ioBlocks)
{
    std::vector<AlignedArray> v;
    assert(buf.size() % LOGICAL_BLOCK_SIZE == 0);
    uint32_t remaining = buf.size() / LOGICAL_BLOCK_SIZE;
    const char *p = buf.data();
    while (remaining > 0) {
        uint32_t lb = std::min(remaining, ioBlocks);
        size_t bytes = lb * LOGICAL_BLOCK_SIZE;
        v.emplace_back(bytes);
        ::memcpy(v.back().data(), p, bytes);
        remaining -= lb;
        p += bytes;
    }
    return v;
}


int compressData(const char *inData, size_t inSize,
                 AlignedArray &outData, size_t &outSize, int type, int level)
{
    outData.resize(inSize + 4096); // margin to reduce malloc at compression.
    walb::Compressor enc(type, level);
    if (enc.run(outData.data(), &outSize, outData.size(), inData, inSize) && outSize < inSize) {
        outData.resize(outSize);
    } else {
        outSize = inSize;
        outData.resize(inSize);
        ::memcpy(outData.data(), inData, inSize);
        type = ::WALB_DIFF_CMPR_NONE;
    }
    return type;
}

void uncompressData(const char *inData, size_t inSize, AlignedArray &outData, int type)
{
    walb::Uncompressor dec(type);
    size_t outSize = dec.run(outData.data(), outData.size(), inData, inSize);
    if (outSize != outData.size()) {
        throw cybozu::Exception("uncompressData:size is invalid") << outSize << outData.size();
    }
}

void compressDiffIo(
    const DiffRecord &inRec, const char *inData,
    DiffRecord &outRec, AlignedArray &outData, int type, int level)
{
    assert(inRec.isNormal());
    assert(!inRec.isCompressed());
    assert(inData != nullptr);

    const size_t inSize = inRec.io_blocks * LOGICAL_BLOCK_SIZE;
    size_t outSize = 0;
    type = compressData(inData, inSize, outData, outSize, type, level);

    outRec = inRec;
    outRec.compression_type = type;
    outRec.data_size = outSize;
    outRec.checksum = calcDiffIoChecksum(outData);
}

void uncompressDiffIo(
    const DiffRecord &inRec, const char *inData,
    DiffRecord &outRec, AlignedArray &outData, bool calcChecksum)
{
    assert(inRec.isNormal());
    assert(inRec.isCompressed());
    assert(inData != nullptr);

    const size_t size = inRec.io_blocks * LOGICAL_BLOCK_SIZE;
    outData.resize(size, false);
    uncompressData(inData, inRec.data_size, outData, inRec.compression_type);

    outRec = inRec;
    outRec.data_size = size;
    outRec.compression_type = ::WALB_DIFF_CMPR_NONE;
    outRec.checksum = calcChecksum ? calcDiffIoChecksum(outData) : 0;
}


std::string IndexedDiffRecord::toStr(const char *prefix) const
{
    return cybozu::util::formatString(
        "%s""%" PRIu64 "\t%u\t%s\t%" PRIu64 "\t%u\t%u\t%u\t%08x\t%08x\t%c%c"
        , prefix, io_address, io_blocks
        , compressionTypeToStr(compression_type).c_str()
        , data_offset, data_size, orig_blocks, io_offset
        , io_checksum, rec_checksum
        , isAllZero() ? 'Z' : '-', isDiscard() ? 'D' : '-');
}

std::vector<IndexedDiffRecord> IndexedDiffRecord::split(uint32_t maxIoBlocks) const
{
    const std::vector<uint32_t> sizeV =
        splitIoToAligned(io_address, io_blocks, maxIoBlocks);

    uint64_t addr = io_address;
    uint32_t off = io_offset;

    std::vector<IndexedDiffRecord> recV;
    recV.reserve(sizeV.size());

    for (uint32_t s : sizeV) {
        recV.push_back(*this);
        IndexedDiffRecord& rec = recV.back();

        rec.io_address = addr;
        rec.io_blocks = s;
        rec.io_offset = off;
        rec.updateRecChecksum();

        addr += s;
        off += s;
    }

    return recV;
}

std::vector<IndexedDiffRecord> IndexedDiffRecord::minus(const IndexedDiffRecord& rhs) const
{
    const IndexedDiffRecord &lhs = *this;

    assert(lhs.isOverlapped(rhs));
    std::vector<IndexedDiffRecord> v;
    /*
     * Pattern 1:
     * __oo__ - xxxxxx = ______
     */
    if (lhs.isOverwrittenBy(rhs)) return v; /* empty */

    /*
     * Pattern 2:
     * oooooo - __xx__ = oo__oo
     */
    if (rhs.isOverwrittenBy(lhs)) {
        uint32_t blks0 = rhs.io_address - lhs.io_address;
        uint32_t blks1 = lhs.endIoAddress() - rhs.endIoAddress();
        uint64_t addr0 = lhs.io_address;
        uint64_t addr1 = rhs.endIoAddress();

        IndexedDiffRecord r0 = lhs;
        IndexedDiffRecord r1 = lhs;
        r0.io_address = addr0;
        r0.io_blocks = blks0;
        // r0.io_offset need not to change.
        r1.io_address = addr1;
        r1.io_blocks = blks1;
        r1.io_offset += addr1 - addr0;

        if (blks0 > 0) {
            r0.updateRecChecksum();
            v.push_back(r0);
        }
        if (blks1 > 0) {
            r1.updateRecChecksum();
            v.push_back(r1);
        }
        return v;
    }

    /*
     * Pattern 3 does not exist.
     * oooo__ - __xxxx = oo____
     *
     * Pattern 4 does not exist.
     * __oooo - xxxx__ = ____oo
     */
    throw cybozu::Exception("BUG: IndexedDiffRecord:minus.") << lhs << rhs;
}

void IndexedDiffRecord::updateRecChecksum()
{
    rec_checksum = 0;
    rec_checksum = cybozu::util::calcChecksum(this, sizeof(*this), 0);
#ifndef NDEBUG
    verify();
#endif
}

bool IndexedDiffRecord::verifyDetail(bool throwError, bool doChecksum) const
{
    if (!isNormal()) {
        if (isAllZero() && isDiscard()) {
            if (throwError) {
                throw cybozu::Exception(NAME)
                    << "allzero and discard flag must be exclusive";
            }
            return false;
        }
        return true;
    }
    if (::WALB_DIFF_CMPR_MAX <= compression_type) {
        if (throwError) {
            throw cybozu::Exception(NAME) << "compression type is invalid";
        }
        return false;
    }
    if (io_blocks == 0) {
        if (throwError) {
            throw cybozu::Exception(NAME) << "io_blocks must not be 0";
        }
        return false;
    }

    if (!doChecksum) return true;

    if (cybozu::util::calcChecksum(this, sizeof(*this), 0) != 0) {
        if (throwError) {
            throw cybozu::Exception(NAME) << "invalid checksum";
        }
        return false;
    }

    return true;
}

} //namesapce walb
