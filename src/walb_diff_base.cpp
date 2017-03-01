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
    const char *const NAME = "DiffRecord";
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

std::string DiffRecord::toStr(const char *prefix) const {
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


bool DiffIo::isValid() const
{
    if (empty()) {
        if (!data.empty()) {
            LOGd("Data is not empty.\n");
            return false;
        }
        return true;
    } else {
        if (isCompressed()) {
            return true;
        } else {
            if (getSize() != ioBlocks * LOGICAL_BLOCK_SIZE) {
                LOGd("dataSize is not the same: %zu %u\n"
                     , getSize(), ioBlocks * LOGICAL_BLOCK_SIZE);
                return false;
            }
            return true;
        }
    }
}


void DiffIo::print(::FILE *fp) const
{
    ::fprintf(fp,
              "ioBlocks %u\n"
              "type %d\n"
              "size %zu\n"
              "checksum %0x\n"
              , ioBlocks
              , compressionType
              , getSize()
              , calcChecksum());
}

void DiffIo::printOneline(::FILE *fp) const
{
    ::fprintf(fp, "ioBlocks %u type %d size %zu checksum %0x\n"
              , ioBlocks
              , compressionType
              , getSize()
              , calcChecksum());
}

void DiffIo::set(const DiffRecord &rec, const char *data0)
{
    if (rec.isNormal()) {
        ioBlocks = rec.io_blocks;
        compressionType = rec.compression_type;
        data.resize(rec.data_size, false);
        if (data0) ::memcpy(data.data(), data0, data.size());
    } else {
        ioBlocks = 0;
        compressionType = ::WALB_DIFF_CMPR_NONE;
        data.clear();
    }
}

std::vector<DiffIo> DiffIo::splitIoDataAll(uint32_t ioBlocks0) const
{
    if (ioBlocks0 == 0) {
        throw cybozu::Exception("splitIoDataAll: ioBlocks0 must not be 0.");
    }
    if (isCompressed()) {
        throw cybozu::Exception("splitIoDataAll: compressed IO can not be splitted.");
    }
    assert(isValid());
    std::vector<DiffIo> v;
    uint32_t remaining = ioBlocks;
    const char *p = data.data();
    while (remaining > 0) {
        uint32_t blks = std::min(remaining, ioBlocks0);
        size_t size = blks * LOGICAL_BLOCK_SIZE;
        v.emplace_back(blks, WALB_DIFF_CMPR_NONE, p, size);
        remaining -= blks;
        p += size;
    }
    return v;
}

void compressDiffIo(
    const DiffRecord &inRec, const char *inData,
    DiffRecord &outRec, AlignedArray &outData, int type, int level)
{
    assert(inRec.isNormal());
    assert(!inRec.isCompressed());
    assert(inData != nullptr);

    const size_t size = inRec.io_blocks * LOGICAL_BLOCK_SIZE;
    outData.resize(size + 4096); // margin to reduce malloc at compression.
    size_t outSize;
    walb::Compressor enc(type, level);
    if (enc.run(outData.data(), &outSize, outData.size(), inData, size) && outSize < size) {
        outData.resize(outSize);
    } else {
        outSize = size;
        outData.resize(size);
        ::memcpy(outData.data(), inData, size);
        type = ::WALB_DIFF_CMPR_NONE;
    }
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
    walb::Uncompressor dec(inRec.compression_type);
    size_t outSize = dec.run(outData.data(), size, inData, inRec.data_size);
    if (outSize != size) {
        throw cybozu::Exception("uncompressDiffIo:size is invalid") << outSize << size << inRec;
    }
    outRec = inRec;
    outRec.data_size = size;
    outRec.compression_type = ::WALB_DIFF_CMPR_NONE;
    outRec.checksum = calcChecksum ? calcDiffIoChecksum(outData) : 0;
}

} //namesapce walb
