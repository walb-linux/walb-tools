#include "walb_diff_io.hpp"


namespace walb {

IoType decideIoType(const DiffRecord& rec, DiscardType discardType)
{
    if (rec.isNormal()) return Normal;
    if (rec.isAllZero()) return Zero;
    assert(rec.isDiscard());

    switch (discardType) {
    case DiscardType::Passdown: return Discard;
    case DiscardType::Ignore: return Ignore;
    case DiscardType::Zero: return Zero;
    }
    throw cybozu::Exception("decideIoType:invalid discard type") << (int)discardType;
}

/**
 * It's allowed that rec's checksum may not valid.
 * @zero is used as zero-filled buffer. It may be resized.
 */
void issueIo(cybozu::util::File& file, DiscardType discardType, const DiffRecord& rec, const char *iodata, AlignedArray& zero)
{
    assert(!rec.isCompressed());
    const int type = decideIoType(rec, discardType);
    if (type == Ignore) return;
    if (type == Discard) {
        cybozu::util::issueDiscard(file.fd(), rec.io_address, rec.io_blocks);
        return;
    }
    const uint64_t ioSizeB = rec.io_blocks * LOGICAL_BLOCK_SIZE;
    const char *data;
    if (type == Zero) {
        if (zero.size() < ioSizeB) zero.resize(ioSizeB, true);
        data = zero.data();
    } else {
        assert(type == Normal);
        assert(iodata != nullptr);
        data = iodata;
    }
    file.pwrite(data, ioSizeB, rec.io_address * LOGICAL_BLOCK_SIZE);
}


void issueAio(AsyncBdevWriter& writer, DiscardType discardType,
              const DiffRecord& rec, AlignedArray&& data)
{
    assert(!rec.isCompressed());
    const int type = decideIoType(rec, discardType);
    if (type == Ignore) return;
    if (type == Discard) {
        writer.discard(rec.io_address, rec.io_blocks);
        return;
    }
    const uint64_t ioSizeB = rec.io_blocks * LOGICAL_BLOCK_SIZE;
    if (type == Zero) {
        AlignedArray buf(ioSizeB, true); // zero-filled.
        writer.prepare(rec.io_address, rec.io_blocks, std::move(buf));
    } else {
        assert(type == Normal);
        assert(!data.empty());
        writer.prepare(rec.io_address, rec.io_blocks, std::move(data));
    }
    writer.submit();
}


/*
 * @zero is used as zero-filled buffer. It may be resized.
 * @return upper offset [logical block].
 */
uint64_t issueDiffPack(cybozu::util::File& file, DiscardType discardType, MemoryDiffPack& pack, AlignedArray& zero)
{
    const DiffPackHeader& head = pack.header();
    DiffRecord rec;
    AlignedArray array; // buffer for uncompressed data.
    uint64_t offLb = 0;
    for (size_t i = 0; i < head.n_records; i++) {
        const DiffRecord& inRec = head[i];
        const char *iodata = nullptr;
        if (inRec.isNormal()) {
            if (inRec.isCompressed()) {
                uncompressDiffIo(inRec, pack.data(i), rec, array, false);
                iodata = array.data();
            } else {
                rec = inRec;
                iodata = pack.data(i);
            }
        } else {
            rec = inRec;
        }
        issueIo(file, discardType, rec, iodata, zero);
        offLb = rec.io_address + rec.io_blocks;
    }
    return offLb;
}

} // namespace walb
