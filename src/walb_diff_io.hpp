#pragma once

#include <vector>
#include <cassert>
#include "fileio.hpp"
#include "bdev_util.hpp"
#include "walb_diff_base.hpp"
#include "discard_type.hpp"
#include "cybozu/exception.hpp"

namespace walb {

enum IoType
{
    Normal, Discard, Zero, Ignore,
};

inline IoType decideIoType(const DiffRecord& rec, DiscardType discardType)
{
    if (rec.isNormal()) return Normal;
    if (rec.isAllZero()) return Zero;
    assert(rec.isDiscard());

    switch (discardType) {
    case DiscardType::Passdown: return Discard;
    case DiscardType::Ignore: return Ignore;
    case DiscardType::Zero: return Zero;
    default:
        assert(false);
    }
}

/**
 * It's allowed that rec's checksum may not valid.
 */
inline void issueIo(cybozu::util::File& file, DiscardType discardType, const DiffRecord& rec, const DiffIo& io, std::vector<char>& zero)
{
    assert(!rec.isCompressed());
    assert(!io.isCompressed());
    const int type = decideIoType(rec, discardType);
    if (type == Ignore) return;
    if (type == Discard) {
        cybozu::util::issueDiscard(file.fd(), rec.io_address, rec.io_blocks);
        return;
    }
    const uint64_t ioSizeB = rec.io_blocks * LOGICAL_BLOCK_SIZE;
    const char *data;
    if (type == Zero) {
        if (zero.size() < ioSizeB) zero.resize(ioSizeB);
        data = zero.data();
    } else {
        assert(type == Normal);
        data = io.get();
    }
    file.pwrite(data, ioSizeB, rec.io_address * LOGICAL_BLOCK_SIZE);
}

inline void issueDiffPack(cybozu::util::File& file, DiscardType discardType, MemoryDiffPack& pack, std::vector<char>& zero)
{
    const DiffPackHeader& head = pack.header();
    DiffRecord rec;
    DiffIo io;
    for (size_t i = 0; i < head.n_records; i++) {
        const DiffRecord& inRec = head[i];
        if (inRec.isNormal()) {
            if (inRec.isCompressed()) {
                uncompressDiffIo(inRec, pack.data(i), rec, io.data, false);
                io.set(rec);
            } else {
                rec = inRec;
                io.set(rec, pack.data(i));
            }
        } else {
            rec = inRec;
        }
        issueIo(file, discardType, rec, io, zero);
    }
}

} // namespace walb
