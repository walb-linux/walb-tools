#pragma once
#include "io_recipe.hpp"
#include "walb_log_file.hpp"
#include "cybozu/exception.hpp"

namespace walb {

namespace local {

inline void verifyLogIoAndPrint(
    const util::IoRecipe &recipe, const WlogRecord &rec,
    const LogBlockShared &blockS, uint32_t salt)
{
    bool isValid = true;
    if (recipe.offset != rec.offset) {
        LOGs.error() << "offset differ" << recipe.offset << rec.offset;
        isValid = false;
    }
    if (recipe.size != rec.io_size) {
        LOGs.error() << "io_size differ" << recipe.size << rec.io_size;
        isValid = false;
    }

    uint32_t csumZ = 0, csumS = 0;
    if (rec.hasDataForChecksum()) {
        /* Validate the log and confirm checksum equality. */
        csumZ = blockS.calcChecksum(rec.io_size, 0);
        csumS = blockS.calcChecksum(rec.io_size, salt);
        isValid = recipe.csum == csumZ && rec.checksum == csumS;
    }

    /* Print result. */
    ::printf("%s %s %s %08x %08x\n",
             isValid ? "OK" : "NG",
             recipe.str().c_str(),
             rec.str().c_str(), csumZ, csumS);
}

} // namespace local

/**
 * Reader must have void read(void *, size_t) member function.
 *
 * @reader logpack stream reader.
 * @bgnLsid lsid to scan from.
 * @endLsid lsid to scan to.
 * @pbs physical block size [byte].
 * @salt checksum salt.
 */
template <typename Reader>
inline void verifyLogStream(
    Reader &reader, util::IoRecipeParser &parser,
    uint64_t bgnLsid, uint64_t endLsid, uint32_t pbs, uint32_t salt)
{
    if (bgnLsid > endLsid) {
        throw cybozu::Exception(__func__)
            << "bad lsid range" << bgnLsid << endLsid;
    }

    LogPackHeader packH(pbs, salt);
    uint64_t lsid = bgnLsid;
    while (lsid < endLsid) {
        if (!readLogPackHeader(reader, packH, lsid)) break;
        LogBlockShared blockS;
        for (size_t i = 0; i < packH.nRecords(); i++) {
            const WlogRecord &rec = packH.record(i);
            if (!readLogIo(reader, packH, i, blockS)) {
                throw cybozu::Exception(__func__)
                    << "read log IO failed" << i << packH;
            }
            const util::IoRecipe recipe = parser.get();
            local::verifyLogIoAndPrint(recipe, rec, blockS, salt);
            blockS.clear();
        }
        lsid = packH.nextLogpackLsid();
    }
    if (!parser.isEnd()) {
        throw cybozu::Exception(__func__) << "There are still remaining recipes";
    }
}

} // namespace walb
