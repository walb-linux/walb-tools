#include "walb_log_gen.hpp"

namespace walb {

void WlogGenerator::Config::check() const
{
    if (!::is_valid_pbs(pbs)) {
        throw RT_ERR("pbs invalid.");
    }
    if (minIoLb > UINT16_MAX) {
        throw RT_ERR("minIoSize must be less than 32MiB.");
    }
    if (maxIoLb > UINT16_MAX) {
        throw RT_ERR("maxIoSize must be less than 32MiB.");
    }
    if (maxIoLb < minIoLb) {
        throw RT_ERR("minIoSize must be <= maxIoSize.");
    }
    if (minDiscardLb > UINT32_MAX) {
        throw RT_ERR("minDiscardSize must be less than 2TB.");
    }
    if (maxDiscardLb > UINT32_MAX) {
        throw RT_ERR("maxDiscardSize must be less than 2TB.");
    }
    if (maxDiscardLb < minDiscardLb) {
        throw RT_ERR("minDiscardSize must be <= maxDiscardSize.");
    }
    if (lsid + outLogPb < lsid) {
        throw RT_ERR("lsid will overflow.");
    }
}

void WlogGenerator::generateAndWrite(int fd)
{
    WlogWriter writer(fd);
    Rand rand;
    uint64_t writtenPb = 0;
    WlogFileHeader wlHead;
    cybozu::Uuid uuid;
    uuid.setRand(rand);
    const uint32_t salt = rand.get32();
    const uint32_t pbs = config_.pbs;
    uint64_t lsid = config_.lsid;

    /* Generate and write walb log header. */
    wlHead.init(pbs, salt, uuid, lsid, uint64_t(-1));
    writer.writeHeader(wlHead);
    if (config_.isVerbose) {
        std::cerr << wlHead << std::endl;
    }

    uint64_t nPack = 0;
    LogPackHeader packH(pbs, salt);
    while (writtenPb < config_.outLogPb) {
        generateLogpackHeader(rand, packH, lsid);
        assert(::is_valid_logpack_header_and_records(&packH.header()));
        uint64_t tmpLsid = lsid + 1;

        /* Prepare blocks and calc checksum if necessary. */
        std::queue<AlignedArray> ioQ;
        for (uint32_t i = 0; i < packH.nRecords(); i++) {
            WlogRecord &rec = packH.record(i);
            if (rec.hasData()) {
                bool isAllZero = false;
                if (config_.isAllZero) {
                    isAllZero = rand.get32() % 100 < 10;
                }
                ChecksumCalculator cc(rec.io_size, salt);
                const uint32_t ioSizePb = rec.ioSizePb(pbs);
                AlignedArray buf(ioSizePb * pbs, true);
                for (uint32_t j = 0; j < ioSizePb; j++) {
                    const size_t off = pbs * j;
                    AlignedArray b(pbs, true);
                    if (!isAllZero) {
                        if (config_.isRandom) {
                            rand.fill(buf.data() + off, pbs);
                        } else {
                            ::memcpy(buf.data() + off, &tmpLsid, sizeof(tmpLsid));
                        }
                    }
                    tmpLsid++;
                    if (rec.hasDataForChecksum()) {
                        cc.update(buf.data() + off, pbs);
                    }
                }
                ioQ.push(std::move(buf));
                if (rec.hasDataForChecksum()) {
                    rec.checksum = cc.get();
                }
            }
        }
        assert(ioQ.size() == packH.nRecordsHavingData());

        /* Calculate header checksum and write. */
        packH.updateChecksum();
        writer.writePack(packH, std::move(ioQ));

        uint64_t w = 1 + packH.totalIoSize();
        assert(tmpLsid == lsid + w);
        writtenPb += w;
        lsid += w;
        nPack++;

        if (config_.isVerbose) {
            ::fprintf(::stderr, ".");
            if (nPack % 80 == 79) {
                ::fprintf(::stderr, "\n");
            }
            ::fflush(::stderr);
        }
    }
    writer.close();

    if (config_.isVerbose) {
        ::fprintf(::stderr,
                  "\n"
                  "nPack: %" PRIu64 "\n"
                  "written %" PRIu64 " physical blocks\n",
                  nPack, writtenPb);
    }
}

void WlogGenerator::generateLogpackHeader(
    Rand &rand, LogPackHeader &packH, uint64_t lsid)
{
    packH.init(lsid);
    const uint32_t pbs = config_.pbs;
    const uint32_t maxNumRecords = ::max_n_log_record_in_sector(pbs);
    const size_t nRecords = (rand.get32() % maxNumRecords) + 1;
    const uint64_t devLb = config_.devLb;

    for (size_t i = 0; i < nRecords; i++) {
        uint64_t offset = rand.get64() % devLb;
        /* Decide IO type. */
        bool isDiscard = false;
        bool isPadding = false;
        bool isPaddingZero = false;
        {
            const size_t v = rand.get32() % 100;
            if (config_.isPadding && v < 10) {
                isPadding = true;
                if (v < 5) isPaddingZero = true;
            } else if (config_.isDiscard && v < 30) {
                isDiscard = true;
            }
        }
        /* Decide io_size. */
        uint32_t minIoLb, maxIoLb;
        if (isDiscard) {
            minIoLb = config_.minDiscardLb;
            maxIoLb = config_.maxDiscardLb;
        } else {
            minIoLb = config_.minIoLb;
            maxIoLb = config_.maxIoLb;
        }
        uint32_t ioSize = minIoLb;
        uint32_t range = maxIoLb - minIoLb;
        if (0 < range) {
            ioSize += rand.get64() % range;
        }
        if (devLb < offset + ioSize) {
            ioSize = devLb - offset; /* clipping. */
        }
        assert(0 < ioSize);
        /* Check total_io_size limitation. */
        if (0 < packH.totalIoSize() && 1 < nRecords && !isDiscard
            && config_.maxPackPb < packH.totalIoSize() + ::capacity_pb(pbs, ioSize)) {
            break;
        }
        /* Decide IO type. */
        if (isPadding) {
            uint32_t psize = capacity_lb(pbs, capacity_pb(pbs, ioSize));
            if (isPaddingZero) psize = 0; /* padding size can be 0. */
            assert(psize <= UINT16_MAX);
            if (!packH.addPadding(psize)) break;
            continue;
        }
        if (isDiscard) {
            if (!packH.addDiscardIo(offset, ioSize)) break;
            continue;
        }
        assert(ioSize <= UINT16_MAX);
        if (!packH.addNormalIo(offset, ioSize)) break;
    }
    packH.isValid(false);
}

} //namespace walb
