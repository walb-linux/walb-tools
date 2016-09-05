#pragma once
/**
 * @file
 * @brief Wlog generator for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */

#include <vector>
#include <memory>
#include <cassert>
#include <cstdio>
#include <cstring>

#include "random.hpp"
#include "util.hpp"
#include "walb_log_base.hpp"
#include "walb_log_file.hpp"

namespace walb {

/**
 * Wlog generator for test.
 */
class WlogGenerator
{
public:
    struct Config
    {
        uint64_t devLb;
        uint32_t minIoLb;
        uint32_t maxIoLb;
        uint32_t pbs;
        uint32_t maxPackPb;
        uint32_t outLogPb;
        uint64_t lsid;
        bool isPadding;
        bool isDiscard;
        bool isAllZero;
        bool isRandom;
        bool isVerbose;

        void check() const {
            if (!::is_valid_pbs(pbs)) {
                throw RT_ERR("pbs invalid.");
            }
            if (maxIoLb < minIoLb) {
                throw RT_ERR("minIoSize must be <= maxIoSize.");
            }
            if (lsid + outLogPb < lsid) {
                throw RT_ERR("lsid will overflow.");
            }
        }
    };
private:
    const Config& config_;

public:
    WlogGenerator(const Config& config)
        : config_(config) {
    }
    void generate(int outFd) {
        generateAndWrite(outFd);
    }
private:
    using Rand = cybozu::util::Random<uint64_t>;

    void generateAndWrite(int fd) {
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
            std::queue<LogBlockShared> blockSQ;
            for (uint32_t i = 0; i < packH.nRecords(); i++) {
                WlogRecord &rec = packH.record(i);
                ChecksumCalculator cc(rec.io_size, salt);

                if (rec.hasData()) {
                    bool isAllZero = false;
                    if (config_.isAllZero) {
                        isAllZero = rand.get32() % 100 < 10;
                    }
                    const uint32_t ioSizePb = rec.ioSizePb(pbs);
                    LogBlockShared blockS(pbs);
                    for (uint32_t j = 0; j < ioSizePb; j++) {
                        AlignedArray b(pbs, true);
                        if (!isAllZero) {
                            if (config_.isRandom) {
                                rand.fill(b.data(), b.size());
                            } else {
                                ::memcpy(b.data(), &tmpLsid, sizeof(tmpLsid));
                            }
                        }
                        tmpLsid++;
                        cc.update(b.data(), b.size());
                        blockS.addBlock(std::move(b));
                    }
                    blockSQ.push(std::move(blockS));
                }
                if (rec.hasDataForChecksum()) {
                    rec.checksum = cc.get();
                }
            }
            assert(blockSQ.size() == packH.nRecordsHavingData());

            /* Calculate header checksum and write. */
            packH.updateChecksum();
            writer.writePack(packH, std::move(blockSQ));

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

    /**
     * Generate logpack header randomly.
     */
    void generateLogpackHeader(
        Rand &rand, LogPackHeader &packH, uint64_t lsid) {
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
            uint32_t minIoLb = config_.minIoLb;
            uint32_t maxIoLb = config_.maxIoLb;
            /* Non-discard IO size must be UINT16_MAX [logical block]. */
            if (!isDiscard) {
                if (minIoLb > UINT16_MAX) minIoLb = UINT16_MAX;
                if (maxIoLb > UINT16_MAX) maxIoLb = UINT16_MAX;
            }
            uint32_t ioSize = minIoLb;
            uint32_t range = maxIoLb - minIoLb;
            if (0 < range) {
                ioSize += rand.get32() % range;
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
};

} //namespace walb
