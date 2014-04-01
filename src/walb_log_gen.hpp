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
#include "memory_buffer.hpp"

namespace walb {
namespace log {

/**
 * WalbLog Generator for test.
 */
class Generator
{
public:
    struct Config
    {
        uint64_t devLb;
        unsigned int minIoLb;
        unsigned int maxIoLb;
        unsigned int pbs;
        unsigned int maxPackPb;
        unsigned int outLogPb;
        uint64_t lsid;
        bool isPadding;
        bool isDiscard;
        bool isAllZero;
        bool isVerbose;

        void check() const {
            if (!::is_valid_pbs(pbs)) {
                throw RT_ERR("pbs invalid.");
            }
            if (65535 < minIoLb) {
                throw RT_ERR("minSize must be < 512 * 65536 bytes.");
            }
            if (65535 < maxIoLb) {
                throw RT_ERR("maxSize must be < 512 * 65536 bytes.");
            }
            if (maxIoLb < minIoLb) {
                throw RT_ERR("minIoSize must be <= maxIoSize.");
            }
            if (maxPackPb < 1 + ::capacity_pb(pbs, maxIoLb)) {
                throw RT_ERR("maxPackSize must be >= pbs + maxIoSize.");
            }
            if (lsid + outLogPb < lsid) {
                throw RT_ERR("lsid will overflow.");
            }
        }
    };
private:
    const Config& config_;

public:
    Generator(const Config& config)
        : config_(config) {
    }
    void generate(int outFd) {
        generateAndWrite(outFd);
    }
private:
    using Block = std::shared_ptr<uint8_t>;
    using Rand = cybozu::util::Random<uint64_t>;

    static void setUuid(Rand &rand, std::vector<uint8_t> &uuid) {
        const size_t t = sizeof(uint64_t);
        const size_t n = uuid.size() / t;
        const size_t m = uuid.size() % t;
        for (size_t i = 0; i < n; i++) {
            uint64_t v = rand.get64();
            ::memcpy(&uuid[i * t], &v, sizeof(v));
        }
        for (size_t i = 0; i < m; i++) {
            uuid[n * t + i] = static_cast<uint8_t>(rand.get32());
        }
    }

    void generateAndWrite(int fd) {
        Writer writer(fd);
        Rand rand;
        uint64_t writtenPb = 0;
        walb::log::FileHeader wlHead;
        std::vector<uint8_t> uuid(UUID_SIZE);
        setUuid(rand, uuid);

        const uint32_t salt = rand.get32();
        const unsigned int pbs = config_.pbs;
        uint64_t lsid = config_.lsid;
        Block hBlock = cybozu::util::allocateBlocks<uint8_t>(pbs, pbs);
        cybozu::util::BlockAllocator<uint8_t> ba(config_.maxPackPb, pbs, pbs);

        /* Generate and write walb log header. */
        wlHead.init(pbs, salt, &uuid[0], lsid, uint64_t(-1));
        writer.writeHeader(wlHead);
        if (config_.isVerbose) {
            wlHead.print(::stderr);
        }

        uint64_t nPack = 0;
        while (writtenPb < config_.outLogPb) {
            walb::log::PackHeader logh(hBlock.get(), pbs, salt);
            generateLogpackHeader(rand, logh, lsid);
            assert(::is_valid_logpack_header_and_records(&logh.header()));
            uint64_t tmpLsid = lsid + 1;

            /* Prepare blocks and calc checksum if necessary. */
            std::queue<Block> blocks;
            for (unsigned int i = 0; i < logh.nRecords(); i++) {
                RecordWrap rec(&logh, i);
                BlockDataShared blockD(pbs);
                PackIoWrap packIo(&rec, &blockD);

                if (rec.hasData()) {
                    bool isAllZero = false;
                    if (config_.isAllZero) {
                        isAllZero = rand.get32() % 100 < 10;
                    }
                    for (unsigned int j = 0; j < rec.ioSizePb(); j++) {
                        Block b = ba.alloc();
                        ::memset(b.get(), 0, pbs);
                        if (!isAllZero) {
                            ::memcpy(b.get(), &tmpLsid, sizeof(tmpLsid));
                        }
                        tmpLsid++;
                        blockD.addBlock(b);
                        blocks.push(b);
                    }
                }
                if (rec.hasDataForChecksum()) {
                    UNUSED bool ret = packIo.setChecksum();
                    assert(ret);
                    assert(packIo.isValid(true));
                }
            }
            assert(blocks.size() == logh.totalIoSize());

            /* Calculate header checksum and write. */
            logh.updateChecksum();
            writer.writePack(logh, std::move(blocks));

            uint64_t w = 1 + logh.totalIoSize();
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
        Rand &rand, walb::log::PackHeader &logh, uint64_t lsid) {
        logh.init(lsid);
        const unsigned int pbs = config_.pbs;
        const unsigned int maxNumRecords = ::max_n_log_record_in_sector(pbs);
        const size_t nRecords = (rand.get32() % maxNumRecords) + 1;
        const uint64_t devLb = config_.devLb;

        for (size_t i = 0; i < nRecords; i++) {
            uint64_t offset = rand.get64() % devLb;
            /* Decide io_size. */
            uint16_t ioSize = config_.minIoLb;
            uint16_t range = config_.maxIoLb - config_.minIoLb;
            if (0 < range) {
                ioSize += rand.get32() % range;
            }
            if (devLb < offset + ioSize) {
                ioSize = devLb - offset; /* clipping. */
            }
            assert(0 < ioSize);
            /* Check total_io_size limitation. */
            if (0 < logh.totalIoSize() && 1 < nRecords &&
                config_.maxPackPb <
                logh.totalIoSize() + ::capacity_pb(pbs, ioSize)) {
                break;
            }
            /* Decide IO type. */
            unsigned int v = rand.get32() % 100;
            if (config_.isPadding && v < 10) {
                uint16_t psize = capacity_lb(pbs, capacity_pb(pbs, ioSize));
                if (v < 5) { psize = 0; } /* padding size can be 0. */
                if (!logh.addPadding(psize)) { break; }
                continue;
            }
            if (config_.isDiscard && v < 30) {
                if (!logh.addDiscardIo(offset, ioSize)) { break; }
                continue;
            }
            if (!logh.addNormalIo(offset, ioSize)) { break; }
        }
        logh.isValid(false);
    }
};

}} //namespace walb::log
