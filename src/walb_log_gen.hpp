#pragma once
/**
 * @file
 * @brief Wlog generator for test.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */

#include <vector>
#include <random>
#include <memory>
#include <cassert>
#include <cstdio>
#include <cstring>

#include "util.hpp"
#include "walb_log.hpp"
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
    uint64_t lsid_;

    class Rand
    {
    private:
        std::random_device rd_;
        std::mt19937 gen_;
        std::uniform_int_distribution<uint32_t> dist32_;
        std::uniform_int_distribution<uint64_t> dist64_;
        std::poisson_distribution<uint16_t> distp_;
    public:
        Rand()
            : rd_()
            , gen_(rd_())
            , dist32_(0, UINT32_MAX)
            , dist64_(0, UINT64_MAX)
            , distp_(4) {}

        uint32_t get32() { return dist32_(gen_); }
        uint64_t get64() { return dist64_(gen_); }
        uint16_t getp() { return distp_(gen_); }
    };
public:
    Generator(const Config& config)
        : config_(config)
        , lsid_(config.lsid) {}

    void generate(int outFd) {
        generateAndWrite(outFd);
    }
private:
    using Block = std::shared_ptr<u8>;

    void setUuid(Rand &rand, std::vector<u8> &uuid) {
        const size_t t = sizeof(uint64_t);
        const size_t n = uuid.size() / t;
        const size_t m = uuid.size() % t;
        for (size_t i = 0; i < n; i++) {
            *reinterpret_cast<uint64_t *>(&uuid[i * t]) = rand.get64();
        }
        for (size_t i = 0; i < m; i++) {
            uuid[n * t + i] = static_cast<u8>(rand.get32());
        }
    }

    void generateAndWrite(int fd) {
        Rand rand;
        uint64_t writtenPb = 0;
        walb::log::FileHeader wlHead;
        std::vector<u8> uuid(UUID_SIZE);
        setUuid(rand, uuid);

        const uint32_t salt = rand.get32();
        const unsigned int pbs = config_.pbs;
        uint64_t lsid = config_.lsid;
        Block hBlock = cybozu::util::allocateBlocks<u8>(pbs, pbs);
        cybozu::util::BlockAllocator<u8> ba(config_.maxPackPb, pbs, pbs);

        /* Generate and write walb log header. */
        wlHead.init(pbs, salt, &uuid[0], lsid, uint64_t(-1));
        if (!wlHead.isValid(false)) {
            throw RT_ERR("WalbLogHeader invalid.");
        }
        wlHead.write(fd);
        if (config_.isVerbose) {
            wlHead.print(::stderr);
        }

        uint64_t nPack = 0;
        while (writtenPb < config_.outLogPb) {
            walb::log::PackHeader logh(hBlock, pbs, salt);
            generateLogpackHeader(rand, logh, lsid);
            assert(::is_valid_logpack_header_and_records(&logh.header()));
            uint64_t tmpLsid = lsid + 1;

            /* Prepare blocks and calc checksum if necessary. */
            std::vector<Block> blocks;
            for (unsigned int i = 0; i < logh.nRecords(); i++) {
                walb::log::PackDataRef logd(logh, i);
                if (logd.hasData()) {
                    bool isAllZero = false;
                    if (config_.isAllZero) {
                        isAllZero = rand.get32() % 100 < 10;
                    }
                    for (unsigned int j = 0; j < logd.ioSizePb(); j++) {
                        Block b = ba.alloc();
                        ::memset(b.get(), 0, pbs);
                        if (!isAllZero) {
                            *reinterpret_cast<uint64_t *>(b.get()) = tmpLsid;
                        }
                        tmpLsid++;
                        logd.addBlock(b);
                        blocks.push_back(b);
                    }
                }
                if (logd.hasDataForChecksum()) {
                    UNUSED bool ret = logd.setChecksum();
                    assert(ret);
                    assert(logd.isValid(true));
                }
            }
            assert(blocks.size() == logh.totalIoSize());

            /* Calculate header checksum and write. */
            cybozu::util::FdWriter fdw(fd);
            logh.write(fdw);

            /* Write each IO data. */
            for (Block b : blocks) {
#if 0
                ::printf("block data %" PRIu64 "\n",
                         *reinterpret_cast<uint64_t *>(b.get())); /* debug */
#endif
                fdw.write(reinterpret_cast<const char *>(b.get()), pbs);
            }

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

        /* Write termination block. */
        walb::log::PackHeader logh(hBlock, pbs, salt);
        logh.setEnd();
        logh.write(fd);

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
