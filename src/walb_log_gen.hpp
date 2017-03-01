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
        uint32_t minDiscardLb;
        uint32_t maxDiscardLb;
        uint32_t pbs;
        uint32_t maxPackPb;
        uint32_t outLogPb;
        uint64_t lsid;
        bool isPadding;
        bool isDiscard;
        bool isAllZero;
        bool isRandom;
        bool isVerbose;

        void check() const;
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

    void generateAndWrite(int fd);

    /**
     * Generate logpack header randomly.
     */
    void generateLogpackHeader(Rand &rand, LogPackHeader &packH, uint64_t lsid);
};

} //namespace walb
