#pragma once
/**
 * @file
 * @brief Wdiff generator for test.
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
#include <future>
#include <thread>
#include <chrono>

#include "process.hpp"
#include "walb_log_gen.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_converter.hpp"

namespace walb {
namespace diff {

/**
 * Walb diff generator for test.
 */
class Generator
{
private:
    const log::Generator::Config &config_;
    MemoryData mem_;

public:
    explicit Generator(const log::Generator::Config &config)
        : config_(config), mem_() {}
    ~Generator() noexcept = default;
    MemoryData &data() { return mem_; }
    const MemoryData &data() const { return mem_; }
    void generate() {
        cybozu::process::Pipe pipe0, pipe1;
        /**
         * Generate wlog.
         */
        auto worker0 = [](int outFd, const log::Generator::Config &config) {
            struct Run {
                int outFd_;
                const log::Generator::Config &config_;
                Run(int outFd, const log::Generator::Config &config)
                    : outFd_(outFd), config_(config) {}
                void operator()() {
                    ::printf("start worker0.\n"); /* debug */
                    log::Generator g(config_);
                    g.generate(outFd_);
                }
                ~Run() noexcept {
                    ::printf("close pipe\n"); /* debug */
                    ::close(outFd_);
                }
            };
            Run r(outFd, config);
            r();
            ::printf("worker0 end\n"); /* debug */
            std::this_thread::sleep_for(std::chrono::seconds(1)); /* debug */
        };
        /**
         * Convert wlog to wdiff.
         */
        auto worker1 = [](int inFd, int outFd) {
            struct Run {
                int inFd_;
                int outFd_;
                Run(int inFd, int outFd)
                    : inFd_(inFd), outFd_(outFd) {}
                void operator()() {
                    ::printf("start worker1.\n"); /* debug */
                    Converter c;
                    c.convert(inFd_, outFd_);
                }
                ~Run() noexcept {
                    ::close(outFd_);
                }
            };
            Run r(inFd, outFd);
            r();
            ::printf("worker1 end\n"); /* debug */
        };
        auto worker2 = [](int inFd, MemoryData &mem) {
            mem.clear();
            mem.readFrom(inFd);
            ::printf("worker2 end\n"); /* debug */
        };

        /* now editing */

        auto f0 = std::async(std::launch::async, worker0, pipe0.fdW(), std::ref(config_));
        auto f1 = std::async(std::launch::async, worker1, pipe0.fdR(), pipe1.fdW());
        auto f2 = std::async(std::launch::async, worker2, pipe1.fdR(), std::ref(mem_));
        f0.get();
        f1.get();
        f2.get();
    }
private:

    /* now editing */
};

}} //namespace walb::diff
