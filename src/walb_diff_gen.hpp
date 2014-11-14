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
#include "thread_util.hpp"
#include "walb_log_gen.hpp"
#include "walb_diff_mem.hpp"
#include "walb_diff_converter.hpp"

namespace walb {

/**
 * Walb diff generator for test.
 */
class DiffGenerator
{
private:
    const WlogGenerator::Config &config_;
    DiffMemory diffMem_;

public:
    explicit DiffGenerator(const WlogGenerator::Config &config)
        : config_(config), diffMem_() {}
    ~DiffGenerator() noexcept = default;
    DiffMemory &data() { return diffMem_; }
    const DiffMemory &data() const { return diffMem_; }
    /**
     * TODO:
     *   * Do not convert via file descriptors with threads.
     *     Convert wlog to wdiff directly.
     */
    void generate() {
        cybozu::process::Pipe pipe0, pipe1;
        /**
         * Generate wlog.
         */
        struct Worker0
        {
            int outFd_;
            const WlogGenerator::Config &config_;
            Worker0(int outFd, const WlogGenerator::Config &config)
                : outFd_(outFd), config_(config) {}
            void operator()() {
                std::exception_ptr ep;
                try {
                    ::printf("start worker0.\n"); /* debug */
                    WlogGenerator g(config_);
                    g.generate(outFd_);
                } catch (...) {
                    ep = std::current_exception();
                }
                /* finally */
                ::close(outFd_);
                if (ep) std::rethrow_exception(ep);
            }
        };
        /**
         * Convert wlog to wdiff.
         */
        struct Worker1
        {
            int inFd_;
            int outFd_;
            Worker1(int inFd, int outFd)
                : inFd_(inFd), outFd_(outFd) {}
            void operator()() {
                std::exception_ptr ep;
                try {
                    ::printf("start worker1.\n"); /* debug */
                    DiffConverter c;
                    c.convert(inFd_, outFd_);
                } catch (...) {
                    ep = std::current_exception();
                }
                /* finally */
                ::close(inFd_);
                ::close(outFd_);
                if (ep) std::rethrow_exception(ep);
            }
        };
        /**
         * Read wdiff stream and make a memory data.
         */
        struct Worker2
        {
            int inFd_;
            DiffMemory &diffMem_;
            Worker2(int inFd, DiffMemory &mem)
                : inFd_(inFd), diffMem_(mem) {}
            void operator()() {
                std::exception_ptr ep;
                try {
                    diffMem_.clear();
                    diffMem_.readFrom(inFd_);
                } catch (...) {
                    ep = std::current_exception();
                }
                ::close(inFd_);
                if (ep) std::rethrow_exception(ep);
            }
        };
        cybozu::thread::ThreadRunnerSet thSet;
        thSet.add(std::make_shared<Worker0>(pipe0.fdW(), config_));
        thSet.add(std::make_shared<Worker1>(pipe0.fdR(), pipe1.fdW()));
        thSet.add(std::make_shared<Worker2>(pipe1.fdR(), diffMem_));
        thSet.start();
        thSet.join();
    }
};

} //namespace walb
