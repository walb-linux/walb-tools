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
            const log::Generator::Config &config_;
            Worker0(int outFd, const log::Generator::Config &config)
                : outFd_(outFd), config_(config) {}
            void operator()() {
                std::exception_ptr ep;
                try {
                    ::printf("start worker0.\n"); /* debug */
                    log::Generator g(config_);
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
                    Converter c;
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
            MemoryData &mem_;
            Worker2(int inFd, MemoryData &mem)
                : inFd_(inFd), mem_(mem) {}
            void operator()() {
                std::exception_ptr ep;
                try {
                    mem_.clear();
                    mem_.readFrom(inFd_);
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
        thSet.add(std::make_shared<Worker2>(pipe1.fdR(), mem_));
        thSet.start();
        thSet.join();
    }
};

}} //namespace walb::diff
