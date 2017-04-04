#pragma once
/**
 * @file
 * @brief Walb log network utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <vector>
#include <cstring>

#include "walb_log_base.hpp"
#include "walb_log_file.hpp"
#include "compressed_data.hpp"
#include "walb_logger.hpp"

namespace walb {

constexpr size_t Q_SIZE = 16;

/**
 * Walb log sender via TCP/IP connection.
 * This will send packets only, never receive packets.
 *
 * Usage:
 *   (1) call setParams() to set parameters.
 *   (2) call start() to start worker threads.
 *   (3) call pushHeader() and corresponding pushIo() multiple times.
 *   (4) repeat (3).
 *   (5) call sync() for normal finish, or fail().
 */
class WlogSender
{
private:
    packet::Packet packet_;
    packet::StreamControl ctrl_;
    Logger &logger_;
    uint32_t pbs_;
    uint32_t salt_;
public:
    static constexpr const char *NAME() { return "WlogSender"; }
    WlogSender(cybozu::Socket &sock, Logger &logger, uint32_t pbs, uint32_t salt)
        : packet_(sock), ctrl_(sock), logger_(logger), pbs_(pbs), salt_(salt) {
    }
    void process(CompressedData& cd, bool doCompress);

    /**
     * You must call pushHeader(h) and n times of pushIo(),
     * where n is h.nRecords().
     */
    void pushHeader(const LogPackHeader &header) {
        verifyPbsAndSalt(header);
        CompressedData cd;
        cd.setUncompressed(header.rawData(), pbs_);
        process(cd, true);
    }
    /**
     * You must call this for discard/padding record also.
     */
    void pushIo(const LogPackHeader &header, uint16_t recIdx, const char *data);

    /**
     * Notify the end of input.
     */
    void sync() {
        ctrl_.end();
    }
private:
    void verifyPbsAndSalt(const LogPackHeader &header) const;
};

/**
 * Walb log receiver via TCP/IP connection.
 *
 * Usage:
 *   (1) call setParams() to set parameters.
 *   (2) call start() to start worker threads.
 *   (3) call popHeader() and corresponding popIo() multiple times.
 *   (4) repeat (3) while popHeader() returns true.
 *   popHeader() will throw an error if something is wrong.
 */
class WlogReceiver
{
private:
    packet::Packet packet_;
    packet::StreamControl ctrl_;
    uint32_t pbs_;
    uint32_t salt_;
public:
    static constexpr const char *NAME() { return "WlogReceiver"; }
    WlogReceiver(cybozu::Socket &sock, uint32_t pbs, uint32_t salt)
        : packet_(sock), ctrl_(sock), pbs_(pbs), salt_(salt) {
    }
    bool process(CompressedData& cd);

    /**
     * You must call popHeader(h) and its corresponding popIo() n times,
     * where n is h.n_records.
     *
     * RETURN:
     *   false if the input stream has reached the end.
     */
    bool popHeader(LogPackHeader &header);

    /**
     * Get IO data.
     * You must call this for discard/padding record also.
     */
    void popIo(const WlogRecord &rec, AlignedArray &data);
};

} //namespace walb
