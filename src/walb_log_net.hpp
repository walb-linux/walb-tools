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

CompressedData convertToCompressedData(const LogBlockShared &blockS, bool doCompress)
{
    const uint32_t pbs = blockS.pbs();
    const size_t n = blockS.nBlocks();
    assert(0 < n);
    std::vector<char> d(n * pbs);
    for (size_t i = 0; i < n; i++) {
        ::memcpy(&d[i * pbs], blockS.get(i), pbs);
    }
    CompressedData cd;
    cd.setUncompressed(std::move(d));
    if (doCompress) cd.compress();
    return cd;
}

inline void convertToLogBlockShared(LogBlockShared& blockS, const CompressedData &cd, uint32_t sizePb, uint32_t pbs)
{
    const char *const FUNC = __func__;
    std::vector<char> v;
    cd.getUncompressed(v);
    if (sizePb * pbs != v.size()) throw cybozu::Exception(FUNC) << "invalid size" << v.size() << sizePb;
    blockS.init(pbs);
    blockS.resize(sizePb);
    for (size_t i = 0; i < sizePb; i++) {
        ::memcpy(blockS.get(i), &v[i * pbs], pbs);
    }
}

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
    void process(CompressedData& cd) try {
        cd.compress();
        ctrl_.next();
        cd.send(packet_);
    } catch (std::exception& e) {
        try {
            packet::StreamControl(packet_.sock()).error();
        } catch (...) {}
        logger_.error() << "WlogSender:process" << e.what();
    }
    /**
     * You must call pushHeader(h) and n times of pushIo(),
     * where n is h.nRecords().
     */
    void pushHeader(const LogPackHeader &header) {
        verifyPbsAndSalt(header);
        CompressedData cd;
        cd.setUncompressed(header.rawData(), pbs_);
        process(cd);
    }
    /**
     * You must call this for discard/padding record also.
     */
    void pushIo(const LogPackHeader &header, uint16_t recIdx, const LogBlockShared &blockS) {
        verifyPbsAndSalt(header);
        const WlogRecord &rec = header.record(recIdx);
        if (rec.hasDataForChecksum()) {
            CompressedData cd = convertToCompressedData(blockS, false);
            assert(0 < cd.originalSize());
            process(cd);
        }
    }
    /**
     * Notify the end of input.
     */
    void sync() {
        ctrl_.end();
    }
private:
    void verifyPbsAndSalt(const LogPackHeader &header) const {
        if (header.pbs() != pbs_) {
            throw cybozu::Exception(NAME()) << "invalid pbs" << pbs_ << header.pbs();
        }
        if (header.salt() != salt_) {
            throw cybozu::Exception(NAME()) << "invalid salt" << salt_ << header.salt();
        }
    }
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
    bool process(CompressedData& cd) {
        if (ctrl_.isNext()) {
            cd.recv(packet_);
            cd.uncompress();
            ctrl_.reset();
            return true;
        }
        if (ctrl_.isError()) {
            throw cybozu::Exception("WlogReceiver:process:isError");
        }
        return false;
    }
    /**
     * You must call popHeader(h) and its corresponding popIo() n times,
     * where n is h.n_records.
     *
     * RETURN:
     *   false if the input stream has reached the end.
     */
    bool popHeader(LogPackHeader &header) {
        const char *const FUNC = __func__;
        CompressedData cd;
        if (!process(cd)) {
            return false;
        }
        assert(!cd.isCompressed());
        if (cd.rawSize() != pbs_) {
            throw cybozu::Exception(FUNC) << "invalid pack header size" << cd.rawSize() << pbs_;
        }
        header.copyFrom(cd.rawData(), pbs_);
        if (header.isEnd()) throw cybozu::Exception(FUNC) << "end header is not permitted";
        return true;
    }
    /**
     * Get IO data.
     * You must call this for discard/padding record also.
     */
    void popIo(const WlogRecord &rec, LogBlockShared &blockS) {
        if (rec.hasDataForChecksum()) {
            CompressedData cd;
            if (!process(cd)) throw cybozu::Exception("WlogReceiver:popIo:failed.");
            convertToLogBlockShared(blockS, cd, rec.ioSizePb(pbs_), pbs_);
            verifyWlogChecksum(rec, blockS, salt_);
        } else {
            blockS.init(pbs_);
        }
    }
private:
};

} //namespace walb
