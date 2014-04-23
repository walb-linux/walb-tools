#pragma once
/**
 * @file
 * @brief Walb log file utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <queue>
#include "walb_log_base.hpp"

namespace walb {
namespace log {

/**
 * Walb logfile header.
 */
class FileHeader
{
private:
    std::vector<uint8_t> data_;

public:
    FileHeader()
        : data_(WALBLOG_HEADER_SIZE, 0) {}

    void init(unsigned int pbs, uint32_t salt, const uint8_t *uuid, uint64_t beginLsid, uint64_t endLsid) {
        ::memset(&data_[0], 0, WALBLOG_HEADER_SIZE);
        header().sector_type = SECTOR_TYPE_WALBLOG_HEADER;
        header().version = WALB_LOG_VERSION;
        header().header_size = WALBLOG_HEADER_SIZE;
        header().log_checksum_salt = salt;
        header().logical_bs = LOGICAL_BLOCK_SIZE;
        header().physical_bs = pbs;
        ::memcpy(header().uuid, uuid, UUID_SIZE);
        header().begin_lsid = beginLsid;
        header().end_lsid = endLsid;
    }

    void read(int fd) {
        cybozu::util::FdReader fdr(fd);
        read(fdr);
    }

    void read(cybozu::util::FdReader& fdr) {
        fdr.read(ptr<char>(), WALBLOG_HEADER_SIZE);
        verify();
    }

    void write(int fd) {
        cybozu::util::FdWriter fdw(fd);
        write(fdw);
    }

    void write(cybozu::util::FdWriter& fdw) {
        updateChecksum();
        fdw.write(ptr<char>(), WALBLOG_HEADER_SIZE);
    }

    void updateChecksum() {
        header().checksum = 0;
        header().checksum = ::checksum(&data_[0], WALBLOG_HEADER_SIZE, 0);
    }

    struct walblog_header& header() {
        return *ptr<struct walblog_header>();
    }

    const struct walblog_header& header() const {
        return *ptr<struct walblog_header>();
    }

    uint32_t checksum() const { return header().checksum; }
    uint32_t salt() const { return header().log_checksum_salt; }
    unsigned int lbs() const { return header().logical_bs; }
    unsigned int pbs() const { return header().physical_bs; }
    uint64_t beginLsid() const { return header().begin_lsid; }
    uint64_t endLsid() const { return header().end_lsid; }
    const uint8_t* uuid() const { return &header().uuid[0]; }
    uint16_t sectorType() const { return header().sector_type; }
    uint16_t headerSize() const { return header().header_size; }
    uint16_t version() const { return header().version; }

    bool isValid(bool isChecksum = true) const {
        CHECKd(header().sector_type == SECTOR_TYPE_WALBLOG_HEADER);
        CHECKd(header().version == WALB_LOG_VERSION);
        CHECKd(header().begin_lsid < header().end_lsid);
        if (isChecksum) {
            CHECKd(::checksum(&data_[0], WALBLOG_HEADER_SIZE, 0) == 0);
        }
        return true;
      error:
        return false;
    }
    void verify() const {
        if (!isValid(true)) {
            throw cybozu::Exception("log::FileHeader") << "invalid";
        }
    }

    void print(FILE *fp = ::stdout) const {
        ::fprintf(
            fp,
            "sector_type %d\n"
            "version %u\n"
            "header_size %u\n"
            "log_checksum_salt %" PRIu32 " (%08x)\n"
            "logical_bs %u\n"
            "physical_bs %u\n"
            "uuid ",
            header().sector_type,
            header().version,
            header().header_size,
            header().log_checksum_salt,
            header().log_checksum_salt,
            header().logical_bs,
            header().physical_bs);
        for (size_t i = 0; i < UUID_SIZE; i++) {
            ::fprintf(fp, "%02x", header().uuid[i]);
        }
        ::fprintf(
            fp,
            "\n"
            "begin_lsid %" PRIu64 "\n"
            "end_lsid %" PRIu64 "\n",
            header().begin_lsid,
            header().end_lsid);
    }

private:
    template <typename T>
    T *ptr() { return reinterpret_cast<T *>(&data_[0]); }

    template <typename T>
    const T *ptr() const { return reinterpret_cast<const T *>(&data_[0]); }
};

/**
 * Walb log reader.
 */
class Reader /* final */
{
private:
    cybozu::util::FdReader fdr_;
    bool isReadHeader_;
    unsigned int pbs_;
    uint32_t salt_;
    bool isEnd_;

    LogPackHeader pack_;
    uint16_t recIdx_;
    uint64_t endLsid_;

public:
    explicit Reader(int fd)
        : fdr_(fd)
        , isReadHeader_(false)
        , pbs_(0)
        , salt_(0)
        , isEnd_(false)
        , pack_()
        , recIdx_(0)
        , endLsid_(0) {}

    /**
     * Read log file header.
     * This will throw EofError.
     */
    void readHeader(FileHeader &fileH) {
        if (isReadHeader_) throw RT_ERR("Wlog file header has been read already.");
        isReadHeader_ = true;
        fileH.read(fdr_);
        pbs_ = fileH.pbs();
        salt_ = fileH.salt();
        endLsid_ = fileH.beginLsid();
        pack_.init(pbs_, salt_);
    }
    /**
     * ReadLog.
     * This does not throw EofError.
     * RETURN:
     *   false when the input reached the end or end pack header was found.
     */
    bool readLog(LogRecord& rec, LogBlockShared& blockS, uint16_t *recIdxP = nullptr)
    {
        if (!fetchNext()) return false;

        /* Copy to the record. */
        rec = pack_.record(recIdx_);
        rec.verify();

        /* Read to the blockS. */
        blockS.init(pbs_);
        if (rec.hasData()) {
            try {
                blockS.read(fdr_, rec.ioSizePb(pbs_));
            } catch (cybozu::util::EofError &) {
                throw InvalidIo(); // QQQ
            }
        } else {
            blockS.resize(0);
        }

        /* Validate. */
        if (rec.hasDataForChecksum()) {
            const uint32_t csum = blockS.calcChecksum(rec.ioSizeLb(), salt_);
            if (rec.checksum != csum) {
                LOGs.debug() << "invalid checksum" << rec.checksum << csum;
                throw InvalidIo();
            }
        }
        if (recIdxP) *recIdxP = recIdx_;
        recIdx_++;
        return true;
    }
    /**
     * RETURN:
     *   true if the cursor indicates the first record in a pack.
     */
    bool isFirstInPack() { // QQQ
        return fetchNext() && recIdx_ == 0;
    }
    /**
     * Get pack header reference.
     */
    LogPackHeader &packHeader() {
        assert(fetchNext());
        return pack_;
    }
    /**
     * RETURN:
     *   true if there are one or more log data remaining.
     */
    bool fetchNext() {
        if (!isReadHeader_) {
            throw cybozu::Exception(__func__)
                << "You have not called readHeader() yet";
        }
        if (isEnd_) return false;
        if (recIdx_ < pack_.nRecords()) return true;

        if (!pack_.read(fdr_) || pack_.isEnd()) {
            isEnd_ = true;
            return false;
        }
        recIdx_ = 0;
        endLsid_ = pack_.nextLogpackLsid();
        return true;
    }
    /**
     * Get the end lsid.
     */
    uint64_t endLsid() {
        if (fetchNext()) throw RT_ERR("Must be reached to the wlog end.");
        return endLsid_;
    }
};

/**
 * Walb log writer.
 */
class Writer
{
private:
    cybozu::util::FdWriter fdw_;
    bool isWrittenHeader_;
    bool isClosed_;

    /* These members will be set in writeHeader(). */
    unsigned int pbs_;
    uint32_t salt_;
    uint64_t beginLsid_;
    uint64_t lsid_;
public:
    explicit Writer(int fd)
        : fdw_(fd), isWrittenHeader_(false), isClosed_(false)
        , pbs_(0), salt_(0), beginLsid_(-1), lsid_(-1) {}
    ~Writer() noexcept {
        try {
            close();
        } catch (...) {}
    }
    /**
     * Finalize the output stream.
     *
     * fdatasync() will not be called.
     * You can call this multiple times.
     */
    void close() {
        if (!isClosed_) {
            writeEof();
            isClosed_ = true;
        }
    }
    /**
     * Write a file header.
     *
     * You must call this just once.
     * You need not call fileH.updateChecksum() by yourself before.
     */
    void writeHeader(FileHeader &fileH) {
        if (isWrittenHeader_) throw RT_ERR("Wlog file header has been written already.");
        if (!fileH.isValid(false)) throw RT_ERR("Wlog file header is invalid.");
        fileH.write(fdw_);
        isWrittenHeader_ = true;
        pbs_ = fileH.pbs();
        salt_ = fileH.salt();
        beginLsid_ = fileH.beginLsid();
        lsid_ = beginLsid_;
    }
    /**
     * Write a pack header block.
     * You must call writePackIo() n times after this.
     * n is header.n_records.
     */
    void writePackHeader(const struct walb_logpack_header &header) {
        if (!isWrittenHeader_) throw RT_ERR("You must call writeHeader() at first.");
        if (header.n_records == 0) return;
        fdw_.write(&header, pbs_);
        lsid_++;
    }
    /**
     * Write a pack IO.
     */
    void writePackIo(const LogBlockShared &blockS) {
        blockS.write(fdw_);
        lsid_ += blockS.nBlocks();
    }
    /**
     * Write a pack.
     */
    void writePack(const LogPackHeader &header, std::queue<LogBlock> &&blockQ) {
        /* Validate. */
        checkHeader(header);
        if (header.totalIoSize() != blockQ.size()) {
            throw cybozu::Exception(__func__)
                << "blocks.size() differ"
                << header.totalIoSize() << blockQ.size();
        }
        std::vector<LogBlockShared> v;
        for (size_t i = 0; i < header.nRecords(); i++) {
            const LogRecord &rec = header.record(i);
            LogBlockShared blockS(pbs_);
            if (rec.hasData()) {
                const size_t ioSizePb = rec.ioSizePb(pbs_);
                for (size_t j = 0; j < ioSizePb; j++) {
                    blockS.addBlock(std::move(blockQ.front()));
                    blockQ.pop();
                }
            }
            verifyLogChecksum(rec, blockS, header.salt());
            v.push_back(std::move(blockS));
        }

        /* Write */
        writePackHeader(header.header());
        for (const LogBlockShared &blockS : v) {
            writePackIo(blockS);
        }
        assert(lsid_ == header.nextLogpackLsid());
    }
    /**
     * Get the end lsid that is next lsid of the latest written logpack.
     * Header's begenLsid will be returned if writePack() was not called at all.
     */
    uint64_t endLsid() const { return lsid_; }
    /**
     * Parameters.
     */
    unsigned int pbs() const { return pbs_; }
    uint32_t salt() const { return salt_; }
private:
    /**
     * Check a pack header block.
     */
    void checkHeader(const LogPackHeader &header) const {
        if (!header.isValid()) {
            throw RT_ERR("Logpack header invalid.");
        }
        if (pbs_ != header.pbs()) {
            throw RT_ERR("pbs differs %u %u.", pbs_, header.pbs());
        }
        if (salt_ != header.salt()) {
            throw RT_ERR("salt differs %" PRIu32 " %" PRIu32 "."
                         , salt_, header.salt());
        }
        if (lsid_ != header.logpackLsid()) {
            throw RT_ERR("logpack lsid differs %" PRIu64 " %" PRIu64 "."
                         , lsid_, header.logpackLsid());
        }
    }
    /**
     * Write the end block.
     */
    void writeEof() {
        LogBlock b = createLogBlock(pbs_);
        LogPackHeader h(b, pbs_, salt_);
        h.setEnd();
        h.write(fdw_);
    }
};

/**
 * Pretty printer of walb log.
 */
class Printer
{
public:
    void operator()(int inFd, ::FILE *fp = ::stdout) {
        if (inFd < 0) throw RT_ERR("inFd_ invalid.");
        Reader reader(inFd);

        FileHeader wh;
        reader.readHeader(wh);
        wh.print(fp);

        while (reader.fetchNext()) {
            LogRecord rec;
            LogBlockShared blockS;
            uint16_t idx;
            const uint32_t recIdx = reader.readLog(rec, blockS, &idx);
            log::printRecordOneline(::stdout, recIdx, rec);
        }
        /*
         * reader.readLog() may throw InvalidIo.
         * We need not shrink the invalid packs. Wlog file must be valid.
         */
    }
};

}} //namespace walb::log
