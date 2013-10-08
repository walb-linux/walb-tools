#pragma once
/**
 * @file
 * @brief Walb log file utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
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
    int fd_;
    cybozu::util::FdReader fdr_;
    bool isReadHeader_;
    unsigned int pbs_;
    uint32_t salt_;
    bool isEnd_;

    std::shared_ptr<PackHeaderRaw> pack_;
    uint16_t recIdx_;
    uint16_t totalSize_;

public:
    explicit Reader(int fd)
        : fd_(fd), fdr_(fd)
        , isReadHeader_(false)
        , pbs_(0)
        , salt_(0)
        , isEnd_(false)
        , pack_()
        , recIdx_(0)
        , totalSize_(0) {}

    ~Reader() noexcept {}
    /**
     * Read log file header.
     */
    void readHeader(FileHeader &fileH) {
        if (isReadHeader_) throw RT_ERR("Log header has been called already.");
        isReadHeader_ = true;
        fileH.read(fdr_);
        if (!fileH.isValid(true)) throw RT_ERR("invalid walb log header.");
        pbs_ = fileH.pbs();
        salt_ = fileH.salt();
    }
    /**
     * ReadLog
     */
    bool readLog(PackIoRef<RecordRaw> &packIo) {
        if (!isReadHeader_) throw RT_ERR("readHeader");
        fillPackIfNeed();
        if (!pack_) return false;

        /* Copy to the record. */
        RecordRaw &rec = packIo.record();
        RecordRef srcRec(*pack_, recIdx_);
        rec = srcRec;

        /* Read to the blockD. */
        BlockData &blockD = packIo.blockData();
        blockD.setPbs(pbs_);
        blockD.clear();
        if (rec.hasData()) {
            for (size_t i = 0; i < rec.ioSizePb(); i++) {
                try {
                    std::shared_ptr<uint8_t> b = allocPb();
                    fdr_.read(reinterpret_cast<char *>(b.get()), pbs_);
                    blockD.addBlock(b);
                } catch (cybozu::util::EofError &e) {
                    throw InvalidLogpackData();
                }
            }
        }

        /* Validate. */
        if (!packIo.isValid()) {
            packIo.print();
            LOGd("invalid...");
            throw InvalidLogpackData();
        }

        recIdx_++;
        return true;
    }
    /**
     * Read a log.
     * RETURN:
     *   true when successfully read.
     *   false when the stream has reached the end.
     */
    bool readLog(RecordRaw &rec, BlockData &blockD) {
        PackIoRef<RecordRaw> packIo(&rec, &blockD);
        return readLog(packIo);
    }
private:
    void fillPackIfNeed() {
        assert(isReadHeader_);
        if (isEnd_ || (pack_ && recIdx_ < pack_->nRecords())) { return; }

        std::shared_ptr<uint8_t> b = allocPb();
        try {
            fdr_.read(reinterpret_cast<char *>(b.get()), pbs_);
            pack_.reset(new PackHeaderRaw(b, pbs_, salt_));
            if (!pack_->isValid()) {
                throw RT_ERR("Invalid logpack header.");
            }
            if (pack_->isEnd()) {
                pack_.reset();
                isEnd_ = true;
                return;
            }
            recIdx_ = 0;
            // pack_->print();
        } catch (cybozu::util::EofError &e) {
            pack_.reset();
            isEnd_ = true;
        }
    }
    std::shared_ptr<uint8_t> allocPb() {
        assert(isReadHeader_);
        return cybozu::util::allocateBlocks<uint8_t>(LOGICAL_BLOCK_SIZE, pbs_);
    }
};

/**
 * Pretty printer of walb log.
 */
class Printer
{
private:
    int inFd_;
public:
    explicit Printer(int inFd) : inFd_(inFd) {}
    void operator()(::FILE *fp = ::stdout) {
        if (inFd_ < 0) throw RT_ERR("inFd_ invalid.");
        Reader reader(inFd_);

        FileHeader wh;
        reader.readHeader(wh);
        wh.print(fp);

        PackIoRaw packIo;
        while (reader.readLog(packIo)) {
            packIo.printOneline(fp);
        }
        /*
         * reader.readLog() may throw InvalidLogpackData.
         * We need not shrink the invalid packs. Wlog file must be valid.
         */
    }
};

}} //namespace walb::log
