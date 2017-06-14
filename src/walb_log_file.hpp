#pragma once
/**
 * @file
 * @brief Walb log file utilities.
 */
#include <queue>
#include "walb_log_base.hpp"
#include "uuid.hpp"

namespace walb {

class WlogFileHeader
{
private:
    AlignedArray data_;

    static_assert(WALBLOG_HEADER_SIZE >= sizeof(struct walblog_header), "WALBLOG_HEADER_SIZE too small");

public:
    WlogFileHeader()
        : data_(WALBLOG_HEADER_SIZE, true) {}

    void init(uint32_t pbs, uint32_t salt, const cybozu::Uuid &uuid, uint64_t beginLsid, uint64_t endLsid);

    template <typename Reader>
    void readFrom(Reader& reader) {
        reader.read(data_.data(), data_.size());
        verify();
    }
    template <typename Writer>
    void writeTo(Writer& writer) {
        updateChecksum();
        writer.write(data_.data(), data_.size());
    }

    void updateChecksum() {
        header().checksum = 0; // data_ contains the checksum field.
        header().checksum = calcChecksum();
    }
    uint32_t calcChecksum() const {
        return ::checksum(data_.data(), data_.size(), 0);
    }

    struct walblog_header& header() {
        return *(struct walblog_header *)data_.data();
    }

    const struct walblog_header& header() const {
        return *(struct walblog_header *)data_.data();
    }

    uint32_t checksum() const { return header().checksum; }
    uint32_t salt() const { return header().log_checksum_salt; }
    uint32_t lbs() const { return header().logical_bs; }
    uint32_t pbs() const { return header().physical_bs; }
    uint64_t beginLsid() const { return header().begin_lsid; }
    uint64_t endLsid() const { return header().end_lsid; }
    cybozu::Uuid getUuid() const { return cybozu::Uuid(&header().uuid[0]); }
    uint16_t sectorType() const { return header().sector_type; }
    uint16_t headerSize() const { return header().header_size; }
    uint16_t version() const { return header().version; }

    bool isValid(bool isChecksum = true) const;
    void verify() const {
        if (!isValid(true)) {
            throw cybozu::Exception("WlogFileHeader") << "invalid";
        }
    }

    std::string str() const;
    std::string strDetail() const;
    friend inline std::ostream& operator<<(std::ostream& os, WlogFileHeader& wh) {
        os << wh.str();
        return os;
    }
};

/**
 * Walb log reader.
 */
class WlogReader /* final */
{
private:
    cybozu::util::File fileR_;
    bool isReadHeader_;
    uint32_t pbs_;
    uint32_t salt_;
    bool isBegun_;
    bool isEnd_;

    LogPackHeader packH_;
    uint16_t recIdx_;
    uint64_t endLsid_;

public:
    explicit WlogReader(cybozu::util::File &&fileR)
        : fileR_(std::move(fileR))
        , isReadHeader_(false)
        , pbs_(0)
        , salt_(0)
        , isBegun_(false)
        , isEnd_(false)
        , packH_()
        , recIdx_(0)
        , endLsid_(0) {}
    explicit WlogReader(int fd)
        : WlogReader(cybozu::util::File(fd)) {}

    /**
     * Read log file header.
     * This will throw EofError.
     */
    void readHeader(WlogFileHeader &fileH);

    /**
     * ReadLog.
     * This does not throw EofError.
     * RETURN:
     *   false when the input reached the end or end pack header was found.
     */
    bool readLog(WlogRecord& rec, AlignedArray& data, uint16_t *recIdxP = nullptr);

    /**
     * Get pack header reference.
     */
    LogPackHeader &packHeader() {
#ifndef NDEBUG
        bool ret = fetchNextPackHeader();
        assert(ret);
#endif
        return packH_;
    }
    /**
     * Get the end lsid.
     */
    uint64_t endLsid() {
        if (fetchNextPackHeader()) throw RT_ERR("Must be reached to the wlog end.");
        return endLsid_;
    }
private:
    /**
     * RETURN:
     *   true if there are one or more log data remaining.
     */
    bool fetchNextPackHeader();
};

/**
 * Walb log writer.
 */
class WlogWriter
{
private:
    cybozu::util::File fileW_;
    bool isWrittenHeader_;
    bool isClosed_;

    /* These members will be set in writeHeader(). */
    uint32_t pbs_;
    uint32_t salt_;
    uint64_t beginLsid_;
    uint64_t lsid_;
public:
    explicit WlogWriter(cybozu::util::File &&fileW)
        : fileW_(std::move(fileW))
        , isWrittenHeader_(false), isClosed_(false)
        , pbs_(0), salt_(0), beginLsid_(-1), lsid_(-1)  {}
    explicit WlogWriter(int fd)
        : WlogWriter(cybozu::util::File(fd)) {}
    ~WlogWriter() noexcept {
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
    void close();

    /**
     * Write a file header.
     *
     * You must call this just once.
     * You need not call fileH.updateChecksum() by yourself before.
     */
    void writeHeader(WlogFileHeader &fileH);

    /**
     * Write a pack header block.
     * You must call writePackIo() n times after this.
     * n is header.n_records.
     */
    void writePackHeader(const struct walb_logpack_header &header) {
        if (!isWrittenHeader_) throw RT_ERR("You must call writeHeader() at first.");
        if (header.n_records == 0) return;
        fileW_.write(&header, pbs_);
        lsid_++;
    }
    void writePackHeader(const LogPackHeader &packH) {
        writePackHeader(packH.header());
    }
    void writePackIo(const AlignedArray &data);
    void writePack(const LogPackHeader &header, std::queue<AlignedArray> &&ioQ);

    /**
     * Get the end lsid that is next lsid of the latest written logpack.
     * Header's begenLsid will be returned if writePack() was not called at all.
     */
    uint64_t endLsid() const { return lsid_; }
    /**
     * Parameters.
     */
    uint32_t pbs() const { return pbs_; }
    uint32_t salt() const { return salt_; }
private:
    /**
     * Check a pack header block.
     */
    void checkHeader(const LogPackHeader &header) const;

    /**
     * Write the end block.
     */
    void writeEof() {
        LogPackHeader h(pbs_, salt_);
        h.setEnd();
        h.updateChecksumAndWriteTo(fileW_);
    }
};

/**
 * Log file reader which has skip().
 */
class LogFile : public cybozu::util::File
{
private:
    bool seekable_;
public:
    using File :: File;

    void setSeekable(bool seekable) {
        seekable_ = seekable;
    }
    void skip(size_t size);
};

} // namespace walb
