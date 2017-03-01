#include "walb_diff_file.hpp"

namespace walb {

bool DiffFileHeader::verify(bool throwError) const
{
    if (cybozu::util::calcChecksum(this, getSize(), 0) != 0) {
        if (throwError) {
            throw cybozu::Exception(__func__) << "invalid checksum";
        }
        return false;
    }
    if (version != WALB_DIFF_VERSION) {
        if (throwError) {
            throw cybozu::Exception(__func__)
                << "invalid walb diff version" << version << WALB_DIFF_VERSION;
        }
        return false;
    }
    return true;
}

std::string DiffFileHeader::str() const
{
    return cybozu::util::formatString(
        "wdiff_h:\n"
        "  checksum: %08x\n"
        "  version: %u\n"
        "  maxIoBlocks: %u\n"
        "  uuid: %s\n"
        , checksum, version, max_io_blocks, getUuid().str().c_str());
}

void DiffWriter::close()
{
    if (!isClosed_) {
        writePack(); // if buffered data exist.
        writeEof();
        fileW_.close();
        isClosed_ = true;
    }
}

void DiffWriter::writeHeader(DiffFileHeader &header)
{
    if (isWrittenHeader_) {
        throw RT_ERR("Do not call writeHeader() more than once.");
    }
    header.writeTo(fileW_);
    isWrittenHeader_ = true;
}

void DiffWriter::writeDiff(const DiffRecord &rec, DiffIo &&io)
{
    checkWrittenHeader();
    assertRecAndIo(rec, io);

    if (addAndPush(rec, std::move(io))) return;
    writePack();
    const bool ret = addAndPush(rec, std::move(io));
    unusedVar(ret);
    assert(ret);
}

void DiffWriter::writeDiff(const DiffRecord &rec, const char *data)
{
    DiffIo io;
    io.set(rec);
    if (rec.isNormal()) {
        ::memcpy(io.get(), data, rec.data_size);
    }
    writeDiff(rec, std::move(io));
}

void DiffWriter::compressAndWriteDiff(
    const DiffRecord &rec, const char *data,
    int type, int level)
{
    if (rec.isCompressed()) {
        writeDiff(rec, data);
        return;
    }
    if (!rec.isNormal()) {
        writeDiff(rec, DiffIo());
        return;
    }
    DiffRecord compRec;
    DiffIo compIo;
    compressDiffIo(rec, data, compRec, compIo.data, type, level);
    compIo.set(compRec);
    writeDiff(compRec, std::move(compIo));
}

void DiffWriter::init()
{
    isWrittenHeader_ = false;
    isClosed_ = false;
    pack_.clear();
    while (!ioQ_.empty()) ioQ_.pop();
    stat_.clear();
    stat_.wdiffNr = 1;
}

bool DiffWriter::addAndPush(const DiffRecord &rec, DiffIo &&io)
{
    if (pack_.add(rec)) {
        ioQ_.push(std::move(io));
        return true;
    }
    return false;
}

#ifdef DEBUG
void DiffWriter::assertRecAndIo(const DiffRecord &rec, const DiffIo &io)
{
    if (rec.isNormal()) {
        assert(!io.empty());
        assert(io.compressionType == rec.compression_type);
        assert(rec.checksum == io.calcChecksum());
    } else {
        assert(io.empty());
    }
}
#else
void DiffWriter::assertRecAndIo(const DiffRecord &, const DiffIo &) {}
#endif

void DiffWriter::writePack()
{
    if (pack_.n_records == 0) {
        assert(ioQ_.empty());
        return;
    }

    stat_.update(pack_);
    pack_.writeTo(fileW_);

    assert(pack_.n_records == ioQ_.size());
    size_t total = 0;
    while (!ioQ_.empty()) {
        DiffIo io0 = std::move(ioQ_.front());
        ioQ_.pop();
        if (io0.empty()) continue;
        io0.writeTo(fileW_);
        total += io0.getSize();
    }
    assert(total == pack_.total_size);
    pack_.clear();
}

} //namespace walb
