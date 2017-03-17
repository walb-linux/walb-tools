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

void DiffIndexMem::checkNoOverlappedAndSorted() const
{
    auto it = index_.cbegin();
    const DiffIndexRecord *prev = nullptr;
    while (it != index_.cend()) {
        const DiffIndexRecord *curr = &it->second;
        if (prev) {
            if (!(prev->io_address < curr->io_address)) {
                throw RT_ERR("Not sorted.");
            }
            if (!(prev->endIoAddress() <= curr->io_address)) {
                throw RT_ERR("Overlapped records exists.");
            }
        }
        prev = curr;
        ++it;
    }
}

void DiffIndexMem::addDetail(const DiffIndexRecord &rec)
{
    assert(isAlignedIo(rec.io_address, rec.io_blocks));

    /* Decide the first item to search. */
    auto it = index_.lower_bound(rec.io_address);
    if (it == index_.end()) {
        if (index_.empty()) {
            index_.emplace(rec.io_address, rec);
            return;
        }
        --it; // the last item.
    } else {
        if (rec.io_address < it->first && it != index_.begin()) {
            --it; // previous item.
        }
    }

    /* Search overlapped items. */
    uint64_t addr1 = rec.endIoAddress();
    std::queue<DiffIndexRecord> q;
    while (it != index_.end() && it->first < addr1) {
        DiffIndexRecord &r = it->second;
        if (r.isOverlapped(rec)) {
            q.push(r);
            it = index_.erase(it);
        } else {
            ++it;
        }
    }

    /* Eliminate overlaps. */
    while (!q.empty()) {
        for (const DiffIndexRecord &r0 : q.front().minus(rec)) {
            for (const DiffIndexRecord &r1 : r0.split()) {
                index_.emplace(r1.io_address, r1);
            }
        }
        q.pop();
    }

    index_.emplace(rec.io_address, rec);
}

void IndexedDiffWriter::finalize()
{
    if (isClosed_) return;

    indexMem_.writeTo(fileW_);
    writeSuper();

    fileW_.close();
    isClosed_ = true;
}

void IndexedDiffWriter::writeHeader(DiffFileHeader &header)
{
    if (isWrittenHeader_) {
        throw cybozu::Exception(NAME)
            << "do not call writeHeader() more than once.";
    }
    header.writeTo(fileW_);
    assert(offset_ == 0);
    offset_ += header.getSize();
    isWrittenHeader_ = true;
}

void IndexedDiffWriter::writeDiff(const DiffIndexRecord &rec, const char *data)
{
    checkWrittenHeader();
    DiffIndexRecord r = rec;
    r.data_offset = offset_;
    if (rec.isNormal()) {
        assert(data != nullptr);
        // r.io_checksum must be up-to-date.
        r.updateRecChecksum();
        fileW_.write(data, rec.data_size);
        offset_ += rec.data_size;
    }
    indexMem_.add(r);
    n_data_++;
}

void IndexedDiffWriter::compressAndWriteDiff(
    const DiffIndexRecord &rec, const char *data, int type, int level)
{
    if (!rec.isNormal()) {
        writeDiff(rec, nullptr);
        return;
    }
    if (rec.isCompressed()) {
        writeDiff(rec, data);
        return;
    }
    size_t outSize = 0;
    type = compressData(data, rec.io_blocks * LOGICAL_BLOCK_SIZE,
                        buf_, outSize, type, level);
    DiffIndexRecord r = rec;
    r.compression_type = type;
    r.data_size = outSize;
    r.io_checksum = calcDiffIoChecksum(buf_);
    writeDiff(r, buf_.data());
}

void IndexedDiffWriter::init()
{
    indexMem_.clear();
    offset_ = 0;
    n_data_ = 0;
    isWrittenHeader_ = false;
    isClosed_ = false;
    stat_.clear();
}

void IndexedDiffWriter::writeSuper()
{
    DiffIndexSuper super;
    super.init();
    super.index_offset = offset_;
    super.n_records = indexMem_.size();
    super.n_data = n_data_;
    super.updateChecksum();

    fileW_.write(&super, sizeof(super));
}

AlignedArray* IndexedDiffCache::find(uint64_t key)
{
    auto it = map_.find(key);
    if (it == map_.end()) return nullptr;
    ListIt lit = it->second;

    std::unique_ptr<AlignedArray> dataPtr = std::move(lit->dataPtr);
    AlignedArray *ret = dataPtr.get();

    list_.erase(lit);
    list_.push_front({key, std::move(dataPtr)});
    map_[key] = list_.begin(); // update.

    return ret;
}

void IndexedDiffCache::add(uint64_t key, std::unique_ptr<AlignedArray> &&dataPtr)
{
    if (map_.find(key) != map_.end()) {
        throw cybozu::Exception("already in cache") << key;
    }

    curBytes_ += dataPtr->size();
    list_.push_front({key, std::move(dataPtr)});
    map_[key] = list_.begin();

    while (maxBytes_ > 0 && curBytes_ > maxBytes_ && map_.size() > 1) {
        evictOne();
    }
}

void IndexedDiffCache::evictOne()
{
    if (list_.empty()) return;
    Item &item = list_.back();
    map_.erase(item.key);
    curBytes_ -= item.dataPtr->size();
    list_.pop_back();
}

void IndexedDiffReader::setFile(cybozu::util::File &&fileR)
{
    memFile_.reset(std::move(fileR));

    // read header.
    ::memcpy(&header_, &memFile_[0], sizeof(header_));
    header_.verify();
    if (header_.type != WALB_DIFF_TYPE_INDEXED) {
        throw cybozu::Exception(NAME) << "Not indexed diff file" << header_.type;
    }

    // read index super.
    DiffIndexSuper super;
    idxEndOffset_  = memFile_.getFileSize() - sizeof(super);
    ::memcpy(&super, &memFile_[idxEndOffset_], sizeof(super));
    super.verify();
    idxBgnOffset_ = super.index_offset;
    idxOffset_ = idxBgnOffset_;
}

bool IndexedDiffReader::readDiff(DiffIndexRecord &rec, AlignedArray &data)
{
    if (!getNextRec(rec)) return false;
    if (!rec.isNormal()) return true;

    AlignedArray *aryPtr = cache_.find(rec.data_offset);
    if (aryPtr == nullptr) {
        std::unique_ptr<AlignedArray> p(new AlignedArray());
        p->resize(rec.orig_blocks * LOGICAL_BLOCK_SIZE);
        uncompressData(&memFile_[rec.data_offset], rec.data_size, *p, rec.compression_type);
        cache_.add(rec.data_offset, std::move(p));
        aryPtr = cache_.find(rec.data_offset);
    }

    const size_t offset = rec.io_offset * LOGICAL_BLOCK_SIZE;
    const size_t size = rec.io_blocks * LOGICAL_BLOCK_SIZE;
    data.resize(size);
    ::memcpy(data.data(), &(*aryPtr)[offset], size);

    return true;
}

bool IndexedDiffReader::getNextRec(DiffIndexRecord& rec)
{
    if (idxOffset_ >= idxEndOffset_) return false;
    ::memcpy(&rec, &memFile_[idxOffset_], sizeof(rec));
    idxOffset_ += sizeof(rec);
    return true;
}

} //namespace walb
