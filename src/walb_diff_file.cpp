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
        "  type: %s\n"
        "  maxIoBlocks: %u\n"
        "  uuid: %s\n"
        , checksum, version, typeStr().c_str(), max_io_blocks, getUuid().str().c_str());
}

bool DiffFileHeader::isIndexed() const
{
    switch (type) {
    case WALB_DIFF_TYPE_SORTED:
        return false;
    case WALB_DIFF_TYPE_INDEXED:
        return true;
    default:
        throw cybozu::Exception(NAME) << "Invalid type" << type;
    }
}

std::string DiffFileHeader::typeStr() const
{
    switch (type) {
    case WALB_DIFF_TYPE_SORTED:
        return "sorted";
    case WALB_DIFF_TYPE_INDEXED:
        return "indexed";
    default:
        return std::string("unknown:") + std::to_string(type);
    }
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
    header.type = ::WALB_DIFF_TYPE_SORTED;
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
    isClosed_ = true;
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


void DiffReader::readHeader(DiffFileHeader &head, bool doReadPackHeader)
{
    if (isReadHeader_) {
        throw RT_ERR("Do not call readHeader() more than once.");
    }
    head.readFrom(fileR_);
    if (head.type != WALB_DIFF_TYPE_SORTED) {
        cybozu::Exception(NAME)
            << "readHeader" << "Not sorted diff file" << head.type;
    }
    isReadHeader_ = true;
    if (doReadPackHeader) readPackHeader();
}

bool DiffReader::readDiff(DiffRecord &rec, DiffIo &io)
{
    if (!prepareRead()) return false;
    assert(pack_.n_records == 0 || recIdx_ < pack_.n_records);
    rec = pack_[recIdx_];

    if (!rec.isValid()) {
        throw cybozu::Exception(__func__)
            << "invalid record" << fileR_.fd() << recIdx_ << rec;
    }
    readDiffIo(rec, io);
    return true;
}

bool DiffReader::readAndUncompressDiff(DiffRecord &rec, DiffIo &io, bool calcChecksum)
{
    if (!readDiff(rec, io)) {
        return false;
    }
    if (!rec.isNormal() || !rec.isCompressed()) {
        return true;
    }
    DiffRecord outRec;
    DiffIo outIo;
    uncompressDiffIo(rec, io.get(), outRec, outIo.data, calcChecksum);
    outIo.set(outRec);
    rec = outRec;
    io = std::move(outIo);
    return true;
}

bool DiffReader::prepareRead()
{
    if (pack_.isEnd()) return false;
    bool ret = true;
    if (recIdx_ == pack_.n_records) {
        ret = readPackHeader();
    }
    return ret;
}

void DiffReader::readDiffIo(const DiffRecord &rec, DiffIo &io, bool verifyChecksum)
{
    if (rec.data_offset != totalSize_) {
        throw cybozu::Exception(__func__)
            << "data offset invalid" << rec.data_offset << totalSize_;
    }
    io.setAndReadFrom(rec, fileR_, verifyChecksum);
    totalSize_ += rec.data_size;
    recIdx_++;
}

bool DiffReader::readPackHeader()
{
    try {
        pack_.readFrom(fileR_);
    } catch (cybozu::util::EofError &e) {
        pack_.setEnd();
        return false;
    }
    if (pack_.isEnd()) return false;
    recIdx_ = 0;
    totalSize_ = 0;
    stat_.update(pack_);
    return true;
}

void DiffReader::init()
{
    pack_.clear();
    isReadHeader_ = false;
    recIdx_ = 0;
    totalSize_ = 0;
    stat_.clear();
    stat_.wdiffNr = 1;
}

void DiffIndexMem::checkNoOverlappedAndSorted() const
{
    auto it = index_.cbegin();
    const IndexedDiffRecord *prev = nullptr;
    while (it != index_.cend()) {
        const IndexedDiffRecord *curr = &it->second;
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

std::vector<IndexedDiffRecord> DiffIndexMem::getAsVec() const
{
    std::vector<IndexedDiffRecord> ret;
    ret.reserve(index_.size());
    for (const Map::value_type& pair : index_) {
        ret.push_back(pair.second);
    }
    return ret;
}

void DiffIndexMem::addDetail(const IndexedDiffRecord &rec)
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
    std::queue<IndexedDiffRecord> q;
    while (it != index_.end() && it->first < addr1) {
        IndexedDiffRecord &r = it->second;
        if (r.isOverlapped(rec)) {
            q.push(r);
            it = index_.erase(it);
        } else {
            ++it;
        }
    }

    /* Eliminate overlaps. */
    while (!q.empty()) {
        for (const IndexedDiffRecord &r0 : q.front().minus(rec)) {
            for (const IndexedDiffRecord &r1 : r0.split()) {
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

    indexMem_.writeTo(fileW_, &stat_);
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
    header.type = ::WALB_DIFF_TYPE_INDEXED;
    header.writeTo(fileW_);
    assert(offset_ == 0);
    offset_ += header.getSize();
    isWrittenHeader_ = true;
}

void IndexedDiffWriter::writeDiff(const IndexedDiffRecord &rec, const char *data)
{
    checkWrittenHeader();
    IndexedDiffRecord r = rec;
    r.data_offset = offset_;
    if (rec.isNormal()) {
        assert(data != nullptr);
        // r.io_checksum must be up-to-date.
        r.updateRecChecksum();
        fileW_.write(data, rec.data_size);
        stat_.dataSize += rec.data_size;
        offset_ += rec.data_size;
    }
    indexMem_.add(r);
    n_data_++;
}

void IndexedDiffWriter::compressAndWriteDiff(
    const IndexedDiffRecord &rec, const char *data, int type, int level)
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
    IndexedDiffRecord r = rec;
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
    isClosed_ = true;
    stat_.clear();
    stat_.wdiffNr = 1;
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

AlignedArray* IndexedDiffCache::find(Key key)
{
    auto it = map_.find(key);
    if (it == map_.end()) return nullptr;
    ListIt lit = it->second;

    std::unique_ptr<AlignedArray> dataPtr = std::move(lit->dataPtr);
    AlignedArray *ret = dataPtr.get();

    lruList_.erase(lit);
    lruList_.push_front({key, std::move(dataPtr)});
    map_[key] = lruList_.begin(); // update.

    return ret;
}

void IndexedDiffCache::add(Key key, std::unique_ptr<AlignedArray> &&dataPtr)
{
    if (map_.find(key) != map_.end()) {
        throw cybozu::Exception("already in cache") << key;
    }

    curBytes_ += dataPtr->size();
    lruList_.push_front({key, std::move(dataPtr)});
    map_[key] = lruList_.begin();

    while (curBytes_ > maxBytes_ && map_.size() > 1) {
        evictOne();
    }
}

void IndexedDiffCache::clear()
{
    lruList_.clear();
    map_.clear();
    curBytes_ = 0;
}

void IndexedDiffCache::evictOne()
{
    if (lruList_.empty()) return;
    Item &item = lruList_.back();
    map_.erase(item.key);
    curBytes_ -= item.dataPtr->size();
    lruList_.pop_back();
}

void IndexedDiffReader::setFile(cybozu::util::File &&fileR, IndexedDiffCache &cache)
{
    if (!fileR.seekable()) {
        throw cybozu::Exception(NAME)
            << "non-seekable file descriptor is not supported" << fileR.fd();
    }
    cache_ = &cache;
    memFile_.setReadOnly();
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

    stat_.clear();
    stat_.wdiffNr = 1;
    stat_.dataSize = idxBgnOffset_ - sizeof(header_);
}

bool IndexedDiffReader::readDiffRecord(IndexedDiffRecord &rec, bool doVerify)
{
    if (!getNextRec(rec)) return false;
    if (doVerify) rec.verify();
    stat_.update(rec);
    return true;
}

bool IndexedDiffReader::isOnCache(const IndexedDiffRecord &rec) const
{
    const IndexedDiffCache::Key key{this, rec.data_offset};
    return cache_->find(key) != nullptr;
}

bool IndexedDiffReader::loadToCache(const IndexedDiffRecord &rec, bool throwError)
{
    assert(!isOnCache(rec));
    if (!verifyIoData(rec.data_offset, rec.data_size, rec.io_checksum, throwError)) {
        return false;
    }
    std::unique_ptr<AlignedArray> p(new AlignedArray());
    p->resize(rec.orig_blocks * LOGICAL_BLOCK_SIZE);
    uncompressData(&memFile_[rec.data_offset], rec.data_size, *p, rec.compression_type);
    const IndexedDiffCache::Key key{this, rec.data_offset};
    cache_->add(key, std::move(p));
    return true;
}

/**
 * Uncompressed data will be set while rec indicates compression.
 */
void IndexedDiffReader::readDiffIo(const IndexedDiffRecord &rec, AlignedArray &data)
{
    if (!rec.isNormal()) return;
    if (cache_ == nullptr) {
        throw cybozu::Exception(NAME) << "BUG: cache_ must be set.";
    }

    const IndexedDiffCache::Key key{this, rec.data_offset};
    AlignedArray *aryPtr = cache_->find(key);
    if (aryPtr == nullptr) {
        loadToCache(rec);
        aryPtr = cache_->find(key);
    }

    const size_t offset = rec.io_offset * LOGICAL_BLOCK_SIZE;
    const size_t size = rec.io_blocks * LOGICAL_BLOCK_SIZE;
    data.resize(size);
    ::memcpy(data.data(), &(*aryPtr)[offset], size);
}

bool IndexedDiffReader::getNextRec(IndexedDiffRecord& rec)
{
    if (idxOffset_ >= idxEndOffset_) return false;
    ::memcpy(&rec, &memFile_[idxOffset_], sizeof(rec));
    idxOffset_ += sizeof(rec);
    return true;
}

bool IndexedDiffReader::verifyIoData(uint64_t offset, uint32_t size, uint32_t csum, bool throwError) const
{
    if (cybozu::util::calcChecksum(&memFile_[offset], size, 0) == csum) {
        return true;
    }
    if (throwError) {
        throw cybozu::Exception(NAME) << "IO data invalid" << offset << size;
    }
    return false;
}


} //namespace walb
