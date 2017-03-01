#include "aio2_util.hpp"
#include "fileio.hpp"

void Aio2::AioData::init(uint32_t key, int type, off_t oft, walb::AlignedArray&& buf)
{
    this->key = key;
    this->type = type;
    ::memset(&iocb, 0, sizeof(iocb));
    this->oft = oft;
    this->size = buf.size();
    this->buf = std::move(buf);
    err = 0;
}

void Aio2::init(int fd, size_t queueSize)
{
    if (isInitialized_.test_and_set()) {
        throw cybozu::Exception("Aio2: do not call init() more than once");
    }
    assert(fd > 0);
    fd_ = fd;
    queueSize_ = queueSize;
    int err = ::io_queue_init(queueSize_, &ctx_);
    if (err < 0) {
        throw cybozu::Exception("Aio2 init failed") << cybozu::ErrorNo(-err);
    }
}

uint32_t Aio2::prepareRead(off_t oft, size_t size)
{
    if (++nrIOs_ > queueSize_) {
        --nrIOs_;
        throw cybozu::Exception("prepareRead: queue is full");
    }
    const uint32_t key = key_++;
    AioDataPtr iop(new AioData());
    iop->init(key, 0, oft, walb::AlignedArray(size));
    ::io_prep_pread(&iop->iocb, fd_, iop->buf.data(), size, oft);
    ::memcpy(&iop->iocb.data, &key, sizeof(key));
    pushToSubmitQ(std::move(iop));
    return key;
}

uint32_t Aio2::prepareWrite(off_t oft, walb::AlignedArray&& buf)
{
    if (++nrIOs_ > queueSize_) {
        --nrIOs_;
        throw cybozu::Exception("prepareWrite: queue is full");
    }
    const uint32_t key = key_++;
    AioDataPtr iop(new AioData());
    const size_t size = buf.size();
    iop->init(key, 1, oft, std::move(buf));
    ::io_prep_pwrite(&iop->iocb, fd_, iop->buf.data(), size, oft);
    ::memcpy(&iop->iocb.data, &key, sizeof(key));
    pushToSubmitQ(std::move(iop));
    return key;
}

void Aio2::submit()
{
    std::vector<AioDataPtr> submitQ;
    {
        AutoLock lk(mutex_);
        submitQ = std::move(submitQ_);
        submitQ_.clear();
    }
    const size_t nr = submitQ.size();
    std::vector<struct iocb *> iocbs(nr);
    for (size_t i = 0; i < nr; i++) {
        iocbs[i] = &submitQ[i]->iocb;
    }
    {
        AutoLock lk(mutex_);
        for (size_t i = 0; i < nr; i++) {
            AioDataPtr iop = std::move(submitQ[i]);
            const uint32_t key = iop->key;
            pendingIOs_.emplace(key, std::move(iop));
        }
    }
    size_t done = 0;
    while (done < nr) {
        int err = ::io_submit(ctx_, nr - done, &iocbs[done]);
        if (err < 0) {
            throw cybozu::Exception("Aio2 submit failed") << cybozu::ErrorNo(-err);
        }
        done += err;
    }
}

walb::AlignedArray Aio2::waitFor(uint32_t key)
{
    verifyKeyExists(key);
    AioDataPtr iop;
    while (!popCompleted(key, iop)) {
        waitDetail();
    }
    verifyNoError(*iop);
    --nrIOs_;
    return std::move(iop->buf);
}

walb::AlignedArray Aio2::waitAny(uint32_t* keyP)
{
    AioDataPtr iop;
    while (!popCompletedAny(iop)) {
        waitDetail();
    }
    verifyNoError(*iop);
    --nrIOs_;
    if (keyP) *keyP = iop->key;
    return std::move(iop->buf);
}

bool Aio2::popCompleted(uint32_t key, AioDataPtr& iop)
{
    AutoLock lk(mutex_);
    Umap::iterator it = completedIOs_.find(key);
    if (it == completedIOs_.end()) return false;
    iop = std::move(it->second);
    assert(iop->key == key);
    completedIOs_.erase(it);
    return true;
}

bool Aio2::popCompletedAny(AioDataPtr& iop)
{
    AutoLock lk(mutex_);
    if (completedIOs_.empty()) return false;
    Umap::iterator it = completedIOs_.begin();
    iop = std::move(it->second);
    completedIOs_.erase(it);
    return true;
}

size_t Aio2::waitDetail(size_t minNr)
{
    size_t maxNr = nrIOs_;
    if (maxNr < minNr) maxNr = minNr;
    std::vector<struct io_event> ioEvents(maxNr);
    int nr = ::io_getevents(ctx_, minNr, maxNr, &ioEvents[0], NULL);
    if (nr < 0) {
        throw cybozu::Exception("io_getevents failed") << cybozu::ErrorNo(-nr);
    }
    AutoLock lk(mutex_);
    for (int i = 0; i < nr; i++) {
        const uint32_t key = getKeyFromEvent(ioEvents[i]);
        Umap::iterator it = pendingIOs_.find(key);
        assert(it != pendingIOs_.end());
        AioDataPtr& iop = it->second;
        assert(iop->key == key);
        iop->err = ioEvents[i].res;
        completedIOs_.emplace(key, std::move(iop));
        pendingIOs_.erase(it);
    }
    return nr;
}

void Aio2::verifyNoError(const AioData& io) const
{
    if (io.err == 0) {
        throw cybozu::util::EofError();
    }
    if (io.err < 0) {
        throw cybozu::Exception("Aio2: IO failed") << io.key << cybozu::ErrorNo(-io.err);
    }
    assert(io.iocb.u.c.nbytes == static_cast<uint>(io.err));
}

void Aio2::waitAll()
{
    for (;;) {
        size_t size;
        {
            AutoLock lk(mutex_);
            size = pendingIOs_.size();
        }
        if (size == 0) break;
        try {
            waitDetail();
        } catch (...) {
            break;
        }
    }
}
