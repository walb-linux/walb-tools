/**
 * Calculate hash value of each blocks.
 */
#include "cybozu/option.hpp"
#include "cybozu/array.hpp"
#include "fileio.hpp"
#include "walb_util.hpp"
#include "siphash.hpp"
#include "bdev_util.hpp"
#include "bdev_reader.hpp"
#include "constant.hpp"
#include "throughput_util.hpp"


struct Option
{
    uint32_t ios;
    uint64_t off;
    uint64_t scanSize;
    uint64_t chunkSize;
    size_t pctScanSleep;
    bool useAio;
    bool useStdin;
    std::string filePath;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.appendOpt(&ios, 64 * 1024, "ios", ": read IO size [byte]. (default: 64K)");
        opt.appendOpt(&off, 0, "off", ": start offset [byte].");
        opt.appendOpt(&scanSize, 0, "size", ": scan size [byte]. (default: whole file size - start offset)");
        opt.appendOpt(&chunkSize, 0, "chunk", ": chunk size [byte]. put hash for each chunk. "
                      "(default: 0. 0 means there is just one chunk.)");
        opt.appendOpt(&pctScanSleep, 0, "scan-sleep-pct", "PERCENTAGE: sleep percentage in scan. (default: 0)");
        opt.appendBoolOpt(&useAio, "aio", ": use aio instead blocking IOs.");
        opt.appendParamOpt(&filePath, "", "FILE_PATH", ": path of a block device or a file. The empty string means stdin.");
        opt.appendHelp("h", ": put this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (ios == 0 || ios % LOGICAL_BLOCK_SIZE != 0) {
            throw cybozu::Exception("bad IO size") << ios;
        }
        verifyMultipleIos(off, "bad offset");
        verifyMultipleIos(scanSize, "bad scan size");
        verifyMultipleIos(chunkSize, "bad chunk size");
        if (pctScanSleep >= 100) {
            throw cybozu::Exception("pctScanSleep must be within from 0 to 99.") << pctScanSleep;
        }
        useStdin = filePath.empty();
    }
    void verifyMultipleIos(uint64_t v, const char *errMsg) {
        if (v != 0 && v % ios != 0) {
            throw cybozu::Exception(errMsg) << v;
        }
    }
};


void putChunkDigest(cybozu::SipHash24& hasher, uint64_t chunkId)
{
    cybozu::Hash128 hash = hasher.finalize128();
    ::printf("%016" PRIx64 "\t%s\n", chunkId, hash.str().c_str());
    ::fflush(::stdout);
}


void putWholeDigest(cybozu::SipHash24& hasher)
{
    cybozu::Hash128 hash = hasher.finalize128();
    ::printf("%s\n", hash.str().c_str());
    ::fflush(::stdout);
}



struct Reader
{
private:
    enum Mode { stdin, file, aio, };

    Mode mode_;
    uint64_t devLb_;
    cybozu::util::File file_;
    std::unique_ptr<walb::AsyncBdevReader> reader_;

public:
    void openStdin() {
        mode_ = Mode::stdin;
        file_.setFd(0);
        devLb_ = UINT64_MAX;
    }
    void openFileNoAio(const std::string& path, uint64_t offsetLb) {
        mode_ = Mode::file;
        file_.open(path, O_RDONLY); // CAUSION: O_DIRECT is not set.
        devLb_ = calcDevSizeLb(file_);
        file_.lseek(offsetLb * LOGICAL_BLOCK_SIZE);
    }
    void openFileAio(const std::string& path, uint64_t offsetLb, size_t maxIoLb) {
        mode_ = Mode::aio;
        file_.open(path, O_RDONLY);
        devLb_ = calcDevSizeLb(file_);
        file_.close();

        const size_t bufferSize = std::min<size_t>(
            64 << 20 /* 64MiB */, maxIoLb * LOGICAL_BLOCK_SIZE * 128);
        reader_.reset(new walb::AsyncBdevReader(path, offsetLb, bufferSize, maxIoLb * LOGICAL_BLOCK_SIZE));
        reader_->setReadAheadUnlimited();
    }
    uint64_t devLb() const {
        return devLb_;
    }
    static uint64_t calcDevSizeLb(cybozu::util::File& file) {
        return cybozu::util::getBlockDeviceSize(file.fd()) / LOGICAL_BLOCK_SIZE;
    }
    size_t read(void *data, size_t size) {
        switch (mode_) {
        case Mode::aio:
            reader_->read(data, size);
            break;
        case Mode::file:
            file_.read(data, size);
            break;
        case Mode::stdin:
            char *p = static_cast<char *>(data);
            size_t total = 0;
            while (total < size) {
                size_t rs = file_.readsome(p, size - total);
                if (rs == 0) return total;
                p += rs;
                total += rs;
            }
            break;
        }
        return size;
    }
};




int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);

    const size_t maxIoLb = opt.ios / LOGICAL_BLOCK_SIZE;
    const uint64_t offsetLb = opt.off / LOGICAL_BLOCK_SIZE;

    Reader reader;
    if (opt.useStdin) {
        reader.openStdin();
    } else if (opt.useAio) {
        reader.openFileAio(opt.filePath, offsetLb, maxIoLb);
    } else {
        reader.openFileNoAio(opt.filePath, offsetLb);
    }

    const uint64_t maxLb = reader.devLb() - offsetLb;
    const uint64_t scanLb = (opt.scanSize == 0 ? maxLb : opt.scanSize / LOGICAL_BLOCK_SIZE);
    if (scanLb > maxLb) {
        throw cybozu::Exception("specified scanLb is larger than maxLb")
            << scanLb << maxLb;
    }

    const bool useChunk = opt.chunkSize > 0;
    const uint64_t chunkLb = useChunk ? opt.chunkSize / LOGICAL_BLOCK_SIZE : scanLb;

    Sleeper sleeper;
    const size_t minMs = 100, maxMs = 1000;
    double t0 = cybozu::util::getTimeMonotonic();
    sleeper.init(opt.pctScanSleep * 10, minMs, maxMs, t0);

    cybozu::SipHash24 hasher;
    cybozu::AlignedArray<char> buf(opt.ios, false);

    uint64_t readLb = 0; // read size in chunk [logical block].
    uint64_t chunkId = offsetLb; // offset of the current chunk [logical block].
    uint64_t remainingLb = scanLb;
    while (remainingLb > 0) {
        const size_t ioLb = std::min<uint64_t>({maxIoLb, chunkLb - readLb, remainingLb});
        const size_t ioBytes = ioLb * LOGICAL_BLOCK_SIZE;
        const size_t ioBytes2 = reader.read(buf.data(), ioBytes);
        if (ioBytes2 % LOGICAL_BLOCK_SIZE != 0) {
            throw cybozu::Exception("readBytes is not multiples of LOGICAL_BLOCK_SIZE") << ioBytes2;
        }
        const size_t ioLb2 = ioBytes2 / LOGICAL_BLOCK_SIZE;
        hasher.compress(buf.data(), ioBytes2);
        readLb += ioLb2;
        if (ioLb2 < ioLb) break;
        if (useChunk && readLb == chunkLb) {
            putChunkDigest(hasher, chunkId);
            hasher.init();
            readLb = 0;
            chunkId += chunkLb;
        }
        remainingLb -= ioLb2;
        sleeper.sleepIfNecessary(cybozu::util::getTimeMonotonic());
    }
    if (useChunk && readLb > 0) {
        putChunkDigest(hasher, chunkId);
    }
    if (!useChunk) {
        putWholeDigest(hasher);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("bhash");
