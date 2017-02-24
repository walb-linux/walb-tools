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

struct Option
{
    uint32_t ios;
    uint64_t off;
    uint64_t scanSize;
    uint64_t chunkSize;
    bool useAio;
    std::string filePath;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.appendOpt(&ios, 64 * 1024, "ios", ": read IO size [byte]. (default: 64K)");
        opt.appendOpt(&off, 0, "off", ": start offset [byte].");
        opt.appendOpt(&scanSize, 0, "size", ": scan size [byte]. (default: whole file size - start offset)");
        opt.appendOpt(&chunkSize, 0, "chunk", ": chunk size [byte]. put hash for each chunk. "
                      "(default: 0. 0 means there is just one chunk.)");
        opt.appendBoolOpt(&useAio, "aio", ": use aio instead blocking IOs.");
        opt.appendParam(&filePath, "FILE_PATH", ": path of a block device or a file.");
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
}


void putWholeDigest(cybozu::SipHash24& hasher)
{
    cybozu::Hash128 hash = hasher.finalize128();
    ::printf("%s\n", hash.str().c_str());
}


int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);

    cybozu::util::File file(opt.filePath, O_RDONLY);
    std::unique_ptr<walb::AsyncBdevReader> reader;

    const size_t maxIoLb = opt.ios / LOGICAL_BLOCK_SIZE;
    const uint64_t offsetLb = opt.off / LOGICAL_BLOCK_SIZE;
    uint64_t scanLb;
    if (opt.scanSize == 0) {
        const uint64_t bytes = cybozu::util::getBlockDeviceSize(file.fd());
        scanLb = bytes / LOGICAL_BLOCK_SIZE - offsetLb;
    } else {
        scanLb = opt.scanSize / LOGICAL_BLOCK_SIZE;
    }
    const bool useChunk = opt.chunkSize > 0;
    const uint64_t chunkLb = useChunk ? opt.chunkSize / LOGICAL_BLOCK_SIZE : scanLb;

    if (opt.useAio) {
        file.close();
        const size_t bufferSize = std::min<size_t>(64 << 20 /* 64MiB */, opt.ios * 128);
        reader.reset(new walb::AsyncBdevReader(opt.filePath, offsetLb, bufferSize, opt.ios));
    }

    cybozu::SipHash24 hasher;
    cybozu::AlignedArray<char> buf(opt.ios, false);

    uint64_t readLb = 0; // read size in chunk [logical block].
    uint64_t chunkId = offsetLb; // offset of the current chunk [logical block].
    uint64_t remainingLb = scanLb;
    while (remainingLb > 0) {
        const size_t ioLb = std::min<uint64_t>({maxIoLb, chunkLb - readLb, remainingLb});
        const size_t ioBytes = ioLb * LOGICAL_BLOCK_SIZE;
        if (opt.useAio) {
            reader->read(buf.data(), ioBytes);
        } else {
            file.read(buf.data(), ioBytes);
        }
        hasher.compress(buf.data(), ioBytes);
        readLb += ioLb;
        if (useChunk && readLb == chunkLb) {
            putChunkDigest(hasher, chunkId);
            hasher.init();
            readLb = 0;
            chunkId += chunkLb;
        }
        remainingLb -= ioLb;
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
