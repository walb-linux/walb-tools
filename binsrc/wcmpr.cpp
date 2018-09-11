/*
 * Simple file compressor/decompressor.
 */
#include <thread>
#include <vector>
#include "cybozu/option.hpp"
#include "cybozu/serializer.hpp"
#include "fileio.hpp"
#include "fileio_serializer.hpp"
#include "thread_util.hpp"
#include "walb_util.hpp"
#include "compressor.hpp"
#include "compression_type.hpp"

using namespace walb;
using Buffer = AlignedArray;

struct Option
{
    size_t unitSize;
    bool isDecompress;
    bool isDebug;
    std::string modeStr;
    size_t concurrency;
    size_t level;
    std::vector<std::string> v;
    std::string srcPath, dstPath;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Simple compressor/decompressor of a file.\n");
        opt.appendOpt(&modeStr, "snappy", "m", ": compression mode (snappy, gzip, lzma, lz4, zstd)");
        opt.appendOpt(&concurrency, 0, "c", ": number of concurrency");
        opt.appendOpt(&unitSize, 1U << 20, "s", ": unit size to compress (default: 1MiB).");
        opt.appendOpt(&level, 0, "l", ": compression level [0, 9] (default: 0).");
        opt.appendBoolOpt(&isDecompress, "d", ": decompress instead compress.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show thie message.");
        opt.appendParamVec(&v, "(SRC_PATH (DST_PATH))", ": source/destination file path. "
                           "If not spedified, stdin/stdout will be used.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (concurrency == 0) {
            concurrency = std::thread::hardware_concurrency();
        }
        assert(concurrency != 0);
        if (unitSize == 0) {
            throw cybozu::Exception("bad unit size") << unitSize;
        }
        if (level > 9) {
            throw cybozu::Exception("bad level") << level;
        }

        if (!v.empty()) {
            srcPath = v[0];
        }
        if (v.size() >= 2) {
            dstPath = v[1];
        }

        if (!srcPath.empty() && dstPath.empty()) {
            if (isDecompress) {
                std::string ext(".");
                ext += modeStr;
                size_t pos = srcPath.rfind(ext);
                if (pos != std::string::npos && pos + ext.size() == srcPath.size()) {
                    dstPath = srcPath.substr(0, pos);
                }
            } else {
                dstPath = srcPath + "." + modeStr;
            }
        }
    }
    int getMode() const {
        return parseCompressionType(modeStr);
    }
    bool isStdin() const {
        return srcPath.empty();
    }
    bool isStdout() const {
        return dstPath.empty();
    }
};

static const std::string HEADER_STRING("walb-compressed");

void writeFileHeader(cybozu::util::File &outFile, int mode)
{
    cybozu::save(outFile, HEADER_STRING);
    cybozu::save(outFile, compressionTypeToStr(mode));
    cybozu::save(outFile, mode);
}

int readFileHeader(cybozu::util::File &inFile)
{
    std::string s;
    cybozu::load(s, inFile);
    if (s != HEADER_STRING) {
        throw cybozu::Exception("bad header") << s;
    }
    std::string modeStr;
    cybozu::load(modeStr, inFile);
    int mode;
    cybozu::load(mode, inFile);
    if (compressionTypeToStr(mode) != modeStr) {
        throw cybozu::Exception("bad mode") << mode << modeStr;
    }
    return mode;
}

struct BlockHeader
{
    bool isEnd;
    size_t origSize;
    size_t cmprSize;

    template <typename Writer>
    void save(Writer& os) const {
        cybozu::save(os, isEnd);
        cybozu::save(os, origSize);
        cybozu::save(os, cmprSize);
    }
    template <typename Reader>
    void load(Reader& is) {
        cybozu::load(isEnd, is);
        cybozu::load(origSize, is);
        cybozu::load(cmprSize, is);
        if (!isEnd) {
            if (origSize == 0 || cmprSize == 0) {
                throw cybozu::Exception("bad BlockHeader")
                    << isEnd << origSize << cmprSize;
            }
        }
    }
};

void compress(cybozu::util::File &inFile, cybozu::util::File &outFile,
              int mode, int level, size_t unitSize)
{
    writeFileHeader(outFile, mode);
    Compressor c(mode, level);
    Buffer src(unitSize);
    Buffer dst(unitSize * 2);

    for (;;) {
        const size_t rs = inFile.readsome(src.data(), src.size());
        if (rs == 0) break;
        size_t ws;
        if (c.run(dst.data(), &ws, dst.size(), src.data(), rs) && ws <= rs) {
            const BlockHeader bh = { false, rs, ws };
            cybozu::save(outFile, bh);
            outFile.write(dst.data(), ws);
        } else {
            throw cybozu::Exception("wcmpr:compres:error");
        }
    }
    const BlockHeader endMark = { true, 0, 0 };
    cybozu::save(outFile, endMark);
}

void decompress(cybozu::util::File &inFile, cybozu::util::File &outFile)
{
    const int mode = readFileHeader(inFile);
    Uncompressor d(mode);
    Buffer src, dst;

    for (;;) {
        BlockHeader bh;
        cybozu::load(bh, inFile);
        if (bh.isEnd) break;
        src.resize(bh.cmprSize);
        dst.resize(bh.origSize);
        inFile.read(src.data(), src.size());

        const size_t ws = d.run(dst.data(), dst.size(), src.data(), src.size());
        if (ws != bh.origSize) {
            throw cybozu::Exception("bad size")
                << ws << bh.origSize << bh.cmprSize;
        }
        outFile.write(dst.data(), dst.size());
    }
}

struct Cmpr
{
    Compressor compr_;
    Uncompressor uncompr_;
    explicit Cmpr(int mode, size_t level)
        : compr_(mode, level)
        , uncompr_(mode) {
    }
    Buffer compress(void *data, size_t size, size_t enoughSize) {
        Buffer dst(enoughSize);
        size_t s;
        if (compr_.run(dst.data(), &s, dst.size(), data, size)) {
            dst.resize(s);
        } else {
            throw cybozu::Exception("Cmpr:compress");
        }
        return dst;
    }
    Buffer uncompress(void *data, size_t size, size_t origSize) {
        Buffer dst(origSize);
        size_t s = uncompr_.run(dst.data(), dst.size(), data, size);
        if (s != origSize) {
            throw cybozu::Exception("uncompress: bad size")
                << s << origSize << size;
        }
        return dst;
    }
};

struct CmprData
{
    size_t origSize;
    Buffer data;
};

void parallelCompress(
    cybozu::util::File &inFile, cybozu::util::File &outFile,
    int mode, int level, size_t unitSize, size_t concurrency)
{
    writeFileHeader(outFile, mode);
    Cmpr cmpr(mode, level);

    cybozu::thread::ParallelConverter<Buffer, CmprData> pconv([&](Buffer&& src) {
        Buffer dst = cmpr.compress(src.data(), src.size(), src.size() * 2 + 4096);
        return CmprData { src.size(), std::move(dst) };
    });
    pconv.setLogger([](const std::exception& e) { LOGs.error() << e.what(); });
    pconv.start(concurrency);

    cybozu::thread::ThreadRunner writer([&]() {
            try {
                CmprData cmprD;
                while (pconv.pop(cmprD)) {
                    const BlockHeader bh = { false, cmprD.origSize, cmprD.data.size() };
                    cybozu::save(outFile, bh);
                    outFile.write(cmprD.data.data(), cmprD.data.size());
                }
                const BlockHeader endMark = { true, 0, 0 };
                cybozu::save(outFile, endMark);
            } catch (...) {
                pconv.fail();
            }
        });
    writer.start();

    for (;;) {
        Buffer src(unitSize);
        const size_t rs = inFile.readsome(src.data(), src.size());
        if (rs == 0) break;
        src.resize(rs);
        pconv.push(std::move(src));
    }
    pconv.sync();
    writer.join();
}

void parallelDecompress(
    cybozu::util::File &inFile, cybozu::util::File &outFile,
    size_t concurrency)
{
    const int mode = readFileHeader(inFile);
    Cmpr cmpr(mode, 0);

    cybozu::thread::ParallelConverter<CmprData, Buffer> pconv([&](CmprData&& src) {
            return cmpr.uncompress(src.data.data(), src.data.size(), src.origSize);
        });
    pconv.setLogger([](const std::exception& e) { LOGs.error() << e.what(); });
    pconv.start(concurrency);

    cybozu::thread::ThreadRunner writer([&]() {
            try {
                Buffer buf;
                while (pconv.pop(buf)) {
                    outFile.write(buf.data(), buf.size());
                }
            } catch (...) {
                pconv.fail();
            }
        });
    writer.start();

    for (;;) {
        BlockHeader bh;
        cybozu::load(bh, inFile);
        if (bh.isEnd) break;
        Buffer src(bh.cmprSize);
        inFile.read(src.data(), src.size());
        pconv.push(CmprData { bh.origSize, std::move(src) });
    }
    pconv.sync();
    writer.join();
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File inFile, outFile;

    if (opt.isStdin()) {
        inFile.setFd(0);
    } else {
        inFile.open(opt.srcPath, O_RDONLY);
    }
    if (opt.isStdout()) {
        outFile.setFd(1);
    } else {
        outFile.open(opt.dstPath, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    }

    if (opt.isDecompress) {
#if 0
        decompress(inFile, outFile);
#else
        parallelDecompress(inFile, outFile, opt.concurrency);
#endif
    } else {
#if 0
        compress(inFile, outFile, opt.getMode(), opt.level, opt.unitSize);
#else
        parallelCompress(inFile, outFile,
                         opt.getMode(), opt.level, opt.unitSize,
                         opt.concurrency);
#endif
    }
    inFile.close();
    outFile.close();
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wcmpr");
