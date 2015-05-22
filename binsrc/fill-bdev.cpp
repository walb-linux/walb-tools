/*
 * Fill block device.
 */
#include <string>
#include "cybozu/option.hpp"
#include "fileio.hpp"
#include "random.hpp"
#include "bdev_util.hpp"
#include "walb_util.hpp"
#include "walb_types.hpp"
#include "constant.hpp"

using namespace walb;

struct Option
{
    size_t bs;
    uint64_t offset;
    uint64_t size;
    size_t diffPml;
    size_t randPml;
    uint64_t fsyncIntervalSize;
    bool isDebug;
    std::string bdevPath;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Fill a block device or file randomly.\n");
        opt.appendOpt(&offset, 0, "o", "OFFSET: offset to start filling [byte].");
        opt.appendOpt(&size, 0, "s", "SIZE: filling target size [byte].");
        opt.appendOpt(&bs, 64 << 10, "b", "SIZE: block size [byte] (default: 64K).");
        opt.appendOpt(&diffPml, 10, "dp", "PERMILLAGE: what permillage of blocks will be written [0,1000].");
        opt.appendOpt(&randPml, 0, "rp", "PERMILLAGE: random data permillage in each written block [0, 1000].");
        opt.appendOpt(&fsyncIntervalSize, 128 * MEBI, "fi", "fsync interval size [bytes].");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendParam(&bdevPath, "DEVICE_PATH");
        opt.appendHelp("h", ": show this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        if (diffPml > 1000) {
            throw cybozu::Exception("bad permillage for -dp") << diffPml;
        }
        if (randPml > 1000) {
            throw cybozu::Exception("bad permillage for -rp") << randPml;
        }
        if (bs == 0 || bs > (32 << 20) || bs % 512 != 0) {
            throw cybozu::Exception("bad block size") << bs;
        }
        if (offset % 512 != 0) {
            throw cybozu::Exception("bad offset") << offset;
        }
        if (size > 0 && size % 512 != 0) {
            throw cybozu::Exception("bad size") << size;
        }
        if (fsyncIntervalSize == 0) {
            throw cybozu::Exception("bad fsyncIntervalSize") << fsyncIntervalSize;
        }
    }
};

class ProgressPrinter
{
    static const uint64_t PERIOD_SIZE = 16ULL << 20; // 16MiB
    static const uint64_t GIBI = 1ULL << 30; // 1GiB
    FILE *fp_;
    uint64_t size_;
public:
    ProgressPrinter(FILE *fp) : fp_(fp), size_(0) {}
    void progress(uint64_t size) {
        size_ += size;
        if (size_ % PERIOD_SIZE == 0) {
            ::fprintf(fp_, ".");
            ::fflush(fp_);
            if (size_ % GIBI == 0) {
                putSize();
            }
        }
    }
    void end() {
        ::fprintf(fp_, "\ntotally ");
        putSize();
    }

private:
    void putSize() {
        ::fprintf(fp_, "%siB\n", cybozu::util::toUnitIntString(size_).c_str());
    }
};

template <typename Rand>
void diffuse(Rand& rand, const AlignedArray& src, AlignedArray& dst)
{
    assert(src.size() <= dst.size());
    size_t nrSpace = dst.size() - src.size();

    size_t i = 0, j = 0;
    while (i < src.size()) {
        if (nrSpace == 0 || rand() % dst.size() < src.size()) {
            dst[j++] = src[i++];
        } else {
            j++;
            nrSpace--;
        }
    };
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File file(opt.bdevPath, O_RDWR);
    const uint64_t bdevSize = cybozu::util::getBlockDeviceSize(file.fd());
    if (opt.offset + opt.size > bdevSize) {
        throw cybozu::Exception("bad filling area")
            << opt.offset << opt.size << bdevSize;
    }
    file.lseek(opt.offset);
    const size_t bs = opt.bs;
    const size_t fillSize = bs * opt.randPml / 1000;
    assert(fillSize <= bs);
    AlignedArray rbuf(fillSize, false);
    uint64_t size = opt.size == 0 ? (bdevSize - opt.offset) : opt.size;
    assert(size % 512 == 0);
    const uint64_t rem = size % bs;
    if (rem != 0) size -= rem;
    assert(size % bs == 0);
    AlignedArray wbuf(bs, false);
#if 0
    cybozu::util::Random<size_t> rand;
#else
    cybozu::util::XorShift128 rand(::time(0));
#endif

    ProgressPrinter pp(::stderr);
    uint64_t nrUpdated = 0, nrSkipped = 0;
    uint64_t remaining = size;
    uint64_t written = 0;
    while (remaining > 0) {
        if (rand() % 1000 < opt.diffPml) {
            rand.fill(rbuf.data(), rbuf.size());
            ::memset(wbuf.data(), 0, bs);
            diffuse(rand, rbuf, wbuf);
            file.write(wbuf.data(), bs);
            nrUpdated++;
            written += bs;
        } else {
            file.lseek(bs, SEEK_CUR);
            nrSkipped++;
        }
        remaining -= bs;
        pp.progress(bs);
        if (written >= opt.fsyncIntervalSize) {
            file.fdatasync();
            written = 0;
        }
    }
    pp.end();
    file.fdatasync();
    file.close();
    ::printf("updated blocks: %" PRIu64 "\n"
             "skipped blocks: %" PRIu64 "\n"
             "update permill: %" PRIu64 "\n"
             , nrUpdated, nrSkipped
             , nrUpdated * 1000 / (nrUpdated + nrSkipped));
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("fill-bdev")
