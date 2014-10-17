/*
 * Modify its buffer during write IO.
 */
#include "aio_util.hpp"
#include "fileio.hpp"
#include "walb_types.hpp"
#include "walb_util.hpp"
#include "cybozu/exception.hpp"
#include "cybozu/option.hpp"

using namespace walb;

struct Option
{
    size_t bs;
    size_t nr;
    uint64_t offsetB;
    std::string bdevPath;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("modify-during-write: modify its buffer during write IO.");
        opt.appendOpt(&bs, 512, "b", ": block size [byte].");
        opt.appendOpt(&nr, 1, "c", ": number of IOs.");
        opt.appendOpt(&offsetB, 0, "o", ": offset to write [block].");
        opt.appendParam(&bdevPath, "BDEV_PATH", ": block device path.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        if (bs == 0 || bs % 512 != 0) {
            throw cybozu::Exception("bad block size") << bs;
        }
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File file(opt.bdevPath, O_RDWR | O_DIRECT);
    cybozu::aio::Aio aio(file.fd(), 1);

    for (size_t i = 0; i < opt.nr; i++) {
        AlignedArray buf(opt.bs);

        const off_t off = opt.offsetB * opt.bs;
        const uint key = aio.prepareWrite(off, opt.bs, buf.data());
        if (key == 0) {
            throw cybozu::Exception("prepareWrite failed") << off << opt.bs;
        }
        aio.submit();
        size_t c = 0;
        while (!aio.isCompleted(key)) {
            ::memcpy(buf.data(), &c, sizeof(c));
            c++;
        }
        aio.waitFor(key);
        ::printf("modify %zu times\n", c);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("modify-during-write")
