/*
 * Calc checksum of data.
 */
#include <cstdio>
#include "cybozu/option.hpp"
#include "checksum.hpp"
#include "walb_util.hpp"

using namespace walb;

struct Option
{
    std::string saltStr;
    std::string ioDataPath;
    uint32_t salt;
    size_t bulkSize;
    bool isHex;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Calculate checksum of IO data.\n");
        opt.appendOpt(&saltStr, "", "salt", ": checksum salt. prefix 0x means hex.");
        opt.appendOpt(&bulkSize, 0, "bulk", ": put checksum for each bulk with size [bytes]. (default: whole file)");
        opt.appendBoolOpt(&isHex, "hex", ": put checksum in hex.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendParamOpt(&ioDataPath, "-", "IO_DATA_FILE", ": IO data dump file.");
        opt.appendHelp("h", ": show this message.");

        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        util::parseDecOrHexInt(saltStr, salt);
        LOGs.debug() << "salt" << saltStr << salt
                     << cybozu::util::intToHexStr(salt);
    }
};

void calcCsumAndPut(const char *data, size_t size, const Option& opt, uint64_t bulkId = size_t(-1))
{
    const uint32_t csum = cybozu::util::calcChecksum(data, size, opt.salt);
    if (bulkId != uint64_t(-1)) ::printf("%" PRIu64 "\t", bulkId);
    if (opt.isHex) {
        ::printf("%08x\n", csum);
    } else {
        ::printf("%u\n", csum);
    }
}

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File file;
    if (opt.ioDataPath == "-") {
        file.setFd(0);
    } else {
        file.open(opt.ioDataPath, O_RDONLY);
    }

    if (opt.bulkSize == 0) {
        std::vector<char> buf;
        cybozu::util::readAllFromFile(file, buf);
        calcCsumAndPut(buf.data(), buf.size(), opt);
    } else {
        std::vector<char> buf(opt.bulkSize);
        size_t off = 0;
        size_t bulkId = 0;
        for (;;) {
            const size_t r = file.readsome(&buf[off], buf.size() - off);
            if (r == 0) {
                if (off > 0) calcCsumAndPut(buf.data(), off, opt, bulkId);
                break;
            }
            off += r;
            if (off == buf.size()) {
                calcCsumAndPut(buf.data(), buf.size(), opt, bulkId);
                off = 0;
                bulkId++;
            }
        }
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("checksum")
