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
    bool isHex;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Calculate checksum of IO data.\n");
        opt.appendOpt(&saltStr, "", "salt", ": checksum salt. prefix 0x means hex.");
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

    std::vector<char> buf;
    readAllFromFile(file, buf);
    const uint32_t csum = cybozu::util::calcChecksum(buf.data(), buf.size(), opt.salt);

    if (opt.isHex) {
        ::printf("%08x\n", csum);
    } else {
        ::printf("%u\n", csum);
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("checksum")
