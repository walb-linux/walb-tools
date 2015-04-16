#include "walb_log_base.hpp"
#include "cybozu/option.hpp"
#include "cybozu/exception.hpp"

using namespace walb;

struct Option
{
    std::string filePath, saltStr;
    uint32_t pbs, salt;
    bool isDebug;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("Show a dumped logpack header block.");
        opt.appendParam(&filePath, "FILE", ": dumped logpack header block file path.");
        opt.appendParam(&pbs, "PBS", ": physical block size [byte].");
        opt.appendParam(&saltStr, "SALT", ": salt. You can use 0x prefix for hex value.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages to stderr.");
        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
        util::parseDecOrHexInt(saltStr, salt);
        if (pbs == 0) throw cybozu::Exception("pbs must not be 0.");
    }
};

int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);

    cybozu::util::File file(opt.filePath, O_RDONLY);
    LogPackHeader packH(opt.pbs, opt.salt);
    packH.rawReadFrom(file);
    std::cout << packH << std::endl;
    if (!packH.isValid()) {
        std::cout << "invalid logpack header" << std::endl;
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wlog-show-raw")
