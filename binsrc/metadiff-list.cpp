/**
 * @file
 * @brief metadiff list.
 */
#include "cybozu/option.hpp"
#include "walb_logger.hpp"
#include "util.hpp"
#include "meta.hpp"
#include "easy_signal.hpp"

using namespace walb;


struct Option
{
    bool isDebug;
    std::string logPath;

    Option(int argc, char* argv[]) {
        cybozu::Option opt;
        opt.setDescription("metadiff-list: do something on metadiff list.");
        opt.appendBoolOpt(&isDebug, "debug", ": debug print to stderr.");
        opt.appendOpt(&logPath, "-", "l", ": log output path. (default: stderr)");

        opt.appendHelp("h", ": show this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};



int doMain(int argc, char* argv[])
{
    Option opt(argc, argv);
    util::setLogSetting(opt.logPath, opt.isDebug);
    cybozu::signal::setSignalHandler({SIGINT, SIGQUIT, SIGTERM});


    MetaDiffManager mgr;

    size_t c = 0;
    std::string line;
    while (std::getline(std::cin, line)) {
        MetaDiff d = parseDiffFileName(cybozu::util::trimSpace(line));
        mgr.add(d);

        // do something.
        // std::cout << d << std::endl;

        if (c % 100000 == 0) {
            ::printf("c %zu\n", c);
        }
        c++;
    }

    // do something.

    return 0;
}

DEFINE_ERROR_SAFE_MAIN("metadiff-list")
