/*
 * Walb diff file name generator.
 */
#include "walb_util.hpp"
#include "meta.hpp"
#include "cybozu/option.hpp"

using namespace walb;

struct Option
{
    bool isDebug, isNotMergeable, isCompDiff;
    std::string dateTimeStr;
    std::vector<uint64_t> lsidV;
    MetaDiff diff;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setDescription("Generate walb diff name\n");
        opt.appendOpt(&dateTimeStr, "", "d", ": datetime string (YYYYMMDDhhmmss)");
        opt.appendBoolOpt(&isNotMergeable, "nomerge", ": do not set mergeable flag.");
        opt.appendBoolOpt(&isCompDiff, "comp", ": set compDiff flag.");
        opt.appendBoolOpt(&isDebug, "debug", ": put debug messages.");
        opt.appendParamVec(&lsidV, "LSID_LIST", ": specify two or four lsids");
        opt.appendHelp("h", ": put this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }

        // verify lsidV.
        const size_t nrLsids = lsidV.size();
        if (nrLsids != 2 && nrLsids != 4) {
            throw cybozu::Exception(__func__)
                << "specify two or four lsids" << nrLsids;
        }

        // Set to meta diff.
        if (nrLsids == 2) {
            diff.snapB.set(lsidV[0]);
            diff.snapE.set(lsidV[1]);
        } else {
            diff.snapB.set(lsidV[0], lsidV[1]);
            diff.snapE.set(lsidV[2], lsidV[3]);
        }
        // verify and set timestamp.
        if (dateTimeStr.empty()) {
            diff.timestamp = ::time(0);
        } else {
            diff.timestamp = cybozu::strToUnixTime(dateTimeStr);
        }
        diff.isMergeable = !isNotMergeable;
        diff.isCompDiff = isCompDiff;
    }
};

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    util::setLogSetting("-", opt.isDebug);
    const std::string name = createDiffFileName(opt.diff);
    ::printf("%s\n", name.c_str());
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("wdiff-name-gen")
