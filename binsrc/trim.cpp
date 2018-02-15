#include "walb_util.hpp"
#include "cybozu/option.hpp"
#include "bdev_util.hpp"
#include "cybozu/exception.hpp"

struct Option
{
    uint64_t offset;
    uint64_t total;
    uint64_t unit;
    std::string devPath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setDescription("trim: issue discard requests to a block device.");
        opt.appendOpt(&offset, 0, "o", ": offset in bytes (default: 0)");
        opt.appendOpt(&total, UINT64_MAX, "t", ": total size to discard in bytes (default: device size - offset)");
        opt.appendOpt(&unit, UINT64_MAX, "u", ": unit size in bytes (default: total size)");
        opt.appendParam(&devPath, "DEVICE_PATH", ": block device path");
        opt.appendHelp("h", ": put this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};


const uint64_t PRINT_PROGRESS_INTERVAL_LB = (1U << 30) / LOGICAL_BLOCK_SIZE; // lb


int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    cybozu::util::File file(opt.devPath, O_RDWR);
    const uint64_t devSize = cybozu::util::getBlockDeviceSize(file.fd());
    if (opt.offset > devSize) {
        throw cybozu::Exception("trim:offset > devSize") << opt.offset << devSize;
    }
    uint64_t offsetLb = opt.offset / LOGICAL_BLOCK_SIZE;
    const uint64_t totalSize = opt.total == UINT64_MAX ? (devSize - opt.offset) : opt.total;
    if (totalSize > devSize - opt.offset) {
        throw cybozu::Exception("trim:total > devSize - offset")
            << totalSize  << devSize - opt.offset;
    }
    uint64_t totalLb = totalSize / LOGICAL_BLOCK_SIZE;
    uint64_t unitSize = opt.unit == UINT64_MAX ? totalSize : opt.unit;
    if (unitSize > totalSize) {
        throw cybozu::Exception("trim:unit > totalSize")
            << unitSize << totalSize;
    }
    const uint64_t unitLb = unitSize / LOGICAL_BLOCK_SIZE;
    uint64_t printLb = 0;
    while (totalLb > 0) {
        uint64_t lb = std::min(unitLb, totalLb);
        cybozu::util::issueDiscard(file.fd(), offsetLb, lb);
        offsetLb += lb;
        printLb += lb;
        totalLb -= lb;
        if (printLb >= PRINT_PROGRESS_INTERVAL_LB) {
            ::printf(".");
            ::fflush(::stdout);
            printLb = 0;
        }
    }
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("trim")
