#include "walb_util.hpp"
#include "cybozu/option.hpp"
#include "bdev_util.hpp"

struct Option
{
    uint64_t offset;
    uint64_t size;
    std::string devPath;

    Option(int argc, char *argv[]) {
        cybozu::Option opt;
        opt.setDescription("trim: issue discard requests to a block device.");
        opt.appendOpt(&offset, 0, "o", ": offset in bytes (default: 0)");
        opt.appendOpt(&size, UINT64_MAX, "s", ": size in bytes (default: device size)");
        opt.appendParam(&devPath, "DEVICE_PATH", ": block device path");
        opt.appendHelp("h", ": put this message.");
        if (!opt.parse(argc, argv)) {
            opt.usage();
            ::exit(1);
        }
    }
};

int doMain(int argc, char *argv[])
{
    Option opt(argc, argv);
    cybozu::util::File file(opt.devPath, O_RDWR);
    const uint64_t offsetLb = opt.offset / LOGICAL_BLOCK_SIZE;
    const uint64_t devSize = cybozu::util::getBlockDeviceSize(file.fd());
    const uint64_t sizeLb = std::min(opt.size, devSize) / LOGICAL_BLOCK_SIZE;
    cybozu::util::issueDiscard(file.fd(), offsetLb, sizeLb);
    return 0;
}

DEFINE_ERROR_SAFE_MAIN("trim")
