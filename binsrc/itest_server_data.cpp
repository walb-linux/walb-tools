/**
 * @file
 * @brief Test server data. You need to prepare a lvm volume group.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <stdexcept>
#include "cybozu/option.hpp"
#include "cybozu/itoa.hpp"
#include "server_data.hpp"
#include "lvm.hpp"
#include "file_path.hpp"
#include "util.hpp"

struct Option : public cybozu::Option
{
    std::string vgName;
    Option() {
        appendParam(&vgName, "[volume group name]", "volume group name for test.");
        appendHelp("h");
    }
};

/**
 * Create a temporal directory with RAII style.
 */
class TmpDir
{
private:
    cybozu::FilePath path_;
public:
    TmpDir(const std::string &prefix) : path_() {
        for (uint8_t i = 0; i < 100; i++) {
            path_ = makePath(prefix, i);
            if (!path_.stat().exists()) break;
        }
        if (!path_.mkdir()) throw RT_ERR("mkdir failed.");
    }
    ~TmpDir() noexcept {
        try {
            path_.rmdirRecursive();
        } catch (...) {
        }
    }
    cybozu::FilePath path() const {
        return path_;
    }
private:
    static cybozu::FilePath makePath(const std::string &prefix, uint8_t i) {
        std::string pathStr = cybozu::util::formatString("%s%02u", prefix.c_str(), i);
        return cybozu::FilePath(pathStr);
    }
};

void testServerData(const Option &opt)
{
    TmpDir tmpDir("tmpdir");
    cybozu::FilePath dir = tmpDir.path();

    /* create server data. */
    walb::ServerData sd(tmpDir.path().str(), "0", opt.vgName);

    /* create lv. */
    uint64_t lvSizeLb = (64ULL << 20) >> 9;
    assert(!sd.lvExists());
    sd.createLv(lvSizeLb);
    assert(sd.lvExists());
    sd.getLv().print();

    /* grow lv. */
    sd.growLv(lvSizeLb * 2);
    sd.getLv().print();

    /* snapshot lv. */
    auto m = sd.getRestores();
    assert(m.empty());

    sd.reset(100);

#if 0
    uint64_t gid;
    bool ret;

    assert(sd.canRestore(0));
    gid = 0;
    assert(sd.getRestores().empty());
    ret = sd.restore(gid);
    assert(ret);
    assert(!sd.getRestores().empty());
    assert(sd.getRestores()[0].snapName() == "r_0_" + cybozu::itoa(gid));
    sd.drop(gid);

    MetaDiff diff;
    sd.add(diff);

    /* apply */
    std::vector<MetaDiff> diffV;
    gid = 0;
    diffV = diffsToApply(gid);
    diff = walb::consolidate(diffV);
    startToApply(diff);
    finishToApply(diff);

    /* consolidate */
    uint64_t ts0 = 0, ts1 = -1;
    std::vecotr<MetaDiff> v = sd.candidatesToConsolidate(ts0, ts1);
    diff = walb::consolidate(v);
    sd.finishToConsolidate(diff);

#endif

    /* now editing */
}

int main(int argc, char *argv[])
{
    try {
        Option opt;
        if (!opt.parse(argc, argv)) {
            opt.usage();
            return 1;
        }
        testServerData(opt);
        return 0;
    } catch (std::runtime_error &e) {
        ::printf("runtime error: %s\n", e.what());
        return 1;
    } catch (...) {
        ::printf("other error caught.\n");
        return 1;
    }
}
