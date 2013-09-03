/**
 * @file
 * @brief Test server data. You need to prepare a lvm volume group.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <vector>
#include <chrono>
#include <thread>
#include <stdexcept>
#include "cybozu/option.hpp"
#include "cybozu/itoa.hpp"
#include "server_data.hpp"
#include "lvm.hpp"
#include "file_path.hpp"
#include "util.hpp"
#include "walb_diff_gen.hpp"
#include "meta.hpp"
#include "file_path.hpp"
#include "tmp_file.hpp"
#include "random.hpp"
#include "memory_buffer.hpp"
#include "walb_diff_merge.hpp"

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

/**
 * Generate config data for walb log generation.
 */
walb::log::Generator::Config generateConfig(uint64_t lvSizeLb, unsigned int outLogMb)
{
    walb::log::Generator::Config cfg;
    cfg.devLb = lvSizeLb;
    cfg.minIoLb = 512 >> 9;
    cfg.maxIoLb = 262144 >> 9;
    cfg.pbs = 512;
    cfg.maxPackPb = (1 << 20) >> 9; // 1MiB.
    cfg.outLogPb = (outLogMb << 20) / cfg.pbs;
    cfg.lsid = 0;
    cfg.isPadding = true;
    cfg.isDiscard = true;
    cfg.isAllZero = true;
    cfg.isVerbose = false;

    cfg.check();
    return cfg;
}

/**
 * RETURN:
 *   Metadata of the generated diff.
 */
walb::MetaDiff createWdiffFile(
    const cybozu::FilePath &dirPath, uint64_t lvSizeLb, unsigned int outLogMb,
    uint64_t gid0, uint64_t gid1)
{
    cybozu::TmpFile tmpFile(dirPath.str());

    /* Generate meta diff. */
    walb::MetaDiff diff(gid0, gid1);
    diff.raw().timestamp = ::time(0);
    diff.raw().can_merge = 1;
    std::string fName = walb::createDiffFileName(diff);
    cybozu::FilePath fPath = dirPath + cybozu::FilePath(fName);

    /* Create a random wdiff file */
    walb::log::Generator::Config cfg = generateConfig(lvSizeLb, outLogMb);
    walb::diff::Generator g(cfg);
    g.generate();
    g.data().writeTo(tmpFile.fd());

    /* Rename */
    tmpFile.save(fPath.str());

    return diff;
}

std::pair<std::shared_ptr<char>, size_t> reallocateBlocksIfNeed(
    std::shared_ptr<char> blk, size_t currSize, size_t newSize)
{
    assert(currSize % LOGICAL_BLOCK_SIZE == 0);
    assert(newSize % LOGICAL_BLOCK_SIZE == 0);
    if (blk && newSize <= currSize) return std::make_pair(blk, currSize);
    return std::make_pair(
        cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, newSize), newSize);
}

std::vector<walb::MetaDiff> mergeDiffs(const std::vector<walb::MetaDiff> &v)
{
    std::vector<walb::MetaDiff> ret;
    if (v.empty()) return ret;
    std::vector<walb::MetaDiff>::const_iterator it = v.cbegin();
    walb::MetaDiff diff = *it;
    diff.raw().timestamp = it->raw().timestamp;
    ++it;
    while (it != v.cend()) {
        if (diff.canMerge(*it)) {
            diff = diff.merge(*it);
            diff.raw().timestamp = it->raw().timestamp;
        } else {
            ret.push_back(diff);
            diff = *it;
            diff.raw().timestamp = it->raw().timestamp;
        }
        ++it;
    }
    ret.push_back(diff);
    return ret;
}

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
    ::printf("exists: %d\n", cybozu::lvm::exists("vg", "i_0")); /* debug */
    assert(sd.lvExists());
    ::printf("----------\n");
    sd.getLv().print();
    ::printf("----------\n");

    /* grow lv. */
    lvSizeLb *= 2;
    sd.growLv(lvSizeLb);
    sd.getLv().print();
    ::printf("----------\n");

    /* snapshot lv. */
    auto m = sd.getRestores();
    assert(m.empty());
    sd.getLv().print(); /* debug */

    /* add wdiffs. */
    uint64_t gid0, gid1;
    cybozu::util::Random<uint32_t> rand(1, 100);
    gid0 = sd.getLatestCleanSnapshot();
    gid1 = gid0 + rand();
    ::printf("gid (%" PRIu64 ", %" PRIu64")\n", gid0, gid1);

    const size_t nDiffs = 10;
    for (size_t i = 0; i < nDiffs; i++) {
        ::printf("create wdiff start %zu\n", i); /* debug */
        ::printf("directory: %s\n", sd.getDiffDir().str().c_str()); /* debug */
        walb::MetaDiff diff = createWdiffFile(sd.getDiffDir(), lvSizeLb, 10, gid0, gid1);
        ::printf("create wdiff end %zu\n", i); /* debug */
        diff.print(); /* debug */
        sd.add(diff);
        gid0 = gid1;
        gid1 = gid0 + rand();
    }
    ::printf("create wdiffs done\n"); /* debug */
    sd.getLv().print(); /* debug */

    /* Apply all wdiffs. */
    std::vector<walb::MetaDiff> diffV = sd.diffsToApply(gid1);
    std::vector<std::string> fpathV;
    for (const walb::MetaDiff &diff : diffV) {
        cybozu::FilePath fpath = sd.getDiffDir() + cybozu::FilePath(walb::createDiffFileName(diff));
        fpathV.push_back(fpath.str());
    }
    walb::diff::Merger merger;
    merger.addWdiffs(fpathV);
    merger.setMaxIoBlocks(-1);
    merger.setShouldValidateUuid(false);
    merger.prepare();

    cybozu::util::BlockDevice bd(sd.getLv().path().str(), O_RDWR | O_DIRECT);
    std::vector<walb::MetaDiff> mDiffV = mergeDiffs(diffV);
    if (mDiffV.size() != 1) throw std::runtime_error("bug.");

    sd.getOldestSnapshot().print();
    sd.getLatestSnapshot().print();
    for (walb::MetaDiff &diff : diffV) diff.print();

    sd.startToApply(mDiffV[0]);
    std::shared_ptr<char> blk;
    size_t blkSize = (1 << 20); /* 1 MiB */
    std::tie(blk, blkSize) = reallocateBlocksIfNeed(blk, 0, blkSize);
    walb::diff::RecIo d;
    while (merger.pop(d)) {
        /* Apply an IO. */
        if (!d.isValid()) throw std::runtime_error("invalid RecIo.");
        off_t oft = d.record().ioAddress() * LOGICAL_BLOCK_SIZE;
        size_t s = d.io().rawSize();
        std::tie(blk, blkSize) = reallocateBlocksIfNeed(blk, blkSize, s);
        ::memcpy(blk.get(), d.io().rawData(), s);
        bd.write(oft, s, blk.get());
    }
    sd.finishToApply(mDiffV[0]);
    sd.removeOldDiffs();
    ::printf("apply all wdiffs.\n"); /* debug */

    sd.getOldestSnapshot().print();
    sd.getLatestSnapshot().print();


    /*
     * restore
     */
    for (size_t i = 0; i < nDiffs; i++) {
        walb::MetaDiff diff = createWdiffFile(sd.getDiffDir(), lvSizeLb, 10, gid0, gid1);
        diff.print(); /* debug */
        sd.add(diff);
        gid0 = gid1;
        gid1 = gid0 + rand();
    }
    if (!sd.restore()) throw std::runtime_error("restore volume creation failed.");
    std::map<uint64_t, cybozu::lvm::Lv> rMap = sd.getRestores();
    assert(!rMap.empty());
    for (auto &p : rMap) p.second.print();

    /* apply diffs to the restored lv. */

    /* now editing */


    /*
     * consolidate
     */

    /* now editing */


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

    /* remove lv. */
    bd.close();
    sd.getLv().remove();

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
    } catch (std::exception &e) {
        ::printf("exception: %s\n", e.what());
    } catch (...) {
        ::printf("other error caught.\n");
    }
    return 1;
}
