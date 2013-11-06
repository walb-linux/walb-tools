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
    uint64_t gid0, uint64_t gid1, bool canMerge = true)
{
    cybozu::TmpFile tmpFile(dirPath.str());

    /* Generate meta diff. */
    walb::MetaDiff diff(gid0, gid1);
    diff.setTimestamp(::time(0));
    diff.setCanMerge(canMerge);
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

/**
 * Create wdiff files in a server data space.
 * @sd server data.
 * @gid0 reference. you must set this value before calling the function.
 *   after returned, the gid0 indicates the latest gid.
 * @logMb log size that will be converted to a diff [MiB]
 * @nDiffs number of diff files.
 */
void createWdiffFiles(walb::ServerData &sd, uint64_t &gid0, size_t logMb, size_t nDiffs)
{
    const uint64_t lvSizeLb = sd.getLv().sizeLb();
    cybozu::util::Random<uint32_t> rand(1, 100);
    uint64_t gid1 = gid0 + rand();
    for (size_t i = 0; i < nDiffs; i++) {
        //::printf("create wdiff start %zu\n", i); /* debug */
        //::printf("directory: %s\n", sd.getDiffDir().str().c_str()); /* debug */
        bool canMerge = true;
        if (rand() % 3 == 0) canMerge = false;
        walb::MetaDiff diff = createWdiffFile(sd.getDiffDir(), lvSizeLb, logMb, gid0, gid1, canMerge);
        ::printf("create wdiff end %zu\n", i); /* debug */
        diff.print(); /* debug */
        sd.add(diff);
        gid0 = gid1;
        gid1 = gid0 + rand();
    }
}

std::pair<std::shared_ptr<char>, size_t> reallocateBlocksIfNeed(
    std::shared_ptr<char> blk, size_t currSize, size_t newSize)
{
    assert(currSize % LOGICAL_BLOCK_SIZE == 0);
    assert(newSize % LOGICAL_BLOCK_SIZE == 0);
    if (blk && newSize <= currSize) return {blk, currSize};
    return {cybozu::util::allocateBlocks<char>(LOGICAL_BLOCK_SIZE, newSize), newSize};
}

std::vector<walb::MetaDiff> mergeDiffs(
    const std::vector<walb::MetaDiff> &v, bool ignoreCanMergeFlag)
{
    std::vector<walb::MetaDiff> ret;
    if (v.empty()) return ret;
    std::vector<walb::MetaDiff>::const_iterator it = v.cbegin();
    walb::MetaDiff diff = *it;
    diff.setTimestamp(it->timestamp());
    ++it;
    while (it != v.cend()) {
        if (walb::canMerge(diff, *it, ignoreCanMergeFlag)) {
            diff = walb::merge(diff, *it, ignoreCanMergeFlag);
        } else {
            ret.push_back(diff);
            diff = *it;
        }
        ++it;
    }
    ret.push_back(diff);
    return ret;
}

walb::MetaSnap applyDiffs(const walb::MetaSnap &snap, const std::vector<walb::MetaDiff> &diffV)
{
    walb::MetaSnap snap0 = snap;
    for (const walb::MetaDiff &diff : diffV) {
        if (!walb::canApply(snap0, diff)) {
            throw std::runtime_error("can not apply diff");
        }
        snap0 = walb::apply(snap0, diff);
    }
    return snap0;
}

void setupMerger(walb::diff::Merger &merger, const cybozu::FilePath &dirPath,
                 const std::vector<walb::MetaDiff> &diffV)
{
    std::vector<std::string> fpathV;
    for (const walb::MetaDiff &diff : diffV) {
        cybozu::FilePath fpath = dirPath + cybozu::FilePath(walb::createDiffFileName(diff));
        fpathV.push_back(fpath.str());
    }
    merger.addWdiffs(fpathV);
    merger.setMaxIoBlocks(-1);
    merger.setShouldValidateUuid(false);
    merger.prepare();
}

/**
 * @sd server data.
 * @gid target gid.
 * @isRestore.
 */
void applyDiffsToLv(walb::ServerData &sd, uint64_t gid, bool isRestore)
{
    std::vector<walb::MetaDiff> diffV = sd.diffsToApply(gid);
    if (diffV.empty()) return;
    walb::MetaSnap appliedSnap = applyDiffs(sd.getOldestSnapshot(), diffV);
    if (appliedSnap.gid0() != gid) {
        throw RT_ERR("gid is invalid: %" PRIu64 " %" PRIu64 "", gid, appliedSnap.gid0());
    }

    cybozu::FilePath lvPath;
    if (isRestore) {
        /* You must create the restore snapshot before. */
        lvPath = sd.getRestores()[gid].path();
    } else {
        lvPath = sd.getLv().path();
    }

    walb::diff::Merger merger;
    setupMerger(merger, sd.getDiffDir(), diffV);

    cybozu::util::BlockDevice bd0(lvPath.str(), O_RDWR | O_DIRECT);
    std::vector<walb::MetaDiff> mDiffV = mergeDiffs(diffV, true);
    if (mDiffV.size() != 1) throw std::runtime_error("bug."); /* debug */
    walb::MetaDiff mDiff = mDiffV[0];

    sd.getOldestSnapshot().print();
    sd.getLatestSnapshot().print();
    for (walb::MetaDiff &diff : diffV) diff.print();

    if (!isRestore) sd.startToApply(mDiff);
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
        bd0.write(oft, s, blk.get());
    }
    if (!isRestore) {
        sd.finishToApply(mDiff);
        sd.removeOldDiffs();
    }
    ::printf("apply all wdiffs.\n"); /* debug */
    sd.getOldestSnapshot().print();
    sd.getLatestSnapshot().print();

    bd0.close();
}

bool consolidateWdiffs(walb::ServerData &sd)
{
    /* Get candidates diffs to merge. */
    std::vector<walb::MetaDiff> cDiffV = sd.candidatesToConsolidate(0, uint64_t(-1));
    if (cDiffV.empty()) return false;

#if 0
    ::printf("cDiffV:\n");
    for (walb::MetaDiff &diff : cDiffV) {
        diff.print();
    }
    ::printf("cDiffV done\n");
#endif

    /* Merge into temporary file. */
    walb::diff::Merger merger;
    setupMerger(merger, sd.getDiffDir(), cDiffV);
    cybozu::TmpFile tmpFile(sd.getDiffDir().str());
    merger.mergeToFd(tmpFile.fd());

    /* Rename the merged diff file. */
    std::vector<walb::MetaDiff> mergedV = mergeDiffs(cDiffV, false);
    if (mergedV.size() != 1) throw std::runtime_error("merged size must be 1.");
    walb::MetaDiff mDiff = mergedV[0];
    cybozu::FilePath mDiffPath =
        sd.getDiffDir() + cybozu::FilePath(walb::createDiffFileName(mDiff));
    tmpFile.save(mDiffPath.str());

    /* Finish consolidation */
    sd.finishToConsolidate(mDiff);
    return true;
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
    uint64_t gid0;
    gid0 = sd.getLatestCleanSnapshot();
    ::printf("gid0 %" PRIu64 "\n", gid0);
    const size_t logMb = 10;
    const size_t nDiffs = 15;
    createWdiffFiles(sd, gid0, logMb, nDiffs);
    ::printf("create wdiffs done\n"); /* debug */
    sd.getLv().print(); /* debug */

    /* Apply all wdiffs. */
    applyDiffsToLv(sd, gid0, false);

    /*
     * restore
     */

    /* Create more wdiff files. */
    createWdiffFiles(sd, gid0, logMb, nDiffs);

#if 0
    /* Create a snapshot as a restore volume. */
    uint64_t rGid = sd.getLatestCleanSnapshot();
    if (!sd.canRestore(rGid)) throw std::runtime_error("can not restore.");
    if (!sd.restore(rGid)) throw std::runtime_error("restore volume creation failed.");
    std::map<uint64_t, cybozu::lvm::Lv> rMap = sd.getRestores();
    assert(!rMap.empty());
    for (auto &p : rMap) p.second.print();

    /* Apply diffs to the restored lv. */
    applyDiffsToLv(sd, rGid, true);

    /* Drop restore volume. */
    {
        bool ret = sd.drop(rGid);
        if (!ret) throw std::runtime_error("drop restore volume failed.");
    }
#endif

    /*
     * consolidate
     */
    sd.print();
    while (consolidateWdiffs(sd)) {
        ::printf("consolidate.\n");
        sd.print();
    }

    /* remove lv. */
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
    } catch (std::exception &e) {
        ::fprintf(::stderr, "exception: %s\n", e.what());
    } catch (...) {
        ::fprintf(::stderr, "other error caught.\n");
    }
    return 1;
}
