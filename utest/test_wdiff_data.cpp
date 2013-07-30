#include "cybozu/test.hpp"
#include "wdiff_data.hpp"
#include "file_path.hpp"
#include "for_test.hpp"

void setDiff(walb::MetaDiff &diff, uint64_t gid0, uint64_t gid1, bool canMerge)
{
    diff.init();
    diff.raw().gid0 = gid0;
    diff.raw().gid1 = gid1;
    diff.raw().gid2 = gid1;
    diff.raw().can_merge = canMerge;
};

void createFile(walb::WalbDiffFiles &diffFiles, walb::MetaDiff &diff)
{
    cybozu::FilePath fp = diffFiles.dirPath() + cybozu::FilePath(diffFiles.createDiffFileName(diff));
    cybozu::util::createEmptyFile(fp.str());
}

CYBOZU_TEST_AUTO(merge)
{
    cybozu::FilePath fp("test_wdiff_files_dir0");
    TestDirectory testDir(fp.str());

    walb::WalbDiffFiles diffFiles(fp.str(), true);
    walb::MetaDiff diff;
    setDiff(diff, 0, 1, false); diffFiles.add(diff); createFile(diffFiles, diff);
    setDiff(diff, 1, 2, true);  diffFiles.add(diff); createFile(diffFiles, diff);
    setDiff(diff, 2, 3, false); diffFiles.add(diff); createFile(diffFiles, diff);
    setDiff(diff, 3, 4, true);  diffFiles.add(diff); createFile(diffFiles, diff);
    setDiff(diff, 4, 5, true);  diffFiles.add(diff); createFile(diffFiles, diff);
    for (const std::string &s : diffFiles.listName()) {
        ::printf("%s\n", s.c_str());
    }
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 5);

    std::vector<std::pair<uint64_t, uint64_t> > v = diffFiles.getMergeCandidates();
    CYBOZU_TEST_EQUAL(v.size(), 2);
    ::printf("%" PRIu64 " %" PRIu64 "\n", v[0].first, v[0].second);

    CYBOZU_TEST_EQUAL(v[0].first, 0);
    CYBOZU_TEST_EQUAL(v[0].second, 2);
    CYBOZU_TEST_EQUAL(v[1].first, 2);
    CYBOZU_TEST_EQUAL(v[1].second, 5);

    setDiff(diff, 0, 2, false);
    createFile(diffFiles, diff);
    CYBOZU_TEST_ASSERT(diffFiles.merge(0, 2));

    setDiff(diff, 2, 5, false);
    createFile(diffFiles, diff);
    CYBOZU_TEST_ASSERT(diffFiles.merge(2, 5));

    for (const std::string &s : diffFiles.listName()) {
        ::printf("%s\n", s.c_str());
    }
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 2);
    diffFiles.removeBeforeGid(5);

    setDiff(diff, 5, 127, false); diffFiles.add(diff); createFile(diffFiles, diff);
    diffFiles.reloadMetadata();
    CYBOZU_TEST_EQUAL(diffFiles.latestGid(), 127);
    std::vector<walb::MetaDiff> diffV = diffFiles.listDiff();
    CYBOZU_TEST_EQUAL(diffV.size(), 1);
    CYBOZU_TEST_EQUAL(diffV[0].gid0(), 5);
    CYBOZU_TEST_EQUAL(diffV[0].gid1(), 127);

    diffFiles.reset(10000);
    setDiff(diff, 10000, 10001, false); diffFiles.add(diff); createFile(diffFiles, diff);
    CYBOZU_TEST_EQUAL(diffFiles.latestGid(), 10001);
}
