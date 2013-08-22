#include "cybozu/test.hpp"
#include "meta.hpp"
#include "wdiff_data.hpp"
#include "file_path.hpp"
#include "for_test.hpp"

CYBOZU_TEST_AUTO(consolidate)
{
    cybozu::FilePath fp("test_wdiff_files_dir0");
    TestDirectory testDir(fp.str(), true);

    walb::WalbDiffFiles diffFiles(fp.str(), true, true);
    walb::MetaDiff diff;
    setDiff(diff, 0, 1, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 1, 2, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 2, 3, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 3, 4, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 4, 5, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    for (const std::string &s : diffFiles.listName()) {
        ::printf("%s\n", s.c_str());
    }
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 5);

    std::vector<std::pair<uint64_t, uint64_t> > v
        = diffFiles.getMergingCandidates();
    CYBOZU_TEST_EQUAL(v.size(), 2);
    ::printf("%" PRIu64 " %" PRIu64 "\n", v[0].first, v[0].second);
    CYBOZU_TEST_EQUAL(v[0].first, 0);
    CYBOZU_TEST_EQUAL(v[0].second, 2);
    CYBOZU_TEST_EQUAL(v[1].first, 2);
    CYBOZU_TEST_EQUAL(v[1].second, 5);

    setDiff(diff, 0, 2, false);
    createDiffFile(diffFiles, diff);
    CYBOZU_TEST_ASSERT(diffFiles.consolidate(0, 2));

    setDiff(diff, 2, 5, false);
    createDiffFile(diffFiles, diff);
    CYBOZU_TEST_ASSERT(diffFiles.consolidate(2, 5));

    for (const std::string &s : diffFiles.listName()) {
        ::printf("%s\n", s.c_str());
    }
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 2);
    diffFiles.cleanup();
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 2);
    diffFiles.removeBeforeGid(5);

    setDiff(diff, 5, 127, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    diffFiles.reloadMetadata();
    CYBOZU_TEST_EQUAL(diffFiles.latestGid(), 127);
    std::vector<walb::MetaDiff> diffV = diffFiles.listDiff();
    CYBOZU_TEST_EQUAL(diffV.size(), 1);
    CYBOZU_TEST_EQUAL(diffV[0].gid0(), 5);
    CYBOZU_TEST_EQUAL(diffV[0].gid1(), 127);

    diffFiles.reset(10000);
    setDiff(diff, 10000, 10001, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    CYBOZU_TEST_EQUAL(diffFiles.latestGid(), 10001);
}

CYBOZU_TEST_AUTO(notCongiguous)
{
    cybozu::FilePath fp("test_wdiff_files_dir1");
    TestDirectory testDir(fp.str(), true);

    walb::WalbDiffFiles diffFiles(fp.str(), false, true);
    walb::MetaDiff diff;
    setDiff(diff, 0, 1, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 2, 3, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 3, 4, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 4, 5, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 7, 8, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    setDiff(diff, 8, 9, true);  diffFiles.add(diff); createDiffFile(diffFiles, diff);
    for (const std::string &s : diffFiles.listName()) {
        ::printf("%s\n", s.c_str());
    }
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 6);

    std::vector<std::pair<uint64_t, uint64_t> > v
        = diffFiles.getMergingCandidates();
    CYBOZU_TEST_EQUAL(v.size(), 3);
    ::printf("%" PRIu64 " %" PRIu64 "\n", v[0].first, v[0].second);
    CYBOZU_TEST_EQUAL(v[0].first, 0);
    CYBOZU_TEST_EQUAL(v[0].second, 1);
    CYBOZU_TEST_EQUAL(v[1].first, 2);
    CYBOZU_TEST_EQUAL(v[1].second, 5);
    CYBOZU_TEST_EQUAL(v[2].first, 7);
    CYBOZU_TEST_EQUAL(v[2].second, 9);

    diffFiles.latestSnap();
    CYBOZU_TEST_EQUAL(diffFiles.oldestGid(), 0);
    CYBOZU_TEST_EQUAL(diffFiles.latestGid(), 9);

    std::vector<walb::MetaDiff> v0 = diffFiles.getTransferCandidates(1);
    CYBOZU_TEST_EQUAL(v0.size(), 1);
    diffFiles.removeBeforeGid(1);
    std::vector<walb::MetaDiff> v1 = diffFiles.getTransferCandidates(1);
    CYBOZU_TEST_EQUAL(v1.size(), 3);
    diffFiles.removeBeforeGid(5);
    std::vector<walb::MetaDiff> v2 = diffFiles.getTransferCandidates(1);
    CYBOZU_TEST_EQUAL(v2.size(), 2);
    diffFiles.removeBeforeGid(9);
    std::vector<walb::MetaDiff> v3 = diffFiles.getTransferCandidates(1);
    CYBOZU_TEST_EQUAL(v3.size(), 0);
}

