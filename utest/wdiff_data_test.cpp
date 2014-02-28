#include "cybozu/test.hpp"
#include "meta.hpp"
#include "wdiff_data.hpp"
#include "file_path.hpp"
#include "for_test.hpp"

CYBOZU_TEST_AUTO(consolidate)
{
    cybozu::FilePath fp("test_wdiff_files_dir0");
    TestDirectory testDir(fp.str(), true);

    walb::WalbDiffFiles diffFiles(fp.str());
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

    {
        std::vector<walb::MetaDiff> v = diffFiles.getMgr().getMergeableDiffList(0);
        CYBOZU_TEST_EQUAL(v.size(), 2);
        CYBOZU_TEST_EQUAL(v[0], walb::MetaDiff(0, 1));
        CYBOZU_TEST_EQUAL(v[1], walb::MetaDiff(1, 2));
        walb::MetaDiff mdiff = walb::merge(v);
        CYBOZU_TEST_EQUAL(mdiff, walb::MetaDiff(0, 2));
        diffFiles.add(mdiff);
        createDiffFile(diffFiles, mdiff);
    }
    {
        std::vector<walb::MetaDiff> v = diffFiles.getMgr().getMergeableDiffList(2);
        CYBOZU_TEST_EQUAL(v.size(), 3);
        CYBOZU_TEST_EQUAL(v[0], walb::MetaDiff(2, 3));
        CYBOZU_TEST_EQUAL(v[1], walb::MetaDiff(3, 4));
        CYBOZU_TEST_EQUAL(v[2], walb::MetaDiff(4, 5));
        walb::MetaDiff mdiff = walb::merge(v);
        CYBOZU_TEST_EQUAL(mdiff, walb::MetaDiff(2, 5));
        diffFiles.add(mdiff);
        createDiffFile(diffFiles, mdiff);
    }
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 7);
    diffFiles.gc();
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 2);
    for (const std::string &s : diffFiles.listName()) {
        ::printf("%s\n", s.c_str());
    }
    diffFiles.removeBeforeGid(5);
    CYBOZU_TEST_EQUAL(diffFiles.listName().size(), 0);

    setDiff(diff, 5, 127, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    diffFiles.reloadMetadata();
    std::vector<walb::MetaDiff> diffV = diffFiles.listDiff();
    CYBOZU_TEST_EQUAL(diffV.size(), 1);
    CYBOZU_TEST_EQUAL(diffV[0].snapB.gidB, 5);
    CYBOZU_TEST_EQUAL(diffV[0].snapE.gidB, 127);

    diffFiles.clear();
    setDiff(diff, 10000, 10001, false); diffFiles.add(diff); createDiffFile(diffFiles, diff);
    CYBOZU_TEST_EQUAL(diffFiles.listDiff().size(), 1);
}

CYBOZU_TEST_AUTO(notCongiguous)
{
    cybozu::FilePath fp("test_wdiff_files_dir1");
    TestDirectory testDir(fp.str(), true);

    walb::WalbDiffFiles diffFiles(fp.str());
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

    {
        std::vector<walb::MetaDiff> v = diffFiles.getMgr().getMergeableDiffList(0);
        CYBOZU_TEST_EQUAL(v.size(), 1);
    }
    {
        std::vector<walb::MetaDiff> v = diffFiles.getMgr().getMergeableDiffList(2);
        CYBOZU_TEST_EQUAL(v.size(), 3);
    }
    {
        std::vector<walb::MetaDiff> v = diffFiles.getMgr().getMergeableDiffList(5);
        CYBOZU_TEST_EQUAL(v.size(), 2);
    }

    std::vector<walb::MetaDiff> v0 = diffFiles.getDiffListToSend(1);
    CYBOZU_TEST_EQUAL(v0.size(), 1);
    diffFiles.removeBeforeGid(1);
    std::vector<walb::MetaDiff> v1 = diffFiles.getDiffListToSend(1);
    CYBOZU_TEST_EQUAL(v1.size(), 3);
    diffFiles.removeBeforeGid(5);
    std::vector<walb::MetaDiff> v2 = diffFiles.getDiffListToSend(1);
    CYBOZU_TEST_EQUAL(v2.size(), 2);
    diffFiles.removeBeforeGid(9);
    std::vector<walb::MetaDiff> v3 = diffFiles.getDiffListToSend(1);
    CYBOZU_TEST_EQUAL(v3.size(), 0);
}
