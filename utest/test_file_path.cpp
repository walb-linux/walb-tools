#include <cstdio>
#include "cybozu/test.hpp"
#include "file_path.hpp"
#include "fileio.hpp"

void checkRedundancy(const std::string &srcPath, const std::string &dstPath)
{
    std::string srcPath0 = cybozu::FilePath(srcPath).removeRedundancy().str();
    CYBOZU_TEST_EQUAL(srcPath0, dstPath);
}

CYBOZU_TEST_AUTO(removeRedundancy)
{
    checkRedundancy("/", "/");
    checkRedundancy("/a/b/c", "/a/b/c");
    checkRedundancy("a/b/c", "a/b/c");
    checkRedundancy("../..", "../..");
    checkRedundancy("a/b/.././..", ".");
    checkRedundancy("/a/b/../b/c", "/a/b/c");
    checkRedundancy("/a/b/../../", "/");
    checkRedundancy("././././a/./././././.", "a");
}

void checkName(const std::string &path, const std::string &dirName, const std::string &baseName)
{
    cybozu::FilePath fp(path);
    CYBOZU_TEST_EQUAL(fp.dirName(), dirName);
    CYBOZU_TEST_EQUAL(fp.baseName(), baseName);
}

CYBOZU_TEST_AUTO(name)
{
    checkName("/usr/lib", "/usr", "lib");
    checkName("/usr/", "/", "usr");
    checkName("usr", ".", "usr");
    checkName("/", "/", "/");
    checkName(".", ".", ".");
    checkName("..", ".", "..");
}

void touchFile(const cybozu::FilePath &fp)
{
    if (fp.stat().exists()) return;
    cybozu::util::FileWriter writer(fp.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
    writer.close();
}

CYBOZU_TEST_AUTO(directory)
{
    cybozu::FilePath fp("testdir0");
    CYBOZU_TEST_ASSERT(fp.mkdir());
    touchFile(fp + cybozu::FilePath("f0"));
    touchFile(fp + cybozu::FilePath("f1"));
    cybozu::FilePath d0 = fp + cybozu::FilePath("d0");
    cybozu::FilePath d1 = fp + cybozu::FilePath("d1");
    CYBOZU_TEST_ASSERT(d0.mkdir());
    CYBOZU_TEST_ASSERT(d1.mkdir());
    touchFile(d0 + cybozu::FilePath("f2"));

    cybozu::Directory dir(fp.str());
    std::vector<std::string> v;
    while (!dir.isEnd()) {
        v.push_back(dir.next());
    }
    CYBOZU_TEST_EQUAL(v.size(), 6);

    CYBOZU_TEST_ASSERT(!fp.rmdir());
    CYBOZU_TEST_ASSERT(fp.rmdirRecursive());
}
