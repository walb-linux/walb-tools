#include "cybozu/test.hpp"
#include "file_path.hpp"

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
