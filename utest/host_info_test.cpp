#include <cstdio>
#include "cybozu/test.hpp"
#include "host_info.hpp"
#include "for_test.hpp"

using namespace walb;

template <typename T>
void serializeTest(TestDirectory &testDir, const T& t0)
{
    const std::string testDirStr = testDir.getPath();
    cybozu::FilePath fpath(testDirStr);
    fpath += "recordFile";
    cybozu::TmpFile tmpFile(testDirStr);
    cybozu::save(tmpFile, t0);
    tmpFile.save(fpath.str());
    cybozu::util::File reader(fpath.str(), O_RDONLY);
    T t1;
    cybozu::load(t1, reader);
    CYBOZU_TEST_EQUAL(t0, t1);
};

CYBOZU_TEST_AUTO(addrInfo)
{
    AddrPort ap0("192.168.1.1", 5000);
    AddrPort ap1("192.168.1.1", 5000);
    AddrPort ap2("192.168.1.1", 5001);
    CYBOZU_TEST_EQUAL(ap0, ap1);
    CYBOZU_TEST_ASSERT(ap1 != ap2);

    std::string testDirStr("addrInfo");
    TestDirectory testDir(testDirStr, true);
    serializeTest(testDir, ap0);
}

CYBOZU_TEST_AUTO(compressOpt)
{
    std::string testDirStr("compressOpt");
    TestDirectory testDir(testDirStr, true);

    CompressOpt cmpr;
    serializeTest(testDir, cmpr);
    cmpr.parse("lzma:9:1");
    serializeTest(testDir, cmpr);
    cmpr.parse("gzip:9:1");
    serializeTest(testDir, cmpr);
    cmpr.parse("none:9:1");
    serializeTest(testDir, cmpr);
    cmpr.parse("none:9:1");
    serializeTest(testDir, cmpr);

    CYBOZU_TEST_EXCEPTION(cmpr.parse("xxx:9:1"), cybozu::Exception);
    CYBOZU_TEST_EXCEPTION(cmpr.parse("snappy:10:1"), cybozu::Exception);
    CYBOZU_TEST_EXCEPTION(cmpr.parse("snappy:9:0"), cybozu::Exception);
}

CYBOZU_TEST_AUTO(hostInfoForBkp)
{
    std::string testDirStr("hostInfoForBkp");
    TestDirectory testDir(testDirStr, true);

    {
        HostInfoForBkp hi0("192.168.1.100", 5000);
        HostInfoForBkp hi1("192.168.1.101", 5001);
        HostInfoForBkp hi2("192.168.1.102", 5002);
        for (const HostInfoForBkp &hi : {hi0, hi1, hi2}) {
            serializeTest(testDir, hi);
        }
    }
    {
        HostInfoForBkp hi;
        CYBOZU_TEST_EXCEPTION(hi.addrPort.verify(), cybozu::Exception);
    }
    {
        HostInfoForBkp hi("192.168.1.100", 5000);
        hi.cmpr.level = 10;
        CYBOZU_TEST_EXCEPTION(hi.cmpr.verify(), cybozu::Exception);
    }
    {
        HostInfoForBkp hi;
        hi.cmpr.type = ::WALB_DIFF_CMPR_MAX;
        CYBOZU_TEST_EXCEPTION(hi.addrPort.verify(), cybozu::Exception);
        CYBOZU_TEST_EXCEPTION(hi.cmpr.verify(), cybozu::Exception);
    }
    {
        HostInfoForBkp hi0, hi1, hi2, hi3;
        hi0.parse({"192.168.1.1:5000", "lzma:9:1", "0"});
        hi1.parse({"192.168.1.1:5000", "gzip:9:1", "0"});
        hi2.parse({"192.168.1.1:5000", "none:9:1", "0"});
        hi3.parse({"192.168.1.1:5000", "none:9:1", "100"});
        CYBOZU_TEST_EXCEPTION(parseHostInfoForBkp({"192.168.1.1:5000", "xxx:9:1"}), cybozu::Exception);
        CYBOZU_TEST_EXCEPTION(parseHostInfoForBkp({"192.168.1.1:5000", "snappy:10:1"}), cybozu::Exception);
        CYBOZU_TEST_EXCEPTION(parseHostInfoForBkp({"192.168.1.1:5000", "snappy:9:0"}), cybozu::Exception);
    }
}

CYBOZU_TEST_AUTO(hostInfoForRepl)
{
    std::string testDirStr("hostInfoForRepl");
    TestDirectory testDir(testDirStr, true);

    {
        HostInfoForRepl hi0("192.168.1.100", 5000);
        HostInfoForRepl hi1("192.168.1.101", 5001);
        HostInfoForRepl hi2("192.168.1.102", 5002);
        for (const HostInfoForRepl &hi : {hi0, hi1, hi2}) {
            serializeTest(testDir, hi);
        }
    }
    {
        HostInfoForBkp hi;
        CYBOZU_TEST_EXCEPTION(hi.addrPort.verify(), cybozu::Exception);
    }
    {
        HostInfoForBkp hi("192.168.1.100", 5000);
        hi.cmpr.level = 10;
        CYBOZU_TEST_EXCEPTION(hi.cmpr.verify(), cybozu::Exception);
    }
    {
        HostInfoForBkp hi;
        hi.cmpr.type = ::WALB_DIFF_CMPR_MAX;
        CYBOZU_TEST_EXCEPTION(hi.addrPort.verify(), cybozu::Exception);
        CYBOZU_TEST_EXCEPTION(hi.cmpr.verify(), cybozu::Exception);
    }
}
