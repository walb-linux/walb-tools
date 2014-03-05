#include <cstdio>
#include "cybozu/test.hpp"
#include "host_info.hpp"
#include "for_test.hpp"

CYBOZU_TEST_AUTO(hostInfo)
{
    std::string testDirStr("hostInfoTest");
    TestDirectory testDir(testDirStr, true);

    walb::HostInfo host0("host0", "192.168.1.100", 5000);
    walb::HostInfo host1("host1", "192.168.1.101", 5001);
    walb::HostInfo host2("host2", "192.168.1.102", 5002);

    host0.verify();
    host1.verify();
    host2.verify();

    {
        walb::HostInfo host("host0", "192.168.1.100", 5000);
        host.compressionLevel = 10;
        CYBOZU_TEST_EXCEPTION(host.verify(), cybozu::Exception);
    }
    {
        walb::HostInfo host;
        host.compressionType = ::WALB_DIFF_CMPR_MAX;
        CYBOZU_TEST_EXCEPTION(host.verify(), cybozu::Exception);
    }

    cybozu::FilePath fpath(testDirStr);
    fpath += "hostInfoRecord";
    for (const walb::HostInfo &host : {host0, host1, host2}) {
        walb::HostInfo hostx;
        cybozu::TmpFile tmpFile(testDirStr);
        cybozu::save(tmpFile, host);
        tmpFile.save(fpath.str());
        cybozu::util::FileReader reader(fpath.str(), O_RDONLY);
        cybozu::load(hostx, reader);
        CYBOZU_TEST_EQUAL(host, hostx);
    }

    walb::HostInfo hi0, hi1, hi2;
    hi0.parse("host0 192.168.1.1:5000 snappy:5");
    hi1.parse("host0   192.168.1.1:5000   snappy:5");
    hi2.parse("host0", "192.168.1.1:5000", "snappy:5");
    CYBOZU_TEST_EQUAL(hi0, hi1);
    CYBOZU_TEST_EQUAL(hi0, hi2);

    hi0.parse("host0 192.168.1.1:5000 lzma:9");
    hi0.parse("host0 192.168.1.1:5000 gzip:9");
    hi0.parse("host0 192.168.1.1:5000 none:9");
    CYBOZU_TEST_EXCEPTION(hi0.parse("host0 192.168.1.1:5000 xxx:9"), cybozu::Exception);
    CYBOZU_TEST_EXCEPTION(hi0.parse("host0 192.168.1.1:5000 snappy:10"), cybozu::Exception);
}
