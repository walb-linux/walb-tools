#include <cstdio>
#include "cybozu/test.hpp"
#include "proxy_data.hpp"
#include "for_test.hpp"

CYBOZU_TEST_AUTO(server)
{
    std::string testDirStr("proxy_data0");
    TestDirectory testDir(testDirStr, true);

    walb::ServerInfo server1("server1", "192.168.1.1", 5678);
    walb::ServerInfo server2("server2", "192.168.1.2", 5678);
    walb::ServerInfo server3("server3", "192.168.1.3", 5678);

    {
        walb::ProxyData proxyData(testDirStr, "0");
        proxyData.addServer(server1);
        proxyData.addServer(server2);
        proxyData.addServer(server3);
        CYBOZU_TEST_ASSERT(proxyData.existsServer("server1"));
        CYBOZU_TEST_ASSERT(proxyData.existsServer("server2"));
        CYBOZU_TEST_ASSERT(proxyData.existsServer("server3"));

        CYBOZU_TEST_EQUAL(proxyData.getServer("server1"), server1);
        CYBOZU_TEST_EQUAL(proxyData.getServer("server2"), server2);
        CYBOZU_TEST_EQUAL(proxyData.getServer("server3"), server3);
    }
    {
        walb::ProxyData proxyData(testDirStr, "0");
        CYBOZU_TEST_EQUAL(proxyData.getServer("server1"), server1);
        CYBOZU_TEST_EQUAL(proxyData.getServer("server2"), server2);
        CYBOZU_TEST_EQUAL(proxyData.getServer("server3"), server3);
    }
}

CYBOZU_TEST_AUTO(wdiff)
{
    std::string testDirStr("proxy_data1");
    TestDirectory testDir(testDirStr, true);

    walb::ProxyData proxyData(testDirStr, "0");
    walb::ServerInfo server1("server1", "192.168.1.1", 5678);
    walb::ServerInfo server2("server2", "192.168.1.2", 5678);
    proxyData.addServer(server1);
    proxyData.addServer(server2);

    walb::MetaDiff diff;
    setDiff(diff, 0, 1, true);
    createDiffFile(proxyData.getWdiffFiles(), diff);
    proxyData.add(diff);
    setDiff(diff, 1, 2, true);
    createDiffFile(proxyData.getWdiffFiles(), diff);
    proxyData.add(diff);
    setDiff(diff, 5, 7, true);
    createDiffFile(proxyData.getWdiffFiles(), diff);
    proxyData.add(diff);

    std::vector<walb::MetaDiff> v;
    v = proxyData.getTransferCandidates("server1", 1);
    CYBOZU_TEST_EQUAL(v.size(), 2);
    proxyData.removeBeforeGid("server1", 2);
    v = proxyData.getTransferCandidates("server1", 1);
    CYBOZU_TEST_EQUAL(v.size(), 1);
    proxyData.removeBeforeGid("server1", 7);
    CYBOZU_TEST_EQUAL(proxyData.getWdiffFiles().listDiff().size(), 0);
    CYBOZU_TEST_EQUAL(proxyData.getWdiffFiles().latestGid(), 7);
}
