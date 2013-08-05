#include <cstdio>
#include "cybozu/test.hpp"
#include "proxy_data.hpp"
#include "for_test.hpp"

CYBOZU_TEST_AUTO(server)
{
    std::string testDirStr("proxy_data0");
    TestDirectory testDir(testDirStr, false);

    walb::Server server1("server1", "192.168.1.1", 5678);
    walb::Server server2("server2", "192.168.1.2", 5678);
    walb::Server server3("server3", "192.168.1.3", 5678);

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
