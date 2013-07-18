/**
 * @file
 * @brief WalB client tool.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Util/Application.h>

namespace {

class app : public Poco::Util::Application
{
public:
    int main(const std::vector<std::string> &args) {
        std::string address = args[0];
        uint16_t port = std::stol(args[1]);
        Poco::Net::SocketAddress sAddr(address, port);

        Poco::Net::StreamSocket sock(Poco::Net::IPAddress::IPv4);
        sock.setSendBufferSize(1 << 20); // 1MiB
        sock.setReceiveBufferSize(1 << 20); // 1MiB
        sock.connect(sAddr);

        char buf[4096];
        buf[0] = 1;
        if (sock.sendBytes(buf, 1) != 1) {
            throw std::runtime_error("send bytes failed.");
        }
        if (sock.receiveBytes(buf, 1) != 1) {
            throw std::runtime_error("recv bytes failed.");
        }
        ::printf("received %0x\n", buf[0]);
        sock.close();
        return EXIT_OK;
    }
};

} //anonymous namespace

POCO_APP_MAIN(app);
