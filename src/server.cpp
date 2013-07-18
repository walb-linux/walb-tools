/**
 * @file
 * @brief WalB server daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Util/ServerApplication.h>

#define LISTEN_PORT 8080

namespace {

/**
 * Connection handler.
 */
class Connection : public Poco::Net::TCPServerConnection
{
public:
    Connection(const Poco::Net::StreamSocket &s)
        : Poco::Net::TCPServerConnection(s) {
    }

    /**
     * Currently receive 1 byte and print, send 1 byte.
     */
    void run() {
        Poco::Net::StreamSocket &sock = socket();
        sock.setSendBufferSize(1 << 20); // 1MiB
        sock.setReceiveBufferSize(1 << 20); // 1MiB
        char buf[4096];
        if (sock.receiveBytes(buf, 1) != 1) {
            throw std::runtime_error("receive failed.");
        }
        ::printf("receive %0x\n", buf[0]);
        buf[0] = buf[0] + 1;
        if (sock.sendBytes(buf, 1) != 1) {
            throw std::runtime_error("send failed.");
        }
    }
};

class ConnectionFactory : public Poco::Net::TCPServerConnectionFactory
{
public:
    ConnectionFactory()
        : Poco::Net::TCPServerConnectionFactory() {
    }

    Poco::Net::TCPServerConnection* createConnection(const Poco::Net::StreamSocket& socket) override {
        return new Connection(socket);
    }
};

class app : public Poco::Util::ServerApplication
{
public:
    const char *name() const {
        return "server";
    }
    int main(const std::vector<std::string> &args) {
        for (const auto &s : args) {
            ::printf("arg: %s\n", s.c_str());
        }
        Poco::Net::ServerSocket s;
        s.bind(LISTEN_PORT, true);
        s.listen();
        Poco::Net::TCPServer server(new ConnectionFactory(), s);
        server.start();
        waitForTerminationRequest();
        server.stop();
        Poco::ThreadPool::defaultPool().joinAll();
        return Application::EXIT_OK;
    }
};

} //anonymous namespace

POCO_SERVER_MAIN(app);
