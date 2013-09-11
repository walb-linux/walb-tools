/**
 * @file
 * @brief WalB client tool.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <string>
#include "cybozu/socket.hpp"
#include "protocol.hpp"

int main(int argc, char *argv[])
try {
    cybozu::Socket sock;
    if (argc < 4) {
        ::printf("specify address, port, and value.\n");
        return 1;
    }
    std::string addr(argv[1]);
    uint16_t port = atoi(argv[2]);
    uint32_t val = atoi(argv[3]);
    sock.connect(addr, port);
#if 0
    sock.write(&val, sizeof(val));
    sock.read(&val, sizeof(val));
    ::printf("recv %u\n", val);
#else
    std::string clientId("client0");
    bool ret = walb::runProtocolAsClient(sock, clientId, "echo");
    if (!ret) {
        throw std::runtime_error("runProtocolAsClient failed.");
    }
#endif
    return 0;
} catch (std::exception &e) {
    ::printf("error: %s\n", e.what());
    return 1;
}

/* end of file */
