/**
 * @file
 * @brief WalB client tool.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include "cybozu/socket.hpp"

int main(int argc, char *argv[])
{
    cybozu::Socket sock;
    if (argc < 4) {
        ::printf("specify address, port, and value.\n");
        return 1;
    }
    std::string addr(argv[1]);
    uint16_t port = atoi(argv[2]);
    uint32_t val = atoi(argv[3]);
    if (!sock.connect(addr, port)) {
        ::printf("connect failed.\n");
        return 1;
    }
    sock.write(&val, sizeof(val));
    sock.readAll(&val, sizeof(val));
    ::printf("recv %u\n", val);
    return 0;
}

/* end of file */
