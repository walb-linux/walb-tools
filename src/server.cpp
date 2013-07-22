/**
 * @file
 * @brief WalB server daemon.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#include <cstdio>
#include <chrono>
#include <thread>
#include "thread_util.hpp"
#include "cybozu/socket.hpp"

#define LISTEN_PORT 8080

class Worker : public cybozu::thread::Runnable
{
private:
    cybozu::Socket sock_;
public:
    Worker(cybozu::Socket &&sock)
        : sock_(sock) /* sock will be invalid. */ {
    }
    void operator()() noexcept override {
        try {
            run();
            done();
        } catch (...) {
            throwErrorLater();
        }
    }
    void run() {
        uint32_t i;
        if (sock_.readAll(reinterpret_cast<char *>(&i), sizeof(i)) != cybozu::Socket::NOERR) {
            throw std::runtime_error("recv failed.");
        }
        ::printf("recv %u\n", i);
        i++;
        if (sock_.writeAll(reinterpret_cast<char *>(&i), sizeof(i)) != cybozu::Socket::NOERR) {
            throw std::runtime_error("send failed.");
        }
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
};

int main()
{
#if 0
    if (daemon(0, 0) < 0) {
        ::printf("daemon() failed\n");
        return 1;
    }
#endif

    cybozu::Socket ssock;
    if (!ssock.bind(LISTEN_PORT)) {
        ::printf("bind failed.\n");
        return 1;
    }

    cybozu::thread::ThreadRunnerPool pool;
    while (true) {
        while (!ssock.queryAccept()) {
        }
        cybozu::Socket sock;
        if (!ssock.accept(sock)) {
            ::printf("accept failed.\n");
            return 1;
        }
        pool.add(std::make_shared<Worker>(std::move(sock))).start();
        pool.gc();
        ::printf("pool size %zu\n", pool.size());
    }
    pool.join();
    return 0;
}

/* end of file */
