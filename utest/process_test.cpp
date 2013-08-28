#include "cybozu/test.hpp"
#include <cstdio>
#include <string>
#include <thread>
#include <chrono>
#include <future>
#include "process.hpp"
#include "fdstream.hpp"
#include <unistd.h>

CYBOZU_TEST_AUTO(call)
{
    std::string s = cybozu::process::call("/usr/bin/basename", {"/usr/bin/test"});
    ::printf("%zu '%s'\n", s.size(), s.c_str());
    CYBOZU_TEST_EQUAL(s, "test\n");
}

CYBOZU_TEST_AUTO(echo)
{
    std::string s = cybozu::process::call("/bin/echo", {"a", "b", "c"});
    CYBOZU_TEST_EQUAL(s, "a b c\n");
}

CYBOZU_TEST_AUTO(pipe)
{
    cybozu::process::Pipe pipe0;

    auto worker0 = [](cybozu::process::Pipe &pipe1) {
#if 1
        cybozu::ofdstream os(pipe1.fdW());
        os << "01234" << std::endl;
        os << "56789" << std::endl;
#else
        //ssize_t s;
        char buf0[] = "01234\n";
        s = ::write(pipe1.fdW(), buf0, sizeof(buf0) - 1);
        //::printf("write [%zd]\n", s);
        char buf1[] = "56789\n";
        s = ::write(pipe1.fdW(), buf1, sizeof(buf1) - 1);
        //::printf("write [%zd]\n", s);
#endif
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        pipe1.closeW();
        ::printf("worker0 end\n");
    };

    auto worker1 = [](cybozu::process::Pipe &pipe1) {
#if 1
        cybozu::ifdstream is(pipe1.fdR());
        std::string s;
        while (true) {
            is >> s;
            if (s.empty()) break;
            ::printf("%s\n", s.c_str());
            s.clear();
        }
#else
        char buf[128];
        ssize_t s;
        while (true) {
            s = ::read(pipe1.fdR(), buf, 128);
            if (s == 0) break;
            buf[s] = '\0';
            ::printf("[%zd] '%s'\n", s, buf);
        }
#endif
    };

    auto f0 = std::async(std::launch::async, worker0, std::ref(pipe0));
    auto f1 = std::async(std::launch::async, worker1, std::ref(pipe0));
    f0.get();
    f1.get();
}
