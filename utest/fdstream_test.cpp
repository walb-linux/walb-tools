#include <fstream>
#include "fdstream.hpp"
#include "cybozu/test.hpp"
#include "fileio.hpp"
#include "file_path.hpp"

void checkNonExists(const cybozu::FilePath &path)
{
    if (path.stat().exists()) {
        throw std::runtime_error("Already exists: " + path.str());
    }
}

CYBOZU_TEST_AUTO(in1)
{
    cybozu::FilePath path("test0.bin");
    checkNonExists(path);
    char buf[1024];
    {
        cybozu::util::File writer(path.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
        int s = ::snprintf(buf, sizeof(buf), "%d\n", 10000);
        writer.write(buf, s);
        CYBOZU_TEST_ASSERT(0 < s);
    }
    {
        cybozu::util::File file(path.str(), O_RDONLY);
        cybozu::ifdstream is(file.fd());
        int i = 0;
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10000);
    }
    path.unlink();
}

CYBOZU_TEST_AUTO(in2)
{
    cybozu::FilePath path("test1.bin");
    checkNonExists(path);
    char buf[1024];
    {
        cybozu::util::File writer(path.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
        int s = ::snprintf(buf, sizeof(buf), "%d\n", 10000);
        writer.write(buf, s);
        s = ::snprintf(buf, sizeof(buf), "%d\n", 10001);
        writer.write(buf, s);
        s = 10002;
        writer.write(&s, sizeof(s));
    }
    {
        std::ifstream is(path.str());
#if 0
        for (int i = 0; i < 20; i++) {
            ::printf("%d\n", is.get());
        }
#else
        int i = 0;
        CYBOZU_TEST_EQUAL(is.peek(), '1');
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10000);
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10001);
        CYBOZU_TEST_EQUAL(is.get(), '\n');
        is.read(reinterpret_cast<char *>(&i), sizeof(i));
        CYBOZU_TEST_EQUAL(i, 10002);
        CYBOZU_TEST_EQUAL(is.peek(), std::ifstream::traits_type::eof());
#endif
        ::printf("-------------------------\n");
    }
    {
        cybozu::util::File file(path.str(), O_RDONLY);
        cybozu::ifdstream is(file.fd());

#if 0
        for (int i = 0; i < 20; i++) {
            ::printf("%d\n", is.get());
        }
#else
        int i = 0;
        CYBOZU_TEST_EQUAL(is.peek(), '1');
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10000);
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10001);
        CYBOZU_TEST_EQUAL(is.get(), '\n');
        is.read(reinterpret_cast<char *>(&i), sizeof(i));
        CYBOZU_TEST_EQUAL(i, 10002);
        CYBOZU_TEST_EQUAL(is.peek(), cybozu::ifdstream::traits_type::eof());
#endif
    }
    path.unlink();
}

CYBOZU_TEST_AUTO(in3)
{
    cybozu::FilePath path("test3.bin");
    checkNonExists(path);
    {
        cybozu::util::File writer(path.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
        char c;
        c = 'a'; writer.write(&c, 1);
        c = 'b'; writer.write(&c, 1);
        c = 'c'; writer.write(&c, 1);
    }
    {
        cybozu::util::File file(path.str(), O_RDONLY);
        cybozu::ifdstream is(file.fd());
        CYBOZU_TEST_EQUAL(is.get(), 'a');
        is.putback('a');
        CYBOZU_TEST_EQUAL(is.get(), 'a');
        CYBOZU_TEST_EQUAL(is.get(), 'b');
        is.putback('e');
        is.putback('d');
        CYBOZU_TEST_EQUAL(is.get(), 'd');
        CYBOZU_TEST_EQUAL(is.get(), 'e');
        CYBOZU_TEST_EQUAL(is.get(), 'c');
        CYBOZU_TEST_EQUAL(is.get(), cybozu::ifdstream::traits_type::eof());
    }
    path.unlink();
}

CYBOZU_TEST_AUTO(out1)
{
    cybozu::FilePath path("test3.bin");
    checkNonExists(path);
    {
        cybozu::util::File file(path.str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
        cybozu::ofdstream os(file.fd());
        int i = 10000;
        os << i << '\n';
        i = 10001;
        os << i << '\n';
        i = 10002;
        os.write(reinterpret_cast<const char *>(&i), sizeof(i));
    }
    {
        std::ifstream is(path.str());
        int i = 0;
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10000);
        is >> i;
        CYBOZU_TEST_EQUAL(i, 10001);
        CYBOZU_TEST_EQUAL(is.get(), '\n');
        is.read(reinterpret_cast<char *>(&i), sizeof(i));
        CYBOZU_TEST_EQUAL(i, 10002);
    }
    path.unlink();
}
