#include "cybozu/test.hpp"
#include "walb_queue_file.hpp"
#include "file_path.hpp"

using namespace walb;

template <typename T>
class TmpQueueFile
{
private:
    cybozu::FilePath fp_;
    QueueFile<T> qf_;
public:
    TmpQueueFile(const std::string &path)
        : fp_(path)
        , qf_(path, O_CREAT | O_TRUNC | O_RDWR, 0644) {
    }
    ~TmpQueueFile() noexcept {
        fp_.unlink();
    }
    QueueFile<T>& ref() { return qf_; }
    const QueueFile<T>& ref() const { return qf_; }
};

struct IntStr
{
    int i;
    std::string s;

    bool operator==(const IntStr &rhs) const {
        return i == rhs.i && s == rhs.s;
    }
    template <typename OutputStream>
    void save(OutputStream &os) const {
        cybozu::save(os, i);
        cybozu::save(os, s);
    }
    template <typename InputStream>
    void load(InputStream &is) {
        cybozu::load(i, is);
        cybozu::load(s, is);
    }
    friend inline std::ostream &operator<<(std::ostream &os, const IntStr &v) {
        os << v.i << "," << v.s;
        return os;
    }
};

CYBOZU_TEST_AUTO(queueFileForwardInt)
{
    TmpQueueFile<int> tqf("tmp_queue_plus");
    QueueFile<int>& qf = tqf.ref();

    qf.pushBack(1);
    qf.pushBack(2);
    qf.pushBack(3);
    qf.verifyAll();

    int i; std::string s; IntStr is;
    qf.front(i);
    CYBOZU_TEST_EQUAL(i, 1);
    qf.popFront();
    qf.front(i);
    CYBOZU_TEST_EQUAL(i, 2);
    qf.popFront();
    qf.front(i);
    CYBOZU_TEST_EQUAL(i, 3);
    qf.popFront();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFileForwardStr)
{
    TmpQueueFile<std::string> tqf("tmp_queue_plus");
    QueueFile<std::string>& qf = tqf.ref();

    qf.pushBack("a");
    qf.pushBack("bbb");
    qf.pushBack("ccccc");
    qf.verifyAll();

    std::string s;
    qf.front(s);
    CYBOZU_TEST_EQUAL(s, "a");
    qf.popFront();
    qf.front(s);
    CYBOZU_TEST_EQUAL(s, "bbb");
    qf.popFront();
    qf.front(s);
    CYBOZU_TEST_EQUAL(s, "ccccc");
    qf.popFront();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFileForwardIntStr)
{
    TmpQueueFile<IntStr> tqf("tmp_queue_plus");
    QueueFile<IntStr>& qf = tqf.ref();

    qf.pushBack(IntStr{1, "a"});
    qf.pushBack(IntStr{2, "bbb"});
    qf.pushBack(IntStr{3, "ccccc"});
    qf.verifyAll();

    IntStr is;
    qf.front(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{1, "a"}));
    qf.popFront();
    qf.front(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{2, "bbb"}));
    qf.popFront();
    qf.front(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{3, "ccccc"}));
    qf.popFront();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFilePlusBackwardInt)
{
    TmpQueueFile<int> tqf("tmp_queue_plus");
    QueueFile<int>& qf = tqf.ref();

    qf.pushFront(1);
    qf.pushFront(2);
    qf.pushFront(3);
    qf.verifyAll();

    int i;
    qf.back(i);
    CYBOZU_TEST_EQUAL(i, 1);
    qf.popBack();
    qf.back(i);
    CYBOZU_TEST_EQUAL(i, 2);
    qf.popBack();
    qf.back(i);
    CYBOZU_TEST_EQUAL(i, 3);
    qf.popBack();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFilePlusBackwardStr)
{
    TmpQueueFile<std::string> tqf("tmp_queue_plus");
    QueueFile<std::string>& qf = tqf.ref();

    qf.pushFront("a");
    qf.pushFront("bbb");
    qf.pushFront("ccccc");
    qf.verifyAll();

    std::string s;
    qf.back(s);
    CYBOZU_TEST_EQUAL(s, "a");
    qf.popBack();
    qf.back(s);
    CYBOZU_TEST_EQUAL(s, "bbb");
    qf.popBack();
    qf.back(s);
    CYBOZU_TEST_EQUAL(s, "ccccc");
    qf.popBack();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFilePlusBackwardIntStr)
{
    TmpQueueFile<IntStr> tqf("tmp_queue_plus");
    QueueFile<IntStr>& qf = tqf.ref();

    qf.pushFront(IntStr{1, "a"});
    qf.pushFront(IntStr{2, "bbb"});
    qf.pushFront(IntStr{3, "ccccc"});
    qf.verifyAll();

    IntStr is;
    qf.back(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{1, "a"}));
    qf.popBack();
    qf.back(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{2, "bbb"}));
    qf.popBack();
    qf.back(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{3, "ccccc"}));
    qf.popBack();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFileItr)
{
    TmpQueueFile<IntStr> tqf("tmp_queue_plus");
    QueueFile<IntStr>& qf = tqf.ref();

    qf.pushBack(IntStr{1, "aaa"});
    qf.pushBack(IntStr{2, "bbb"});
    qf.pushBack(IntStr{3, "ccc"});
    qf.pushBack(IntStr{4, "ddd"});
    qf.pushBack(IntStr{5, "eee"});
    qf.verifyAll();

    IntStr is;
    QueueFile<IntStr>::ConstIterator itr = qf.cbegin();
    CYBOZU_TEST_ASSERT(itr == qf.cbegin());
    ++itr;
    is = *itr;
    std::cout << is << std::endl;
    CYBOZU_TEST_EQUAL(is, (IntStr{2, "bbb"}));
    ++itr;
    ++itr;
    is = *itr;
    CYBOZU_TEST_EQUAL(is, (IntStr{4, "ddd"}));
    ++itr;
    ++itr;
    CYBOZU_TEST_ASSERT(itr == qf.cend());
    --itr;
    --itr;
    --itr;
    is = *itr;
    CYBOZU_TEST_EQUAL(is, (IntStr{3, "ccc"}));
    --itr;
    --itr;
    is = *itr;
    CYBOZU_TEST_EQUAL(is, (IntStr{1, "aaa"}));
    CYBOZU_TEST_ASSERT(itr == qf.cbegin());
}

CYBOZU_TEST_AUTO(queueFileGc)
{
    TmpQueueFile<size_t> tqf("tmp_queue");
    QueueFile<size_t>& qf = tqf.ref();

    for (size_t i = 0; i < 100; i++) {
        qf.pushBack(i);
    }
    qf.verifyAll();
    for (size_t i = 0; i < 100; i++) {
        qf.popFront();
    }
    qf.verifyAll();
    for (size_t i = 0; i < 50; i++) {
        qf.pushBack(i);
    }
    qf.verifyAll();
    qf.gc();
    qf.verifyAll();
    for (size_t i = 0; i < 50; i++) {
        size_t j;
        qf.front(j);
        CYBOZU_TEST_EQUAL(j, i);
        qf.popFront();
    }
    CYBOZU_TEST_ASSERT(qf.empty());
}

