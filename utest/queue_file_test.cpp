#include "cybozu/test.hpp"
#include "queue_file.hpp"
#include "file_path.hpp"

class TmpQueueFile
{
private:
    cybozu::FilePath fp_;
    cybozu::util::QueueFile qf_;
public:
    TmpQueueFile(const std::string &path)
        : fp_(path)
        , qf_(path, O_CREAT | O_TRUNC | O_RDWR, 0644) {
    }
    ~TmpQueueFile() noexcept {
        fp_.unlink();
    }
    cybozu::util::QueueFile& ref() { return qf_; }
    const cybozu::util::QueueFile& ref() const { return qf_; }
};

CYBOZU_TEST_AUTO(queueFile)
{
    TmpQueueFile tqf("tmp_queue");
    cybozu::util::QueueFile& qf = tqf.ref();

    {
        CYBOZU_TEST_ASSERT(qf.empty());

        qf.pushBack(1);
        qf.pushBack(2);
        qf.pushBack("xxx");
        qf.pushBack("yyyy");

        int i; std::string s;
        qf.front(i);
        CYBOZU_TEST_EQUAL(i, 1);
        qf.back(s);
        CYBOZU_TEST_EQUAL(s, "yyyy");
        qf.popFront();
        qf.front(i);
        CYBOZU_TEST_EQUAL(i, 2);
        qf.popFront();
        qf.front(s);
        CYBOZU_TEST_EQUAL(s, "xxx");
        qf.popFront();
        qf.front(s);
        CYBOZU_TEST_EQUAL(s, "yyyy");
        qf.popFront();
        CYBOZU_TEST_ASSERT(qf.empty());
    }
    {
        qf.pushBack(1);
        qf.pushBack(2);
        qf.pushBack(3);

        int i;
        qf.back(i);
        CYBOZU_TEST_EQUAL(i, 3);
        qf.popBack();
        qf.back(i);
        CYBOZU_TEST_EQUAL(i, 2);
        qf.popBack();
        qf.back(i);
        CYBOZU_TEST_EQUAL(i, 1);
        qf.popBack();
        CYBOZU_TEST_ASSERT(qf.empty());
    }
    {
        qf.pushBack(1);
        qf.pushBack(2);
        qf.pushBack(3);
        qf.pushBack(4);
        qf.pushBack(5);

        int i;
        cybozu::util::QueueFile::ConstIterator itr = qf.begin();
        CYBOZU_TEST_ASSERT(itr == qf.cbegin());
        ++itr;
        itr.get(i);
        CYBOZU_TEST_EQUAL(i, 2);
        ++itr;
        ++itr;
        itr.get(i);
        CYBOZU_TEST_EQUAL(i, 4);
        ++itr;
        ++itr;
        CYBOZU_TEST_ASSERT(itr == qf.cend());
    }
}

CYBOZU_TEST_AUTO(queueFileGc)
{
    TmpQueueFile tqf("tmp_queue");
    cybozu::util::QueueFile& qf = tqf.ref();


    for (size_t i = 0; i < 100; i++) {
        qf.pushBack(i);
    }
    for (size_t i = 0; i < 100; i++) {
        qf.popFront();
    }
    for (size_t i = 0; i < 50; i++) {
        qf.pushBack(i);
    }
    qf.gc();
    for (size_t i = 0; i < 50; i++) {
        size_t j;
        qf.front(j);
        CYBOZU_TEST_EQUAL(j, i);
        qf.popFront();
    }
}
