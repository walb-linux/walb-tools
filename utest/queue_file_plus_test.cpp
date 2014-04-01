#include "cybozu/test.hpp"
#include "queue_file_plus.hpp"
#include "file_path.hpp"

class TmpQueueFilePlus
{
private:
    cybozu::FilePath fp_;
    QueueFilePlus qf_;
public:
    TmpQueueFilePlus(const std::string &path)
        : fp_(path)
        , qf_(path, O_CREAT | O_TRUNC | O_RDWR, 0644) {
    }
    ~TmpQueueFilePlus() noexcept {
        fp_.unlink();
    }
    QueueFilePlus& ref() { return qf_; }
    const QueueFilePlus& ref() const { return qf_; }
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

CYBOZU_TEST_AUTO(queueFilePlusForward)
{
    TmpQueueFilePlus tqf("tmp_queue_plus");
    QueueFilePlus& qf = tqf.ref();

    qf.saveBack(1);
    qf.saveBack("xxx");
    qf.saveBack(IntStr{3, "aaa"});
    qf.verifyAll();

    int i; std::string s; IntStr is;
    qf.loadFront(i);
    CYBOZU_TEST_EQUAL(i, 1);
    qf.popFront();

    qf.loadFront(s);
    CYBOZU_TEST_EQUAL(s, "xxx");
    qf.popFront();

    qf.loadFront(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{3, "aaa"}));
    qf.popFront();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFilePlusBackward)
{
    TmpQueueFilePlus tqf("tmp_queue_plus");
    QueueFilePlus& qf = tqf.ref();

    qf.saveFront(1);
    qf.saveFront("xxx");
    qf.saveFront(IntStr{3, "aaa"});
    qf.verifyAll();

    int i; std::string s; IntStr is;
    qf.loadBack(i);
    CYBOZU_TEST_EQUAL(i, 1);
    qf.popBack();

    qf.loadBack(s);
    CYBOZU_TEST_EQUAL(s, "xxx");
    qf.popBack();

    qf.loadBack(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{3, "aaa"}));
    qf.popBack();

    CYBOZU_TEST_ASSERT(qf.empty());
}

CYBOZU_TEST_AUTO(queueFilePlusItr)
{
    TmpQueueFilePlus tqf("tmp_queue_plus");
    QueueFilePlus& qf = tqf.ref();

    qf.saveBack(IntStr{1, "aaa"});
    qf.saveBack(IntStr{2, "bbb"});
    qf.saveBack(IntStr{3, "ccc"});
    qf.saveBack(IntStr{4, "ddd"});
    qf.saveBack(IntStr{5, "eee"});
    qf.verifyAll();

    IntStr is;
    QueueFilePlus::ConstIterator itr = qf.cbegin();
    CYBOZU_TEST_ASSERT(itr == qf.cbegin());
    ++itr;
    itr.load(is);
    std::cout << is << std::endl;
    CYBOZU_TEST_EQUAL(is, (IntStr{2, "bbb"}));
    ++itr;
    ++itr;
    itr.load(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{4, "ddd"}));
    ++itr;
    ++itr;
    CYBOZU_TEST_ASSERT(itr == qf.cend());
    --itr;
    --itr;
    --itr;
    itr.load(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{3, "ccc"}));
    --itr;
    --itr;
    itr.load(is);
    CYBOZU_TEST_EQUAL(is, (IntStr{1, "aaa"}));
    CYBOZU_TEST_ASSERT(itr == qf.cbegin());
}
