#include "cybozu/test.hpp"
#include "task_queue.hpp"
#include "walb_util.hpp"

//using Task = std::pair<std::string, std::string>;
using Task = std::string;

CYBOZU_TEST_AUTO(taskQueue)
{
    walb::TaskQueue<Task> tq;

    tq.push("aaa");
    Task task;
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "aaa");
    CYBOZU_TEST_ASSERT(!tq.pop(task));

    tq.push("aaa");
    tq.push("bbb");
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "aaa");
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "bbb");
    CYBOZU_TEST_ASSERT(!tq.pop(task));

    tq.push("aaa");
    tq.push("bbb");
    tq.pushForce("aaa", 10);

    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "bbb");
    CYBOZU_TEST_ASSERT(!tq.pop(task));
    walb::util::sleepMs(20);
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "aaa");


    tq.push("aaa1");
    tq.push("bbb2");
    tq.push("bbb3");
    tq.push("aaa4");
    tq.push("bbb5");
    tq.push("aaa6");

    tq.remove([](const Task &task) {
            return task.find("aaa") == 0;
        });

    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "bbb2");
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "bbb3");
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "bbb5");
    CYBOZU_TEST_ASSERT(!tq.pop(task));

    tq.push("aaa");
    tq.push("bbb");
    tq.quit();
    tq.push("ccc");
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "aaa");
    CYBOZU_TEST_ASSERT(tq.pop(task));
    CYBOZU_TEST_EQUAL(task, "bbb");
    CYBOZU_TEST_ASSERT(!tq.pop(task));
}
