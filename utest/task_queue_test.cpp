#include "cybozu/test.hpp"
#include "task_queue.hpp"

struct Task
{
    bool operator==(const Task &) const {
        return true;
    }
    bool operator!=(const Task &) const {
        return false;
    }
};

CYBOZU_TEST_AUTO(taskQueue)
{
    walb::TaskQueue<Task> tq;
    // QQQ
}
