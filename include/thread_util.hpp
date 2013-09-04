/**
 * @file
 * @brief Thread utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
#ifndef THREAD_UTIL_HPP
#define THREAD_UTIL_HPP

#include <future>
#include <thread>
#include <memory>
#include <queue>
#include <list>
#include <map>
#include <set>
#include <string>
#include <exception>
#include <vector>
#include <atomic>
#include <cassert>
#include <functional>

/**
 * Thread utilities.
 *
 * Prepare a 'Runnable' object at first,
 * then, pass it to 'ThreadRunner'.
 * You can call start() and join() to
 * create new thread and start/join it easily.
 *
 * You can throw error in your runnable operator()().
 * It will be thrown from join().
 *
 * You can use ThreadRunnableSet class to
 * start/join multiple threads in bulk.
 * This is useful for benchmark.
 *
 * You can use ThreadRunnerPool class to
 * manage multiple tasks.
 *
 * BoundedQueue class will help you to
 * make threads' communication functionalities
 * easily.
 */
namespace cybozu {
namespace thread {

/**
 * This is used by thread runners.
 * Any exceptions will be thrown when
 * join() of thread runners' call.
 *
 * You must call throwErrorLater() or done() finally
 * inside your operator()().
 */
class Runnable
{
protected:
    std::string name_;
    std::promise<void> promise_;
    std::shared_future<void> future_;
    std::atomic<bool> isEnd_;
    std::function<void()> callback_;

    void throwErrorLater(std::exception_ptr p) {
        if (isEnd_) { return; }
        promise_.set_exception(p);
        isEnd_.store(true);
        callback_();
    }

    /**
     * Call this in a catch clause.
     */
    void throwErrorLater() {
        throwErrorLater(std::current_exception());
    }

    void done() {
        if (isEnd_.load()) { return; }
        promise_.set_value();
        isEnd_.store(true);
        callback_();
    }

public:
    explicit Runnable(const std::string &name = "runnable")
        : name_(name)
        , promise_()
        , future_(promise_.get_future())
        , isEnd_(false)
        , callback_() {}

    virtual ~Runnable() noexcept {
        try {
            if (!isEnd_.load()) { done(); }
        } catch (...) {}
    }

    /**
     * You must override this.
     */
    virtual void operator()() {
        throw std::runtime_error("Implement operator()().");
    }

    /**
     * Get the result or exceptions thrown.
     */
    void get() {
        future_.get();
    }

    /**
     * Returns true when the execution has done.
     */
    bool isEnd() const {
        return isEnd_.load();
    }

    /**
     * Set a callback function which will be called at end.
     */
    void setCallback(std::function<void()> f) {
        callback_ = f;
    }
};

/**
 * Thread runner.
 * This is not thread-safe.
 */
class ThreadRunner /* final */
{
private:
    std::shared_ptr<Runnable> runnableP_;
    std::shared_ptr<std::thread> threadP_;

public:
    explicit ThreadRunner(std::shared_ptr<Runnable> runnableP)
        : runnableP_(runnableP)
        , threadP_() {}
    ThreadRunner(const ThreadRunner &rhs) = delete;
    ThreadRunner(ThreadRunner &&rhs)
        : runnableP_(rhs.runnableP_)
        , threadP_(std::move(rhs.threadP_)) {}
    ~ThreadRunner() noexcept {
        try {
            join();
        } catch (...) {}
    }
    ThreadRunner &operator=(const ThreadRunner &rhs) = delete;
    ThreadRunner &operator=(ThreadRunner &&rhs) {
        runnableP_ = std::move(rhs.runnableP_);
        threadP_ = std::move(rhs.threadP_);
        return *this;
    }

    /**
     * Start a thread.
     */
    void start() {
        /* You need std::ref(). */
        threadP_.reset(new std::thread(std::ref(*runnableP_)));
    }

    /**
     * Wait for thread done.
     * You will get an exception thrown in the thread running.
     */
    void join() {
        if (!threadP_) { return; }
        threadP_->join();
        threadP_.reset();
        runnableP_->get();
    }

    /**
     * Check whether you can join the thread just now.
     */
    bool canJoin() const {
        return runnableP_->isEnd();
    }
};

/**
 * Manage ThreadRunners in bulk.
 */
class ThreadRunnerSet /* final */
{
private:
    std::vector<ThreadRunner> v_;

public:
    ThreadRunnerSet() : v_() {}
    ~ThreadRunnerSet() noexcept {}

    void add(ThreadRunner &&runner) {
        v_.push_back(std::move(runner));
    }

    void add(std::shared_ptr<Runnable> runnableP) {
        v_.push_back(ThreadRunner(runnableP));
    }

    void start() {
        for (ThreadRunner &r : v_) {
            r.start();
        }
    }

    /**
     * Wait for all threads.
     */
    std::vector<std::exception_ptr> join() {
        std::vector<std::exception_ptr> v;
        for (ThreadRunner &r : v_) {
            try {
                r.join();
            } catch (...) {
                v.push_back(std::current_exception());
            }
        }
        v_.clear();
        return std::move(v);
    }
};

/**
 * Manage ThreadRunners which starting/ending timing differ.
 * This is thread-safe class.
 *
 * (1) You call add() workers to start them.
 *     Retured uint32_t value is unique identifier of each worker.
 * (2) You can call cancel(uint32_t) to cancel a worker only if it has not started running.
 * (3) You should call gc() periodically to check error occurence in the workers execution.
 * (4) You can call join(uint32_t) to wait for a thread to be done.
 * (5) You call join() to wait for all threads to be done.
 */
class ThreadRunnerPool /* final */
{
private:
    std::list<std::pair<uint32_t, std::shared_ptr<Runnable> > > readyQ_;
    std::map<uint32_t, std::shared_ptr<Runnable> > ready_;
    std::map<uint32_t, ThreadRunner> running_;
    std::map<uint32_t, ThreadRunner> done_;
    size_t maxNumThreads_; /* 0 means unlimited. */
    uint32_t id_;
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;

public:
    explicit ThreadRunnerPool(size_t maxNumThreads = 0)
        : readyQ_(), ready_(), running_(), done_()
        , maxNumThreads_(maxNumThreads), id_(0), mutex_() ,cv_() {}
    ~ThreadRunnerPool() noexcept {
        cancelAll();
        join();
        assert(readyQ_.empty());
        assert(ready_.empty());
        assert(running_.empty());
        assert(done_.empty());
    }
    /**
     * A new thread will start in the function.
     */
    uint32_t add(std::shared_ptr<Runnable> runnableP) {
        std::lock_guard<std::mutex> lk(mutex_);
        return addNolock(runnableP);
    }
    /**
     * Try to cancel a runnable if it has not started yet.
     * RETURN:
     *   true if succesfully canceled.
     */
    bool cancel(uint32_t id) {
        std::lock_guard<std::mutex> lk(mutex_);
        assert(readyQ_.size() == ready_.size());
        __attribute__((unused)) bool found = false;
        {
            auto it = ready_.find(id);
            if (it == ready_.end()) {
                found = false;
            } else {
                found = true;
                ready_.erase(it);
            }
        }
        auto it = readyQ_.begin();
        while (it != readyQ_.end()) {
            if (it->first == id) {
                readyQ_.erase(it);
                assert(found);
                return true;
            }
            ++it;
        }
        assert(readyQ_.size() == ready_.size());
        assert(!found);
        return false;
    }
    /**
     * Cancel all runnables in the ready queue.
     */
    void cancelAll() {
        std::lock_guard<std::mutex> lk(mutex_);
        assert(readyQ_.size() == ready_.size());
        readyQ_.clear();
        ready_.clear();
    }
    /**
     * Remove all ended threads and get exceptions of them if exists.
     * RETURN:
     *   a list of exceptions thrown from threads.
     */
    std::vector<std::exception_ptr> gc() {
        std::lock_guard<std::mutex> lk(mutex_);
        return gcNolock();
    }
    /**
     * Wait for a thread done.
     */
    std::vector<std::exception_ptr> join(uint32_t id) {
        std::unique_lock<std::mutex> lk(mutex_);
        while (isReadyOrRunning(id)) cv_.wait(lk);
        return gcNolock(id);
    }
    /**
     * Wait for all threads done.
     * RETURN:
     *   the same as gc().
     */
    std::vector<std::exception_ptr> join() {
        std::unique_lock<std::mutex> lk(mutex_);
        while (existsReadyOrRunning()) cv_.wait(lk);
        return gcNolock();
    }
    size_t size() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return ready_.size() + running_.size() + done_.size();
    }
private:
    /**
     * Lock must be held.
     */
    bool isReadyOrRunning(uint32_t id) const {
        return ready_.find(id) != ready_.end() ||
            running_.find(id) != running_.end();
    }
    /**
     * Lock must be held.
     */
    bool existsReadyOrRunning() const {
        assert(readyQ_.size() == ready_.size());
        return !ready_.empty() || !running_.empty();
    }
    /**
     * Run a runnable.
     */
    void runNolock(uint32_t id, std::shared_ptr<Runnable> runnableP) {
        auto pair = running_.insert(std::make_pair(id, ThreadRunner(runnableP)));
        assert(pair.second);
        /* This callback will move the runnable from running_ to done_. */
        auto callback = [this, id]() {
            std::lock_guard<std::mutex> lk(mutex_);
            auto it = running_.find(id);
            assert(it != running_.end());
            __attribute__((unused)) auto pair = done_.insert(std::make_pair(id, std::move(it->second)));
            assert(pair.second);
            running_.erase(it);
            cv_.notify_all();
            fill();
        };
        runnableP->setCallback(callback);
        ThreadRunner &runner = pair.first->second;
        runner.start();
    }
    bool canRun() const {
        return maxNumThreads_ == 0 || running_.size() < maxNumThreads_;
    }
    /**
     * Make ready runnables be running.
     * Lock must be held.
     */
    void fill() {
        while (canRun() && !readyQ_.empty()) {
            assert(readyQ_.size() == ready_.size());
            uint32_t id;
            std::shared_ptr<Runnable> rp;
            std::tie(id, rp) = readyQ_.front();
            runNolock(id, rp);
            readyQ_.pop_front();

            __attribute__((unused)) size_t i = ready_.erase(id);
            assert(i == 1);
        }
    }
    uint32_t addNolock(std::shared_ptr<Runnable> runnableP) {
        assert(runnableP);
        uint32_t id = id_++;
        readyQ_.push_back(std::make_pair(id, runnableP));
        __attribute__((unused)) auto pair = ready_.insert(std::make_pair(id, runnableP));
        assert(pair.second);
        fill();
        return id;
    }
    std::vector<std::exception_ptr> gcNolock(uint32_t id) {
        std::vector<std::exception_ptr> v;
        std::map<uint32_t, ThreadRunner>::iterator it = done_.find(id);
        if (it == done_.end()) return {}; /* already collected. */
        ThreadRunner &r = it->second;
        try {
            r.join();
        } catch (...) {
            v.push_back(std::current_exception());
        }
        done_.erase(it);
        return std::move(v);
    }
    std::vector<std::exception_ptr> gcNolock() {
        std::vector<std::exception_ptr> v;
        for (auto &p : done_) {
            try {
                ThreadRunner &r = p.second;
                r.join();
            } catch (...) {
                v.push_back(std::current_exception());
            }
        }
        done_.clear();
        return std::move(v);
    }
};

class ThreadRunnerPool2 /* final */
{
private:
    class Task
    {
    private:
        uint32_t id_;
        std::shared_ptr<Runnable> runnable_;
    public:
        Task() : id_(uint32_t(-1)), runnable_() {}
        Task(uint32_t id, std::shared_ptr<Runnable> runnable)
            : id_(id), runnable_(runnable) {}
        Task(const Task &rhs) = delete;
        Task(Task &&rhs) : id_(rhs.id_), runnable_(std::move(rhs.runnable_)) {}
        Task &operator=(const Task &rhs) = delete;
        Task &operator=(Task &&rhs) {
            id_ = rhs.id_;
            rhs.id_ = uint32_t(-1);
            runnable_ = std::move(rhs.runnable_);
            return *this;
        }
        uint32_t id() const { return id_; }
        std::exception_ptr run() {
            std::exception_ptr ep;
            /* Execute the runnable. */
            (*runnable_)();
            try {
                runnable_->get();
            } catch (...) {
                ep = std::current_exception();
            }
            return ep;
        }
    };

    class TaskWorker : public Runnable
    {
    private:
        std::list<Task> &readyQ_; /* shared */
        std::set<uint32_t> &ready_;
        std::set<uint32_t> &running_;
        std::map<uint32_t, std::exception_ptr> &done_;
        std::mutex &mutex_; /* shared */
        std::condition_variable &cv_; /* shared */
    public:
        TaskWorker(std::list<Task> &readyQ, std::set<uint32_t> &ready,
                   std::set<uint32_t> &running, std::map<uint32_t, std::exception_ptr> &done,
                   std::mutex &mutex, std::condition_variable &cv)
            : readyQ_(readyQ), ready_(ready)
            , running_(running), done_(done)
            , mutex_(mutex), cv_(cv) {}
        void operator()() override {
            try {
                while (tryRunTask());
                done();
            } catch (...) {
                throwErrorLater();
            }
        }
    private:
        bool tryRunTask() {
            Task task;
            {
                std::lock_guard<std::mutex> lk(mutex_);
                if (readyQ_.empty()) return false;
                task = std::move(readyQ_.front());
                readyQ_.pop_front();
                __attribute__((unused)) size_t i = ready_.erase(task.id());
                assert(i == 1);
                __attribute__((unused)) auto pair = running_.insert(task.id());
                assert(pair.second);
            }
            std::exception_ptr ep = task.run();
            {
                std::lock_guard<std::mutex> lk(mutex_);
                __attribute__((unused)) size_t i = running_.erase(task.id());
                assert(i == 1);
                __attribute__((unused)) auto pair = done_.insert(std::make_pair(task.id(), ep));
                assert(pair.second);

                cv_.notify_all();
            }
            return true;
        }
    };

    std::list<ThreadRunner> runners_;
    std::atomic<size_t> numActiveThreads_;
    std::list<Task> readyQ_;
    std::set<uint32_t> ready_;
    std::set<uint32_t> running_;
    std::map<uint32_t, std::exception_ptr> done_;
    size_t maxNumThreads_; /* 0 means unlimited. */
    uint32_t id_;
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;

public:
    explicit ThreadRunnerPool2(size_t maxNumThreads = 0)
        : runners_(), numActiveThreads_(0)
        , readyQ_(), ready_(), running_(), done_()
        , maxNumThreads_(maxNumThreads), id_(0), mutex_(), cv_() {
    }
    ~ThreadRunnerPool2() noexcept {
        try {
            cancelAll();
            assert(readyQ_.empty());
            assert(ready_.empty());
            join();
            assert(running_.empty());
            assert(done_.empty());
            gcThreadRunner();
            assert(runners_.empty());
        } catch (...) {
        }
    }
    /**
     * A new thread will start in the function.
     */
    uint32_t add(std::shared_ptr<Runnable> runnableP) {
        std::lock_guard<std::mutex> lk(mutex_);
        return addNolock(runnableP);
    }
    /**
     * Try to cancel a runnable if it has not started yet.
     * RETURN:
     *   true if succesfully canceled.
     */
    bool cancel(uint32_t id) {
        std::lock_guard<std::mutex> lk(mutex_);
        __attribute__((unused)) size_t n = ready_.erase(id);
        auto it = readyQ_.begin();
        while (it != readyQ_.end()) {
            if (it->id() == id) {
                readyQ_.erase(it);
                assert(n == 1);
                return true;
            }
            ++it;
        }
        assert(n == 0);
        return false;
    }
    /**
     * Cancel all runnables in the ready queue.
     */
    void cancelAll() {
        std::lock_guard<std::mutex> lk(mutex_);
        assert(readyQ_.size() == ready_.size());
        readyQ_.clear();
        ready_.clear();
    }
    /**
     * Wait for a thread done.
     */
    std::exception_ptr join(uint32_t id) {
        std::unique_lock<std::mutex> lk(mutex_);
        while (isReadyOrRunning(id)) cv_.wait(lk);
        return waitForNolock(id);
    }
    /**
     * Wait for all threads done.
     * RETURN:
     *   the same as gc().
     */
    std::vector<std::exception_ptr> join() {
        std::unique_lock<std::mutex> lk(mutex_);
        while (existsReadyOrRunning()) cv_.wait(lk);
        return waitForAllNolock();
    }
    /**
     * Number of pending tasks in the pool.
     */
    size_t size() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return readyQ_.size() + running_.size() + done_.size();
    }
private:
    /**
     * Lock must be held.
     */
    bool isReadyOrRunning(uint32_t id) const {
        return ready_.find(id) != ready_.end() ||
            running_.find(id) != running_.end();
    }
    /**
     * Lock must be held.
     */
    bool existsReadyOrRunning() const {
        assert(readyQ_.size() == ready_.size());
        return !ready_.empty() || !running_.empty();
    }
    bool canRun() const {
        return maxNumThreads_ == 0 || numActiveThreads_.load() < maxNumThreads_;
    }
    bool shouldGc() const {
        return (maxNumThreads_ == 0 ? running_.size() : maxNumThreads_) * 2 <= runners_.size();
    }
    /**
     * Make ready runnables be running.
     * Lock must be held.
     */
    void makeThread() {
        auto wp = std::make_shared<TaskWorker>(readyQ_, ready_, running_, done_, mutex_, cv_);
        auto callback = [this]() { numActiveThreads_--; };
        wp->setCallback(callback);
        ThreadRunner runner(wp);
        runner.start();
        runners_.push_back(std::move(runner));
        numActiveThreads_++;
    }
    uint32_t addNolock(std::shared_ptr<Runnable> runnableP) {
        assert(runnableP);
        uint32_t id = id_++;
        readyQ_.push_back(Task(id, runnableP));
        __attribute__((unused)) auto pair = ready_.insert(id);
        assert(pair.second);
        if (canRun()) {
            if (shouldGc()) gcThreadRunner();
            makeThread();
        }
        return id;
    }
    std::exception_ptr waitForNolock(uint32_t id) {
        std::exception_ptr ep;
        std::map<uint32_t, std::exception_ptr>::iterator it = done_.find(id);
        if (it == done_.end()) return ep; /* already collected. */
        ep = it->second;
        done_.erase(it);
        return ep;
    }
    std::vector<std::exception_ptr> waitForAllNolock() {
        std::vector<std::exception_ptr> v;
        for (auto &p : done_) {
            std::exception_ptr ep = p.second;
            if (ep) v.push_back(ep);
        }
        done_.clear();
        return std::move(v);
    }
    /**
     * Lock must be held.
     */
    void gcThreadRunner() {
        std::list<ThreadRunner>::iterator it = runners_.begin();
        while (it != runners_.end()) {
            if (it->canJoin()) {
                it->join(); /* never throw an exception that is related to tasks. */
                it = runners_.erase(it);
            } else {
                ++it;
            }
        }
    }
};

/**
 * Thread-safe bounded queue.
 *
 * T is type of items.
 *   You should use integer types or copyable classes, or shared pointers.
 */
template <typename T>
class BoundedQueue /* final */
{
private:
    const size_t size_;
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable condEmpty_;
    std::condition_variable condFull_;
    bool closed_;
    bool isError_;

    using lock = std::unique_lock<std::mutex>;

public:
    class ClosedError : public std::exception {
    public:
        ClosedError() : std::exception() {}
    };

    class OtherError : public std::exception {
    public:
        OtherError() : std::exception() {}
    };

    /**
     * @size queue size.
     */
    BoundedQueue(size_t size)
        : size_(size)
        , queue_()
        , mutex_()
        , condEmpty_()
        , condFull_()
        , closed_(false)
        , isError_(false) {}

    BoundedQueue(const BoundedQueue &rhs) = delete;
    BoundedQueue& operator=(const BoundedQueue &rhs) = delete;
    ~BoundedQueue() noexcept {}

    void push(T t) {
        lock lk(mutex_);
        checkError();
        if (closed_) { throw ClosedError(); }
        while (!isError_ && !closed_ && isFull()) { condFull_.wait(lk); }
        checkError();
        if (closed_) { throw ClosedError(); }

        bool isEmpty0 = isEmpty();
        queue_.push(t);
        if (isEmpty0) { condEmpty_.notify_all(); }
    }

    T pop() {
        lock lk(mutex_);
        checkError();
        if (closed_ && isEmpty()) { throw ClosedError(); }
        while (!isError_ && !closed_ && isEmpty()) { condEmpty_.wait(lk); }
        checkError();
        if (closed_ && isEmpty()) { throw ClosedError(); }

        bool isFull0 = isFull();
        T t = queue_.front();
        queue_.pop();
        if (isFull0) { condFull_.notify_all(); }
        return t;
    }

    /**
     * You must call this when you have no more items to push.
     * After calling this, push() will fail.
     * The pop() will not fail until queue will be empty.
     */
    void sync() {
        lock lk(mutex_);
        checkError();
        closed_ = true;
        condEmpty_.notify_all();
        condFull_.notify_all();
    }

    /**
     * Check if there is no more items and push() will be never called.
     */
    bool isEnd() const {
        lock lk(mutex_);
        checkError();
        return closed_ && isEmpty();
    }

    /**
     * max size of the queue.
     */
    size_t maxSize() const { return size_; }

    /**
     * Current size of the queue.
     */
    size_t size() const {
        lock lk(mutex_);
        return queue_.size();
    }

    /**
     * You should call this when error has ocurred.
     * Blockded threads will be waken up and will throw exceptions.
     */
    void error() {
        lock lk(mutex_);
        if (isError_) { return; }
        closed_ = true;
        isError_ = true;
        condEmpty_.notify_all();
        condFull_.notify_all();
    }

private:
    bool isFull() const {
        return size_ <= queue_.size();
    }

    bool isEmpty() const {
        return queue_.empty();
    }

    void checkError() const {
        if (isError_) { throw OtherError(); }
    }
};

/**
 * Shared lock with limits.
 */
class MutexN
{
private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    const size_t max_;
    size_t counter_;

public:
    explicit MutexN(size_t max)
        : max_(max), counter_(0) {
        if (max == 0) {
            std::runtime_error("max must be > 0.");
        }
    }
    void lock() {
        std::unique_lock<std::mutex> lk(mutex_);
        while (!(counter_ < max_)) {
            cv_.wait(lk);
        }
        counter_++;
    }
    void unlock() {
        std::unique_lock<std::mutex> lk(mutex_);
        counter_--;
        if (counter_ < max_) {
            cv_.notify_one();
        }
    }
};

/**
 * Sequence lock with limits.
 */
class SeqMutexN
{
private:
    const size_t max_;
    size_t counter_;
    std::mutex mutex_;
    std::queue<std::shared_ptr<std::condition_variable> > waitQ_;

public:
    explicit SeqMutexN(size_t max)
        : max_(max), counter_(0) {
    }
    void lock(std::shared_ptr<std::condition_variable> cvP) {
        std::unique_lock<std::mutex> lk(mutex_);
        if (!(counter_ < max_)) {
            waitQ_.push(cvP);
            cvP->wait(lk);
        }
        counter_++;
    }
    void lock() {
        lock(std::make_shared<std::condition_variable>());
    }
    void unlock() {
        std::unique_lock<std::mutex> lk(mutex_);
        counter_--;
        if (counter_ < max_ && !waitQ_.empty()) {
            waitQ_.front()->notify_one();
            waitQ_.pop();
        }
    }
};

/**
 * RAII for MutexN.
 */
class LockN
{
private:
    MutexN &mutexN_;
public:
    LockN(MutexN &mutexN)
        : mutexN_(mutexN) {
        mutexN.lock();
    }
    ~LockN() noexcept {
        mutexN_.unlock();
    }
};

/**
 * RAII for SeqMutexN.
 */
class SeqLockN
{
private:
    SeqMutexN &seqMutexN_;
public:
    SeqLockN(SeqMutexN &seqMutexN)
        : seqMutexN_(seqMutexN) {
        seqMutexN.lock();
    }
    SeqLockN(SeqMutexN &seqMutexN,
             std::shared_ptr<std::condition_variable> cvP)
        : seqMutexN_(seqMutexN) {
        seqMutexN.lock(cvP);
    }
    ~SeqLockN() noexcept {
        seqMutexN_.unlock();
    }
};

}} // namespace cybozu::thread

#endif /* THREAD_UTIL_HPP */
