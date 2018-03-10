#pragma once
/**
 * @file
 * @brief Thread utilities.
 * @author HOSHINO Takashi
 *
 * (C) 2013 Cybozu Labs, Inc.
 */
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
#include <sstream>
#include <type_traits>

/**
 * Thread utilities.
 *
 * Prepare a functor or a shared_ptr of a functor at first.
 * A functor can be a lambda expression, a object that has operator(), etc.
 *
 * Then, pass it to 'ThreadRunner'.
 * You can call start() and join() to
 * create new thread and start/join it easily.
 *
 * You can throw an error in your functor.
 * It will be thrown when calling join().
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
 * A functor wrapper to deal with exceptions and callbacks.
 */
class Runner /* final */
{
private:
    struct BaseHolder
    {
        virtual void run() = 0;
        virtual ~BaseHolder() noexcept {}
    };
    template <typename Func>
    struct Holder : public BaseHolder
    {
        Func func;
        explicit Holder(Func&& func) : func(std::forward<Func>(func)) {
        }
        void run() override {
            func();
        }
    };
    template <typename Func>
    struct UPtrHolder : public BaseHolder
    {
        std::unique_ptr<Func> funcP;
        explicit UPtrHolder(std::unique_ptr<Func> &&funcP) : funcP(std::move(funcP)) {
        }
        void run() override {
            (*funcP)();
        }
    };
    template <typename Func>
    struct SPtrHolder : public BaseHolder
    {
        std::shared_ptr<Func> funcP;
        explicit SPtrHolder(std::shared_ptr<Func> funcP) : funcP(funcP) {
        }
        void run() override {
            (*funcP)();
        }
    };
    std::unique_ptr<BaseHolder> holderP_;

    std::promise<void> promise_;
    std::shared_future<void> future_;
    std::atomic<bool> isEnd_;
    std::function<void()> callback_;

    void throwErrorLater(std::exception_ptr p) noexcept {
        assert(p);
        bool expected = false;
        if (!isEnd_.compare_exchange_strong(expected, true)) return;
        promise_.set_exception(p);
        runCallback();
    }
    /**
     * Call this in a catch clause.
     */
    void throwErrorLater() noexcept {
        throwErrorLater(std::current_exception());
    }
    /**
     * You can call this multiple times.
     * This function is thread-safe.
     */
    void done() noexcept {
        bool expected = false;
        if (!isEnd_.compare_exchange_strong(expected, true)) return;
        promise_.set_value();
        runCallback();
    }
    void runCallback() noexcept try {
        if (callback_) callback_();
    } catch (...) {
    }
public:
    template <typename Func>
    explicit Runner(Func&& func)
        : holderP_(new Holder<Func>(std::forward<Func>(func)))
        , promise_()
        , future_(promise_.get_future())
        , isEnd_(false)
        , callback_() {
    }
    template <typename Func>
    explicit Runner(std::unique_ptr<Func> &&funcP)
        : holderP_(new UPtrHolder<Func>(std::move(funcP)))
        , promise_()
        , future_(promise_.get_future())
        , isEnd_(false)
        , callback_() {
    }
    template <typename Func>
    explicit Runner(std::shared_ptr<Func> funcP)
        : holderP_(new SPtrHolder<Func>(funcP))
        , promise_()
        , future_(promise_.get_future())
        , isEnd_(false)
        , callback_() {
    }
    ~Runner() noexcept {
        done();
    }
    void operator()() noexcept try {
        holderP_->run();
        done();
    } catch (...) {
        throwErrorLater();
    }
    void get() { future_.get(); }
    std::exception_ptr getNoThrow() noexcept try {
        future_.get();
        return std::exception_ptr();
    } catch (...) {
        return std::current_exception();
    }
    bool isEnd() const { return isEnd_; }
    /**
     * Callback's throwing exception will be ignored.
     */
    void setCallback(const std::function<void()>& f) { callback_ = f; }
};

/**
 * Thread runner.
 * This is not thread-safe.
 */
class ThreadRunner /* final */
{
private:
    std::shared_ptr<Runner> runnerP_;
    std::unique_ptr<std::thread> threadP_;

public:
    ThreadRunner() {}
    template <typename Func>
    explicit ThreadRunner(Func&& func)
        : runnerP_(new Runner(std::forward<Func>(func)))
        , threadP_() {
    }
    ThreadRunner(const ThreadRunner &) = delete;
    ThreadRunner(ThreadRunner &&rhs)
        : runnerP_(std::move(rhs.runnerP_))
        , threadP_(std::move(rhs.threadP_)) {
    }
    ~ThreadRunner() noexcept {
        joinNoThrow();
    }
    ThreadRunner &operator=(const ThreadRunner &) = delete;
    /**
     * Do not call this function when you are running a thread on *this.
     * Calling of std::thread dstr running a thread will cause std::terminate().
     */
    ThreadRunner &operator=(ThreadRunner &&rhs) {
        runnerP_ = std::move(rhs.runnerP_);
        threadP_ = std::move(rhs.threadP_);
        return *this;
    }
    /**
     * You must join() before calling this
     * when you try to reuse the instance.
     */
    template <typename Func>
    void set(Func&& func) {
        set(std::make_shared<Runner>(std::forward<Func>(func)));
    }
    void set(std::shared_ptr<Runner> runnerP) {
        if (threadP_) throw std::runtime_error("threadP must be null");
        runnerP_ = runnerP;
    }
    void setCallback(const std::function<void()>& f) {
        if (!runnerP_) throw std::runtime_error("runnerP must be not null");
        runnerP_->setCallback(f);
    }
    /**
     * Start a thread.
     */
    void start() {
        /* You need std::ref(). */
        threadP_.reset(new std::thread(std::ref(*runnerP_)));
    }
    /**
     * Wait for the thread done.
     * You will get an exception thrown in the thread running.
     */
    void join() {
        if (!threadP_) return;
        std::unique_ptr<std::thread> tp = std::move(threadP_);
        threadP_.reset();
        tp->join();
        std::shared_ptr<Runner> rp = std::move(runnerP_);
        runnerP_.reset();
        rp->get(); // may throw an exception.
    }
    /**
     * Wait for the thread done.
     * This is nothrow version.
     * Instead, you will get an exception pointer.
     */
    std::exception_ptr joinNoThrow() noexcept {
        std::exception_ptr ep;
        try {
            join();
        } catch (...) {
            ep = std::current_exception();
        }
        return ep;
    }
    /**
     * Check a thread is alive or not.
     * Call canJoin() if the thread is done or not.
     */
    bool isAlive() const {
        return bool(threadP_);
    }
    /**
     * Check whether you can join the thread just now.
     */
    bool canJoin() const {
        return runnerP_->isEnd();
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
    template <typename Func>
    void add(Func&& func) {
        v_.emplace_back(std::forward<Func>(func));
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
        return v;
    }
};

/**
 * Manage ThreadRunners which starting/ending timing differ.
 * This is thread-safe class.
 *
 * (1) You call add() tasks to start them.
 *     Retured uint32_t value is unique identifier of each task.
 * (2) You can call cancel(uint32_t) to cancel a task only if it has not started running.
 * (3) You can call waitFor(uint32_t) to wait for a task to be done.
 * (4) You can call waitForAll() to wait for all tasks to be done.
 * (5) You can call gc() to get results of finished tasks.
 */
class ThreadRunnerPool /* final */
{
private:
    /**
     * A task contains its unique id and a runnable object.
     * Not copyable, but movable.
     */
    class Task
    {
    private:
        uint32_t id_;
        std::shared_ptr<Runner> runnerP_;
    public:
        Task() : id_(uint32_t(-1)), runnerP_() {}
        Task(uint32_t id, std::shared_ptr<Runner> runner)
            : id_(id), runnerP_(runner) {}
        Task(const Task &) = delete;
        Task(Task &&rhs) : id_(rhs.id_), runnerP_(std::move(rhs.runnerP_)) {}
        Task &operator=(const Task &) = delete;
        Task &operator=(Task &&rhs) {
            id_ = rhs.id_;
            rhs.id_ = uint32_t(-1);
            runnerP_ = std::move(rhs.runnerP_);
            return *this;
        }
        uint32_t id() const { return id_; }
        bool isValid() const { return id_ != uint32_t(-1) && runnerP_; }
        std::exception_ptr run() noexcept {
            assert(isValid());
            std::exception_ptr ep;
            try {
                (*runnerP_)();
                runnerP_->get();
            } catch (...) {
                ep = std::current_exception();
            }
            return ep;
        }
    };

    /**
     * A thread has a TaskWorker and run its operator()().
     * A thread will run tasks until the readyQ becomes empty.
     */
    class TaskWorker
    {
    private:
        /* All members are shared with ThreadRunnerPool instance. */
        std::list<Task> &readyQ_;
        std::set<uint32_t> &ready_;
        std::set<uint32_t> &running_;
        std::map<uint32_t, std::exception_ptr> &done_;
        std::mutex &mutex_;
        std::condition_variable &cv_;
    public:
        TaskWorker(std::list<Task> &readyQ, std::set<uint32_t> &ready,
                   std::set<uint32_t> &running, std::map<uint32_t, std::exception_ptr> &done,
                   std::mutex &mutex, std::condition_variable &cv)
            : readyQ_(readyQ), ready_(ready)
            , running_(running), done_(done)
            , mutex_(mutex), cv_(cv) {}
        void operator()() {
            while (tryRunTask());
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
                __attribute__((unused)) auto pair = done_.emplace(task.id(), ep);
                assert(pair.second);

                cv_.notify_all();
            }
            return true;
        }
    };

    /* Threads container. You must call gcThread() to collect finished threads. */
    std::list<ThreadRunner> runners_;
    std::atomic<size_t> numActiveThreads_;

    /* Task container.
       A task will be inserted into readyQ_ and ready_ at first,
       next, moved to running_, and moved to done_, and collected. */
    std::list<Task> readyQ_; /* FIFO. */
    std::set<uint32_t> ready_; /* This is for faster cancel() and waitFor(). */
    std::set<uint32_t> running_;
    std::map<uint32_t, std::exception_ptr> done_;

    size_t maxNumThreads_; /* 0 means unlimited. */
    uint32_t id_; /* for id generator. */

    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;

public:
    explicit ThreadRunnerPool(size_t maxNumThreads = 0)
        : runners_(), numActiveThreads_(0)
        , readyQ_(), ready_(), running_(), done_()
        , maxNumThreads_(maxNumThreads), id_(0), mutex_(), cv_() {
    }
    ~ThreadRunnerPool() noexcept {
        try {
            cancelAll();
            assert(readyQ_.empty());
            assert(ready_.empty());
            waitForAll();
            assert(running_.empty());
            assert(done_.empty());
            gcThread();
            assert(runners_.empty());
        } catch (...) {
        }
    }
    /**
     * Add a runnable task to be executed in the pool.
     */
    template <typename Func>
    uint32_t add(Func&& func) {
        std::lock_guard<std::mutex> lk(mutex_);
        return addNolock(std::make_shared<Runner>(std::forward<Func>(func)));
    }
    template <typename Func>
    uint32_t add(std::shared_ptr<Func> funcP) {
        std::lock_guard<std::mutex> lk(mutex_);
        return addNolock(std::make_shared<Runner>(funcP));
    }
    /**
     * Try to cancel a task if it has not started yet.
     * RETURN:
     *   true if succesfully canceled.
     *   false if the task has already started or done.
     */
    bool cancel(uint32_t id) {
        std::lock_guard<std::mutex> lk(mutex_);
        __attribute__((unused)) const size_t n = ready_.erase(id);
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
     * Cancel all tasks in the ready queue.
     */
    size_t cancelAll() {
        std::lock_guard<std::mutex> lk(mutex_);
        assert(readyQ_.size() == ready_.size());
        size_t ret = readyQ_.size();
        readyQ_.clear();
        ready_.clear();
        return ret;
    }
    /**
     * RETURN:
     *   true if a specified task has finished
     *   and your calling waitFor() will not be blocked.
     */
    bool finished(uint32_t id) {
        std::unique_lock<std::mutex> lk(mutex_);
        return !isReadyOrRunning(id);
    }
    /**
     * Wait for a task done.
     * RETURN:
     *    valid std::exception_ptr if the task has thrown an error.
     *    else std::exception_ptr().
     */
    std::exception_ptr waitFor(uint32_t id) {
        std::unique_lock<std::mutex> lk(mutex_);
        while (isReadyOrRunning(id)) cv_.wait(lk);
        return getResult(id);
    }
    /**
     * Wait for the all tasks done.
     * RETURN:
     *   exception pointer list which tasks had thrown.
     */
    std::vector<std::exception_ptr> waitForAll() {
        std::unique_lock<std::mutex> lk(mutex_);
        while (existsReadyOrRunning()) cv_.wait(lk);
        return getAllResults();
    }
    /**
     * Garbage collect of currently finished tasks.
     * This does not effect to current running tasks.
     * RETURN:
     *   the same as waitForAll().
     */
    std::vector<std::exception_ptr> gc() {
        std::lock_guard<std::mutex> lk(mutex_);
        return getAllResults();
    }
    /**
     * Number of pending tasks in the pool.
     */
    size_t size() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return readyQ_.size() + running_.size() + done_.size();
    }
    /**
     * Number of running tasks in the pool.
     */
    size_t getNumActiveThreads() const {
        return numActiveThreads_;
    }
private:
    bool isReadyOrRunning(uint32_t id) const {
        return ready_.find(id) != ready_.end() ||
            running_.find(id) != running_.end();
    }
    bool existsReadyOrRunning() const {
        assert(readyQ_.size() == ready_.size());
        return !ready_.empty() || !running_.empty();
    }
    bool shouldMakeThread() const {
        return maxNumThreads_ == 0 || numActiveThreads_.load() < maxNumThreads_;
    }
    bool shouldGcThread() const {
        return (maxNumThreads_ == 0 ? running_.size() : maxNumThreads_) * 2 <= runners_.size();
    }
    void makeThread() {
        ThreadRunner runner(TaskWorker(readyQ_, ready_, running_, done_, mutex_, cv_));
        numActiveThreads_++;
        runner.setCallback([this]() { numActiveThreads_--; });
        runner.start();
        runners_.push_back(std::move(runner));
    }
    uint32_t addNolock(std::shared_ptr<Runner> runnerP) {
        uint32_t id = id_++;
        if (id_ == uint32_t(-1)) id_ = 0;
        readyQ_.emplace_back(id, runnerP);
        __attribute__((unused)) auto pair = ready_.insert(id);
        assert(pair.second);
        if (shouldMakeThread()) {
            if (shouldGcThread()) gcThread();
            makeThread();
        }
        return id;
    }
    std::exception_ptr getResult(uint32_t id) {
        std::exception_ptr ep;
        std::map<uint32_t, std::exception_ptr>::iterator it = done_.find(id);
        if (it == done_.end()) return ep; /* already got or there is no task. */
        ep = it->second;
        done_.erase(it);
        return ep;
    }
    std::vector<std::exception_ptr> getAllResults() {
        std::vector<std::exception_ptr> v;
        for (auto &p : done_) {
            std::exception_ptr ep = p.second;
            if (ep) v.push_back(ep);
        }
        done_.clear();
        return v;
    }
    void gcThread() {
        typename std::list<ThreadRunner>::iterator it = runners_.begin();
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
 * Thread pool with a fixed number of threads.
 */
class ThreadRunnerFixedPool /* final */
{
    class Err : public std::exception {
        std::string s_;
    public:
        explicit Err(const std::string& s = "") : std::exception(), s_(s) {
        }
        template <typename T>
        Err& operator<<(const T& t) {
            std::stringstream ss;
            ss << ":" << t;
            s_ += ss.str();
            return *this;
        }
        const char *what() const noexcept override {
            return s_.c_str();
        }
    };
    using AutoLock = std::lock_guard<std::mutex>;
    using UniqueLock = std::unique_lock<std::mutex>;
    struct Worker {
        std::unique_ptr<Runner> runner;
        ThreadRunner th;
        std::mutex mu;
        std::condition_variable cv;
        bool quit;
        Worker() : runner(), th(), mu(), cv(), quit(false) {}
        ~Worker() noexcept {
            {
                AutoLock lk(mu);
                if (quit && !th.isAlive()) return;
                quit = true;
                cv.notify_one();
            }
            th.joinNoThrow();
            assert(!runner);
        }
    };
    void threadWorker(Worker& w) noexcept {
        for (;;) {
            UniqueLock lk(w.mu);
            while (!w.quit && !w.runner) w.cv.wait(lk);
            if (w.quit) {
                if (w.runner) {
                    nrRunning_--;
                    w.runner.reset();
                }
                break;
            }
            (*w.runner)();
            std::exception_ptr ep = w.runner->getNoThrow();
            if (ep) {
                AutoLock lk2(mu_);
                epV_.push_back(ep);
            }
            nrRunning_--;
            w.runner.reset();
        }
    }
    bool addDetail(std::unique_ptr<Runner>&& runner) {
        if (workerV_.empty()) throw Err(NAME()) << "stopped";
        const size_t s = workerV_.size();
        size_t id = id_;
        for (size_t i = 0; i < s; i++) {
            Worker& w = *workerV_[id++ % s];
            UniqueLock lk(w.mu, std::defer_lock);
            if (!lk.try_lock()) continue;
            if (w.runner) continue;
            assert(!w.quit);
            w.runner = std::move(runner);
            nrRunning_++;
            w.cv.notify_one();
            id_ = id;
            return true;
        }
        return false;
    }
    std::vector<std::unique_ptr<Worker> > workerV_;
    std::vector<std::exception_ptr> epV_;
    std::mutex mu_; // for epV_.
    std::atomic<size_t> id_;
    std::atomic<size_t> nrRunning_;
    std::function<void()> setQuitFlag_;
public:
    static constexpr const char *NAME() { return "ThreadRunnerFixedPool"; }
    ThreadRunnerFixedPool()
        : workerV_(), epV_(), mu_(), id_(0), nrRunning_(0), setQuitFlag_() {
    }
    ~ThreadRunnerFixedPool() noexcept {
        stop();
    }
    void setSetQuitFlag(std::function<void()> setQuitFlag) {
        setQuitFlag_ = setQuitFlag;
    }
    /**
     * Start threads.
     * You can call add() for started pools.
     * This is not thread-safe.
     */
    void start(size_t nrThreads) {
        if (nrThreads == 0) throw Err(NAME()) << "nrThreads must be > 0";
        if (!workerV_.empty()) throw Err(NAME()) << "started";
        assert(nrRunning_ == 0);
        for (size_t i = 0; i < nrThreads; i++) {
            workerV_.emplace_back(new Worker());
            Worker& w = *workerV_.back();
            w.th.set([this, &w]() noexcept { threadWorker(w); });
            w.th.start();
        }
    }
    /**
     * Stop all threads. All running tasks will finish.
     * You can call this mutliple times safely.
     * This is not thread-safe.
     */
    void stop() noexcept {
        if (setQuitFlag_) setQuitFlag_();
        workerV_.clear();
    }
    /**
     * Add a task.
     *
     * RETURN:
     *   true if successfully added.
     *   false if there is no free thread.
     *
     * This is thread-safe.
     *
     * The task function can throw an exception.
     * Thrown exceptions will be saved to the queue
     * and you can get them later using gc().
     *
     * If add() failed and func is rvalue, func will be moved to a Runner instance
     * allocated in heap memory, and will be deallocated in the end of add().
     * You can use shared pointer version of add() in order to
     * access to the function you create after calling add().
     */
    template <typename Func>
    bool add(Func&& func) {
        return addDetail(std::unique_ptr<Runner>(new Runner(std::forward<Func>(func))));
    }
    /**
     * Get errors of finished tasks.
     * This is thread-safe.
     */
    std::vector<std::exception_ptr> gc() {
        std::vector<std::exception_ptr> ret;
        {
            AutoLock lk(mu_);
            ret = std::move(epV_);
            epV_.clear();
        }
        return ret;
    }
    /**
     * add() may not succeed even if this number is less than nrThreads
     * unless add() called by one thread only.
     * This is thread-safe.
     */
    size_t nrRunning() const { return nrRunning_; }
};

/**
 * Thread-safe bounded queue.
 *
 * T is type of items.
 * T must be movable or copyable, and possibly default constructible.
 *
 * CAUSION:
 *   If you push 'end data' as T, you can use
 *   while (!q.isEnd()) q.pop(); pattern.
 *   Else, q.pop() may throw ClosedError()
 *   when the sync() is called between the last isEnd() and q.pop().
 *   You had better use the pattern.
 *   T t; while (q.pop(t)) { use(t); }
 */
template <typename T>
class BoundedQueue /* final */
{
private:
    struct IsMovableT {
        static const bool value =
            std::is_move_assignable<T>::value &&
            std::is_move_constructible<T>::value;
    };
    struct IsCopyableT {
        static const bool value =
            std::is_copy_assignable<T>::value &&
            std::is_copy_constructible<T>::value;
    };
    static_assert(IsMovableT::value || IsCopyableT::value,
                  "T is neither movable nor copyable.");

    size_t size_;
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable condEmpty_;
    std::condition_variable condFull_;
    bool isClosed_;
    bool isFailed_;

    using AutoLock = std::unique_lock<std::mutex>;

    void waitForPush(AutoLock& lk) {
        verifyFailed();
        if (isClosed_) throw ClosedError();
        while (!isFailed_ && !isClosed_ && isFull()) condFull_.wait(lk);
        verifyFailed();
        if (isClosed_) throw ClosedError();
    }
public:
    class ClosedError : public std::exception {
    public:
        const char *what() const noexcept override { return "ClosedError"; }
    };
    class FailedError : public std::exception {
    public:
        const char *what() const noexcept override { return "FailedError"; }
    };

    /**
     * @size queue size.
     */
    explicit BoundedQueue(size_t size)
        : size_(size)
        , queue_()
        , mutex_()
        , condEmpty_()
        , condFull_()
        , isClosed_(false)
        , isFailed_(false) {
        verifySize();
    }
    /**
     * Default constructor.
     */
    BoundedQueue() : BoundedQueue(2) {}
    /**
     * Disable copy/move constructors.
     */
    BoundedQueue(const BoundedQueue &rhs) = delete;
    BoundedQueue(BoundedQueue &&rhs) = delete;

    /**
     * Disable copy/move.
     */
    BoundedQueue& operator=(const BoundedQueue &rhs) = delete;
    BoundedQueue& operator=(BoundedQueue &&rhs) = delete;

    /**
     * Change bounded size.
     */
    void resize(size_t size) {
        AutoLock lk(mutex_);
        size_ = size;
        verifySize();
    }
    /**
     * Push an item.
     * This may block if the queue is full.
     * This is for movable T.
     */
    void push(T &&t) {
        static_assert(IsMovableT::value, "T is not movable.");
        pushInner(std::move(t));
    }
    /**
     * This is for copyable T.
     */
    void push(const T &t) {
        static_assert(IsCopyableT::value, "T is not copyable.");
        pushInner(t);
    }
    /**
     * Pop an item.
     * This may block if the queue is empty.
     * The popped value is moved to an argument if T is movable, or copied.
     * RETURN:
     *   true if pop succeeded, false otherwise.
     */
    bool pop(T &t) {
        using TRef = typename std::conditional<IsMovableT::value, T&&, T&>::type;
        return popInner(static_cast<TRef>(t));
    }
    /**
     * Pop an item.
     * This will throw ClosedError, instead returning false.
     * Default constructor required for T.
     */
    T pop() {
        static_assert(std::is_default_constructible<T>::value,
                      "T is not default constructible");
        T t;
        if (!pop(t)) throw ClosedError();
        return t;
    }
    /**
     * You must call this when you have no more items to push.
     * After calling this, push() will fail.
     * The pop() will not fail until queue will be empty.
     */
    void sync() {
        AutoLock lk(mutex_);
        verifyFailed();
        isClosed_ = true;
        condEmpty_.notify_all();
        condFull_.notify_all();
    }
    /**
     * Check if there is no more items and push() will be never called.
     */
#if 0
#ifdef __GNUC__
    __attribute__((deprecated))
#endif
    bool isEnd() const {
        AutoLock lk(mutex_);
        verifyFailed();
        return isClosed_ && isEmpty();
    }
#endif
    /**
     * max size of the queue.
     */
    size_t maxSize() const { return size_; }
    /**
     * Current size of the queue.
     */
    size_t size() const {
        AutoLock lk(mutex_);
        return queue_.size();
    }
    /**
     * You should call this when an error has ocurred.
     * Blockded threads will be waken up and will throw FailedError.
     */
    void fail() noexcept {
        AutoLock lk(mutex_);
        if (isFailed_) return;
        isClosed_ = true;
        isFailed_ = true;
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
    void verifyFailed() const {
        if (isFailed_) throw FailedError();
    }
    void verifySize() const {
        if (size_ < 2) throw std::runtime_error("queue size must be more than 1.");
    }
    template <typename U>
    void pushInner(U &&t) {
        AutoLock lk(mutex_);
        waitForPush(lk);

        bool isEmpty0 = isEmpty();
        queue_.push(std::forward<U>(t));
        if (isEmpty0) { condEmpty_.notify_all(); }
    }
    template <typename U>
    bool popInner(U &&t) {
        AutoLock lk(mutex_);
        verifyFailed();
        if (isClosed_ && isEmpty()) return false;
        while (!isFailed_ && !isClosed_ && isEmpty()) condEmpty_.wait(lk);
        verifyFailed();
        if (isClosed_ && isEmpty()) return false;

        bool isFull0 = isFull();
        t = std::forward<U>(queue_.front());
        queue_.pop();
        if (isFull0) condFull_.notify_all();
        return true;
    }
};

inline std::string exceptionPtrToStr(std::exception_ptr ep) try
{
    std::rethrow_exception(ep);
    return "exceptionPtrToStr:no error";
} catch (std::exception &e) {
    return e.what();
} catch (...) {
    return "exceptionPtrToStr:other error";
}

/**
 * Parallel converter.
 * T1 and T2 must be movable and default constructible.
 *
 * This converter holds the order of items, that is FIFO.
 * This class supports single producer and single consumer with parallel worker.
 */
template <typename T1, typename T2>
class ParallelConverter
{
    struct Src {
        uint64_t id;
        T1 t1;
    };
    struct Dst {
        uint64_t id;
        T2 t2;
    };

    template <typename TT1, typename TT2>
    struct BaseHolder {
        virtual TT2 convert(TT1&&) = 0;
        virtual ~BaseHolder() noexcept {}
    };
    template <typename TT1, typename TT2, typename Converter>
    struct Holder : BaseHolder<TT1, TT2> {
        Converter conv;
        explicit Holder(Converter&& conv)
            : conv(std::forward<Converter>(conv)) {
        }
        TT2 convert(TT1&& t1) override {
            return conv(std::move(t1));
        }
    };
    std::unique_ptr<BaseHolder<T1, T2> > holderP_;

    std::mutex pushMu_;
    uint64_t pushId_;

    std::mutex popMu_;
    uint64_t popId_;
    std::map<uint64_t, T2> map_;

    BoundedQueue<Src> inQ_;
    BoundedQueue<Dst> outQ_;
    ThreadRunnerSet workerSet_;

public:
    /**
     * Convrter must be function of type T2 (*)(T1&&).
     */
    template <typename Converter>
    explicit ParallelConverter(Converter&& conv)
        : holderP_(new Holder<T1, T2, Converter>(std::forward<Converter>(conv)))
        , pushMu_(), pushId_(0)
        , popMu_(), popId_(0), map_()
        , inQ_(2), outQ_(2)
        , workerSet_() {
    }
    ~ParallelConverter() noexcept {
        // You called sync() before, this will not effect anything.
        fail();
    }
    void start(size_t concurrency = 0) {
        std::lock_guard<std::mutex> lock(pushMu_);
        if (concurrency == 0) {
            concurrency = std::thread::hardware_concurrency();
        }
        const size_t qs = concurrency * 2;
        inQ_.resize(qs);
        outQ_.resize(qs);
        for (size_t i = 0; i < concurrency; i++) {
            workerSet_.add([this]() { runWorker(); });
        }
        workerSet_.start();
    }
    /**
     * Do not call this function from multiple threads.
     */
    void push(T1&& t1) {
        std::lock_guard<std::mutex> lock(pushMu_);
        inQ_.push(Src { pushId_, std::move(t1) });
        pushId_++;
    }
    /**
     * Do not call this function from multiple threads.
     */
    bool pop(T2& t2) {
        std::lock_guard<std::mutex> lock(popMu_);
        Dst dst;
        if (findInMap(popId_, t2)) {
            popId_++;
            return true;
        }
        while (outQ_.pop(dst)) {
            if (dst.id == popId_) {
                t2 = std::move(dst.t2);
                popId_++;
                return true;
            }
            map_.insert(std::make_pair(dst.id, std::move(dst.t2)));
        }
        return false;
    }
    /**
     * After calling this, push() always fails.
     */
    void sync() {
        inQ_.sync();
        joinWorkerSet();
        outQ_.sync();
    }
    /**
     * This is thread-safe.
     */
    void fail() noexcept {
        inQ_.fail();
        outQ_.fail();
        joinWorkerSet();
    }
private:
    /**
     * Lock must be held.
     */
    bool findInMap(uint64_t id, T2& t2) {
        if (map_.empty() || id != map_.begin()->first) {
            return false;
        }
        t2 = std::move(map_.begin()->second);
        map_.erase(map_.begin());
        return true;
    }
    void joinWorkerSet() noexcept {
        std::lock_guard<std::mutex> lock(pushMu_);
        workerSet_.join();
    }
    void runWorker() noexcept try {
        Src src;
        Dst dst;
        while (inQ_.pop(src)) {
            dst.id = src.id;
            dst.t2 = holderP_->convert(std::move(src.t1));
            outQ_.push(std::move(dst));
        }
    } catch (...) {
        inQ_.fail();
        outQ_.fail();
    }
};

}} // namespace cybozu::thread
