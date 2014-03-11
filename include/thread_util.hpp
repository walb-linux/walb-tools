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
#include <type_traits>

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

    void throwErrorLater(std::exception_ptr p) noexcept {
        if (isEnd_) { return; }
        assert(p);
        promise_.set_exception(p);
        isEnd_.store(true);
        if (callback_) callback_();
    }

    /**
     * Call this in a catch clause.
     */
    void throwErrorLater() noexcept {
        throwErrorLater(std::current_exception());
    }

    void done() {
        if (isEnd_.load()) { return; }
        promise_.set_value();
        isEnd_.store(true);
        if (callback_) callback_();
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
    void setCallback(const std::function<void()>& f) {
        callback_ = f;
    }
};

/**
 * Thread runner.
 * This is not thread-safe.
 */
template <typename Worker = Runnable>
class ThreadRunner /* final */
{
private:
    static_assert(std::is_base_of<Runnable, Worker>::value,
                  "Worker is not derived from Runnable.");
    std::shared_ptr<Worker> workerP_;
    std::shared_ptr<std::thread> threadP_;

public:
    ThreadRunner() : ThreadRunner(nullptr) {}
    explicit ThreadRunner(const std::shared_ptr<Worker> &workerP)
        : workerP_(workerP)
        , threadP_() {}
    ThreadRunner(const ThreadRunner &rhs) = delete;
    ThreadRunner(ThreadRunner &&rhs)
        : workerP_(rhs.workerP_)
        , threadP_(std::move(rhs.threadP_)) {}
    ~ThreadRunner() noexcept {
        try {
            join();
        } catch (...) {}
    }
    ThreadRunner &operator=(const ThreadRunner &rhs) = delete;
    ThreadRunner &operator=(ThreadRunner &&rhs) {
        workerP_ = std::move(rhs.workerP_);
        threadP_ = std::move(rhs.threadP_);
        return *this;
    }
    /**
     * You must join() before calling this
     * when you try to reuse the instance.
     */
    void set(const std::shared_ptr<Worker>& workerP) {
        if (threadP_) throw std::runtime_error("threadP must be null.");
        workerP_ = workerP;
    }
    /**
     * Start a thread.
     */
    void start() {
        /* You need std::ref(). */
        threadP_.reset(new std::thread(std::ref(*workerP_)));
    }
    /**
     * Wait for the thread done.
     * You will get an exception thrown in the thread running.
     */
    void join() {
        if (!threadP_) { return; }
        threadP_->join();
        threadP_.reset();
        workerP_->get();
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
     * Check whether you can join the thread just now.
     */
    bool canJoin() const {
        return workerP_->isEnd();
    }
};

/**
 * Manage ThreadRunners in bulk.
 */
template <typename Worker = Runnable>
class ThreadRunnerSet /* final */
{
private:
    static_assert(std::is_base_of<Runnable, Worker>::value,
                  "Worker is not derived from Runnable.");
    using Runner = ThreadRunner<Worker>;
    std::vector<Runner > v_;

public:
    void add(Runner &&runner) {
        v_.push_back(std::move(runner));
    }
    void add(const std::shared_ptr<Worker>& workerP) {
        v_.push_back(Runner(workerP));
    }
    void start() {
        for (Runner &r : v_) {
            r.start();
        }
    }
    /**
     * Wait for all threads.
     */
    std::vector<std::exception_ptr> join() {
        std::vector<std::exception_ptr> v;
        for (Runner &r : v_) {
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
template <typename Worker = Runnable>
class ThreadRunnerPool /* final */
{
private:
    static_assert(std::is_base_of<Runnable, Worker>::value,
                  "Worker is not derived from Runnable.");
    /**
     * A task contains its unique id and a runnable object.
     * Not copyable, but movable.
     */
    class Task
    {
    private:
        uint32_t id_;
        std::shared_ptr<Worker> worker_;
    public:
        Task() : id_(uint32_t(-1)), worker_() {}
        Task(uint32_t id, const std::shared_ptr<Worker>& worker)
            : id_(id), worker_(worker) {}
        Task(const Task &rhs) = delete;
        Task(Task &&rhs) : id_(rhs.id_), worker_(std::move(rhs.worker_)) {}
        Task &operator=(const Task &rhs) = delete;
        Task &operator=(Task &&rhs) {
            id_ = rhs.id_;
            rhs.id_ = uint32_t(-1);
            worker_ = std::move(rhs.worker_);
            return *this;
        }
        uint32_t id() const { return id_; }
        bool isValid() const { return id_ != uint32_t(-1) && worker_; }
        std::exception_ptr run() noexcept {
            assert(isValid());
            std::exception_ptr ep;
            try {
                (*worker_)();
                worker_->get();
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
    class TaskWorker : public Runnable
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
                __attribute__((unused)) auto pair = done_.emplace(task.id(), ep);
                assert(pair.second);

                cv_.notify_all();
            }
            return true;
        }
    };

    /* Threads container. You must call gcThread() to collect finished threads. */
    std::list<ThreadRunner<TaskWorker> > runners_;
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
    uint32_t add(const std::shared_ptr<Worker> &worker) {
        std::lock_guard<std::mutex> lk(mutex_);
        return addNolock(worker);
    }
    /**
     * Try to cancel a task if it has not started yet.
     * RETURN:
     *   true if succesfully canceled.
     *   false if the task has already started or done.
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
        auto wp = std::make_shared<TaskWorker>(readyQ_, ready_, running_, done_, mutex_, cv_);
        wp->setCallback([this]() { numActiveThreads_--; });
        ThreadRunner<TaskWorker> runner(wp);
        runner.start();
        runners_.push_back(std::move(runner));
        numActiveThreads_++;
    }
    uint32_t addNolock(const std::shared_ptr<Worker>& worker) {
        assert(worker);
        uint32_t id = id_++;
        if (id_ == uint32_t(-1)) id_ = 0;
        readyQ_.push_back(Task(id, worker));
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
        typename std::list<ThreadRunner<TaskWorker> >::iterator it = runners_.begin();
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
#ifndef _MSC_VER
	~BoundedQueue() noexcept = delete;
#endif

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
#ifdef __GNUC__
	__attribute__((deprecated))
#endif
		bool isEnd() const {
        AutoLock lk(mutex_);
        verifyFailed();
        return isClosed_ && isEmpty();
    }
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
        verifyFailed();
        if (isClosed_) throw ClosedError();
        while (!isFailed_ && !isClosed_ && isFull()) condFull_.wait(lk);
        verifyFailed();
        if (isClosed_) throw ClosedError();

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

inline std::string exceptionPtrToStr(std::exception_ptr ep)
try {
    std::rethrow_exception(ep);
    return "exceptionPtrToStr:no error";
} catch (std::exception &e) {
    return e.what();
} catch (...) {
    return "exceptionPtrToStr:other error";
}

}} // namespace cybozu::thread
