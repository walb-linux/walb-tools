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
#include <deque>
#include <map>
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
 * (1) You add workers and start them.
 * (2) You should call gc() periodically to check error occurence in the workers execution.
 * (3) You should call join() to wait for all threads to be done.
 */
class ThreadRunnerPool /* final */
{
private:
    std::map<uint32_t, ThreadRunner> map_; /* running */
    std::vector<ThreadRunner> done_;
    uint32_t id_;
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;

public:
    ThreadRunnerPool() : map_(), done_(), id_(0), mutex_() ,cv_() {}
    ~ThreadRunnerPool() noexcept {
        join();
        assert(map_.empty());
        assert(done_.empty());
    }
    /**
     * You must call start() of returned ThreadRunner reference by yourself.
     */
    ThreadRunner &add(std::shared_ptr<Runnable> runnableP) {
        assert(runnableP);
        std::lock_guard<std::mutex> lk(mutex_);
        uint32_t id = id_;
        ++id_;
        auto pair = map_.insert(std::make_pair(id, ThreadRunner(runnableP)));
        assert(pair.second);
        /* This callback will move the runnable from map_ to done_. */
        auto callback = [this, id]() {
            std::lock_guard<std::mutex> lk(mutex_);
            auto it = map_.find(id);
            assert(it != map_.end());
            done_.push_back(std::move(it->second));
            map_.erase(it);
            if (map_.empty()) {
                cv_.notify_all();
            }
        };
        runnableP->setCallback(callback);
        return pair.first->second;
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
     * Wait for all threads done.
     * RETURN:
     *   the same as gc().
     */
    std::vector<std::exception_ptr> join() {
        std::unique_lock<std::mutex> lk(mutex_);
        while (!map_.empty()) {
            cv_.wait(lk);
        }
        return gcNolock();
    }
    size_t size() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return map_.size();
    }
private:
    std::vector<std::exception_ptr> gcNolock() {
        std::vector<std::exception_ptr> v;
        for (ThreadRunner &r : done_) {
            try {
                r.join();
            } catch (...) {
                v.push_back(std::current_exception());
            }
        }
        done_.clear();
        done_.shrink_to_fit();
        return std::move(v);
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
