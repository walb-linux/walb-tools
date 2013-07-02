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
#include <string>
#include <exception>
#include <vector>

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
 * You can ThreadRunnableSet class to
 * start/join multiple threads in bulk.
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
 * You should call throwErrorLater() or done() finally.
 */
class Runnable
{
protected:
    std::string name_;
    std::promise<void> promise_;
    std::shared_future<void> future_;
    bool isEnd_;

    void throwErrorLater(std::exception_ptr p) {
        if (isEnd_) { return; }
        promise_.set_exception(p);
        isEnd_ = true;
    }

    void done() {
        if (isEnd_) { return; }
        promise_.set_value();
        isEnd_ = true;
    }

public:
    explicit Runnable(const std::string &name = "runnable")
        : name_(name)
        , promise_()
        , future_(promise_.get_future())
        , isEnd_(false) {}

    virtual ~Runnable() noexcept {
        try {
            if (!isEnd_) { done(); }
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
};

/**
 * Thread runner.
 */
class ThreadRunner /* final */
{
private:
    Runnable &runnable_;
    std::shared_ptr<std::thread> threadPtr_;

public:
    explicit ThreadRunner(Runnable &runnable)
        : runnable_(runnable)
        , threadPtr_() {}

    ThreadRunner(ThreadRunner &&rhs)
        : runnable_(rhs.runnable_)
        , threadPtr_(std::move(rhs.threadPtr_)) {}

    ThreadRunner(const ThreadRunner &rhs) = delete;
    ThreadRunner &operator=(const ThreadRunner &rhs) = delete;

    ~ThreadRunner() noexcept {
        try {
            join();
        } catch (...) {}
    }

    /**
     * Start a thread.
     */
    void start() {
        /* You need std::ref(). */
        threadPtr_.reset(new std::thread(std::ref(runnable_)));
    }

    /**
     * Wait for thread done.
     * You will get an exception thrown in the thread running.
     */
    void join() {
        if (!threadPtr_) { return; }
        threadPtr_->join();
        threadPtr_.reset();
        runnable_.get();
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

    void add(Runnable &runnable) {
        v_.push_back(ThreadRunner(runnable));
    }

    void start() {
        for (ThreadRunner &r : v_) {
            r.start();
        }
    }

    void join() {
        for (ThreadRunner &r : v_) {
            r.join();
        }
        v_.clear();
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
        while (!isError_ && isEmpty()) { condEmpty_.wait(lk); }
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

}} // namespace cybozu::thread

#endif /* THREAD_UTIL_HPP */
