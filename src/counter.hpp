#pragma once
#include <atomic>

namespace counter {
namespace local {

template <int type = 0>
inline std::atomic<size_t> &getSingletonCounter()
{
    static std::atomic<size_t> c(0);
    return c;
}

} // namespace local

template <int type = 0>
inline size_t getCounter()
{
    return counter::local::getSingletonCounter<type>().load();
}

template <int type = 0>
struct CounterTransaction
{
    CounterTransaction() {
        counter::local::getSingletonCounter<type>()++;
    }
    ~CounterTransaction() noexcept {
        counter::local::getSingletonCounter<type>()--;
    }
    size_t get() const {
        return getCounter<type>();
    }
};

} // namespace counter
