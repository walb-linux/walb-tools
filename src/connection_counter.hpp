#pragma once
#include <atomic>

namespace connection_counter {
namespace local {

inline std::atomic<size_t> &getSingletonCounter()
{
    static std::atomic<size_t> c(0);
    return c;
}

}} // namespace connection_counter::local

inline size_t getConnectionCounter()
{
    return connection_counter::local::getSingletonCounter().load();
}

struct ConnectionCounterTransation
{
    ConnectionCounterTransation() {
        connection_counter::local::getSingletonCounter()++;
    }
    ~ConnectionCounterTransation() noexcept {
        connection_counter::local::getSingletonCounter()--;
    }
    size_t get() const {
        return getConnectionCounter();
    }
};
