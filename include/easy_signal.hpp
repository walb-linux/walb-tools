#pragma once
#include <csignal>
#include <stdexcept>
#include <initializer_list>


namespace cybozu {
namespace signal {

namespace local {

inline sig_atomic_t& getSignalVariable()
{
    static sig_atomic_t signal = 0;
    return signal;
}

inline void signalHandler(int val)
{
    sig_atomic_t& signal = getSignalVariable();
    signal = val;
}

} // namespace local

inline bool setSignalHandler(void (*callback)(int), std::initializer_list<int> signals, bool throwError = true)
{
    struct sigaction sa;
    sa.sa_handler = callback;
    ::sigfillset(&sa.sa_mask);
    sa.sa_flags = 0;
    for (int signal : signals) {
        if (::sigaction(signal, &sa, NULL) != 0) {
            if (throwError) throw std::runtime_error("register signal handler failed.");
            return false;
        }
    }
    return true;
}

inline void setSignalHandler(std::initializer_list<int> signals, bool throwError = true)
{
    setSignalHandler(local::signalHandler, signals, throwError);
}

inline bool gotSignal(int* val = nullptr)
{
    const int signal = local::getSignalVariable();
    if (val != nullptr) *val = signal;
    return signal != 0;
}

}} // cybozu::signal
