#pragma once
#include "walb_logger.hpp"
#include "time.hpp"

/**
 * Use this class in order to avoid putting the same log messages too many times.
 */
class SuppressedLogger
{
    class Sync {
        SuppressedLogger& logger_;
        // gcc-4.8 does not support move cstr of std::ostringstream.
        using SSPtr = std::unique_ptr<std::ostringstream>;
        SSPtr osP_;
        bool atBegin_;
    public:
        explicit Sync(SuppressedLogger& logger)
            : logger_(logger), osP_(new std::ostringstream()), atBegin_(true) {}
        ~Sync() noexcept {
            try {
                if (osP_) logger_.tryToWrite(osP_->str());
            } catch (...){
            }
        }
        Sync(Sync&& rhs)
            : logger_(rhs.logger_), osP_(std::move(rhs.osP_)), atBegin_(rhs.atBegin_) {}
        Sync(const Sync& rhs) = delete;
        template <typename T>
        Sync& operator<<(const T& t) {
            if (!osP_) return *this;
            if (atBegin_) atBegin_ = false;
            else *osP_ << ':';
            *osP_ << t;
            return *this;
        }
    };

    size_t count_;
    cybozu::Timespec ts_;
    cybozu::LogPriority pri_;
    std::string suffix_;
    time_t intervalSec_;
public:
    SuppressedLogger()
        : count_(0), ts_(0), pri_(cybozu::LogDebug), suffix_()
        , intervalSec_(1) {}
    void setSuppressMessageSuffix(const std::string &suffix) {
        suffix_ = suffix;
    }
    void setInervalSec(size_t intervalSec) {
        if (intervalSec == 0) {
            throw cybozu::Exception("SuppressedLogger:intervalSec must not be 0");
        }
        intervalSec_ = intervalSec;
    }
    void putSuppressedMessageIfNecessary() {
        if (count_ == 0) return;
        cybozu::Timespec now = cybozu::getNowAsTimespec();
        const cybozu::TimespecDiff interval((int)intervalSec_);
        if (now - ts_ < interval) return;
        LOGs.writeS(pri_, suppressMessage());
        count_ = 0;
        ts_ = now;
    }
    Sync error() { return makeSync(cybozu::LogError); }
    Sync warn() { return makeSync(cybozu::LogWarning); }
    Sync info() { return makeSync(cybozu::LogInfo); }
    Sync debug() { return makeSync(cybozu::LogDebug); }
private:
    Sync makeSync(cybozu::LogPriority pri) {
        pri_ = pri;
        return Sync(*this);
    }
    void tryToWrite(const std::string& s) {
        cybozu::Timespec now = cybozu::getNowAsTimespec();
        const cybozu::TimespecDiff interval((int)intervalSec_);
        if (now - ts_ >= interval) {
            if (count_ > 0) {
                LOGs.writeS(pri_, suppressMessage());
                count_ = 0;
            }
            LOGs.writeS(pri_, s);
            ts_ = now;
        } else {
            count_++; // suppressed.
        }
    }
    std::string suppressMessage() const {
        return cybozu::util::formatString(
            "Suppressed %zu messages %s", count_, suffix_.c_str());
    }
};
