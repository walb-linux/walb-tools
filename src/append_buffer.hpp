#pragma once

#include <vector>
#include "walb_types.hpp"

namespace walb {

class AppendBuffer
{
private:
    std::vector<AlignedArray> v_;
public:
    size_t size() const {
        return v_.size();
    }
    size_t totalSize() const {
        size_t total = 0;
        for (const AlignedArray& ary : v_) {
            total += ary.size();
        }
        return total;
    }
    AlignedArray& operator[](size_t i) {
        return v_[i];
    }
    const AlignedArray& operator[](size_t i) const {
        return v_[i];
    }
    void append(size_t size) {
        v_.emplace_back();
        v_.back().resize(size, false);
    }
    void append(AlignedArray&& ary) {
        v_.push_back(std::move(ary));
    }
    void append(const void* data, size_t size) {
        append(size);
        ::memcpy(v_.back().data(), data, size);
    }
    void clear() {
        v_.clear();
    }
    void resize(size_t i) {
        v_.resize(i);
    }
    AlignedArray getAsArray() const {
        AlignedArray ret(totalSize(), false);
        size_t off = 0;
        for (const AlignedArray& ary : v_) {
            ::memcpy(&ret[off], ary.data(), ary.size());
            off += ary.size();
        }
        return ret;
    }
};

} // namespace walb
