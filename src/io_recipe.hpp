#pragma once
/**
 * @file
 * @brief IO recipe.
 */
#include "cybozu/exception.hpp"
#include <queue>
#include "util.hpp"

namespace walb {
namespace util {

struct IoRecipe
{
    uint64_t offset;
    uint32_t size;
    uint32_t csum;

    IoRecipe(uint64_t offset, uint32_t size, uint32_t csum)
        : offset(offset), size(size), csum(csum) {}
    std::string str() const {
        return cybozu::util::formatString(
            "%" PRIu64 " %u %08x", offset, size, csum);
    }
    friend inline std::ostream& operator<<(std::ostream& os, const IoRecipe& recipe) {
        os << recipe.str();
        return os;
    }
    void print(::FILE *fp = ::stdout) const {
        ::fprintf(fp, "%s\n", str().c_str());
    }
    static IoRecipe parse(const std::string& line) {
        if (line.empty()) {
            throw cybozu::Exception(__func__) << "empty input line";
        }
        std::vector<std::string> v = cybozu::util::splitString(line, " \t");
        cybozu::util::removeEmptyItemFromVec(v);
        if (v.size() != 3) {
            throw cybozu::Exception(__func__) << "invalid number of columns" << v.size();
        }
        const uint64_t offset = std::stoul(v[0]);
        const uint32_t size = std::stoul(v[1]);
        const uint32_t csum = std::stoul(v[2], nullptr, 16);
        return IoRecipe(offset, size, csum);
    }
};

/**
 * Input data.
 */
class IoRecipeParser
{
private:
    ::FILE *fp_;
    std::queue<IoRecipe> q_;
    bool isEnd_;
public:
    static constexpr const char *NAME() { return "IoRecipeParser"; }

    explicit IoRecipeParser(int fd)
        : fp_(::fdopen(fd, "r")), q_(), isEnd_(false) {
        if (fp_ == nullptr) {
            throw cybozu::Exception(NAME()) << "bad file descriptor";
        }
    }
    bool isEnd() const {
        return isEnd_ && q_.empty();
    }
    IoRecipe get() {
        readAhead();
        if (q_.empty()) throw cybozu::Exception(NAME()) << "no more input";
        IoRecipe recipe = q_.front();
        q_.pop();
        return recipe;
    }
private:
    void readAhead() {
        if (isEnd_) return;
        const size_t bufSize = 1024;
        const size_t maxQueueSize = 16;
        char buf[bufSize];
        while (q_.size() < maxQueueSize) {
            if (::fgets(buf, bufSize, fp_) == nullptr) {
                isEnd_ = true;
                return;
            }
            q_.push(IoRecipe::parse(buf));
        }
    }
};

}} //namespace walb::util
