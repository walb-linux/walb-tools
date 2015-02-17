#pragma once
#include <string>
#include "cybozu/exception.hpp"

namespace walb {

enum class DiscardType
{
    Ignore, Passdown, Zero,
};

struct {
    DiscardType type;
    const char *name;
} const discardTypeTbl_[] = {
    {DiscardType::Ignore, "ignore"},
    {DiscardType::Passdown, "passdown"},
    {DiscardType::Zero, "zero"},
};

inline DiscardType parseDiscardType(const std::string &s, const char *msg)
{
    for (const auto& p : discardTypeTbl_) {
        if (s == p.name) return p.type;
    }
    throw cybozu::Exception(msg) << "bad discard type" << s;
}

} // namespace walb
