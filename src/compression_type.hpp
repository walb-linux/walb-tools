#pragma once
#include "cybozu/exception.hpp"
#include "walb_diff.h"
#include <string>

namespace walb {

namespace compression_type_local {

struct Pair
{
    std::string typeStr;
    int type;
};

static const Pair compressionTypeTable[] = {
    { "none", ::WALB_DIFF_CMPR_NONE },
    { "snappy", ::WALB_DIFF_CMPR_SNAPPY },
    { "gzip", ::WALB_DIFF_CMPR_GZIP },
    { "lzma", ::WALB_DIFF_CMPR_LZMA },
};

} // namespace compression_type_local

inline int parseCompressionType(const std::string &typeStr)
{
    namespace lo = compression_type_local;
    for (const lo::Pair &p : lo::compressionTypeTable) {
        if (p.typeStr == typeStr) {
            return p.type;
        }
    }
    throw cybozu::Exception("parseCompressionType:wrong type") << typeStr;
}

inline const std::string &compressionTypeToStr(int type)
{
    namespace lo = compression_type_local;
    for (const lo::Pair &p : lo::compressionTypeTable) {
        if (p.type == type) {
            return p.typeStr;
        }
    }
    throw cybozu::Exception("compressionTypeToStr:wrong type") << type;
}

} // namespace walb
