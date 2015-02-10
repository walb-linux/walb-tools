#pragma once
#include <vector>
#include <string>
#include <mutex>
#include "cybozu/array.hpp"
#include "walb/block_size.h"

namespace walb {

typedef std::vector<std::string> StrVec;
typedef std::vector<char> Buffer;
typedef std::unique_lock<std::recursive_mutex> UniqueLock;
using AlignedArray = cybozu::AlignedArray<char, LOGICAL_BLOCK_SIZE, false>;
using Buffer2 = AlignedArray; // QQQ

} // namespace walb
