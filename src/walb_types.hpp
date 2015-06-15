#pragma once
#include <vector>
#include <string>
#include <mutex>
#include "cybozu/array.hpp"
#include "linux/walb/block_size.h"

namespace walb {

typedef std::vector<std::string> StrVec;
typedef std::unique_lock<std::recursive_mutex> UniqueLock;
using AlignedArray = cybozu::AlignedArray<char, LOGICAL_BLOCK_SIZE, false>;

} // namespace walb
